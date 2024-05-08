use std::path::{Path, PathBuf};
use std::{str::FromStr, sync::Arc};

use crate::config::XetConfig;
use crate::errors::{GitXetRepoError, Result};
use crate::git_integration::git_repo_salt::RepoSalt;
use anyhow::anyhow;
use cas::singleflight;
use cas_client::Staging;
use lru::LruCache;

use mdb_shard::MDBShardFile;
use mdb_shard::{
    error::MDBShardError, file_structs::MDBFileInfo, shard_file_manager::ShardFileManager,
    shard_file_reconstructor::FileReconstructor,
};
use merklehash::MerkleHash;
use shard_client::ShardClientInterface;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::constants::{FILE_RECONSTRUCTION_CACHE_SIZE, GIT_XET_VERSION};
use std::sync::Mutex;

use super::mdb;

#[derive(PartialEq, Default, Clone, Debug, Copy)]
pub enum SmudgeQueryPolicy {
    /// Query local first, then the shard server.
    #[default]
    LocalFirst,

    /// Only query the server; ignore local shards.
    ServerOnly,

    /// Only query local shards.
    LocalOnly,
}

impl FromStr for SmudgeQueryPolicy {
    type Err = std::io::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "local_first" => Ok(SmudgeQueryPolicy::LocalFirst),
            "server_only" => Ok(SmudgeQueryPolicy::ServerOnly),
            "local_only" => Ok(SmudgeQueryPolicy::LocalOnly),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid file smudge policy, should be one of local_first, server_only, local_only: {}", s),
            )),
        }
    }
}

#[derive(PartialEq, Default, Clone, Debug, Copy)]
pub enum GlobalDedupPolicy {
    /// Never query for new shards using chunk hashes.
    Never,

    /// Only query for new shards when using direct file access methods like `xet cp`
    #[default]
    OnDirectAccess,

    /// Always query for new shards by chunks (not recommended except for testing)
    Always,
}

impl FromStr for GlobalDedupPolicy {
    type Err = std::io::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "never" => Ok(GlobalDedupPolicy::Never),
            "direct_only" => Ok(GlobalDedupPolicy::OnDirectAccess),
            "always" => Ok(GlobalDedupPolicy::Always),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid global dedup query policy, should be one of never, direct_only, always: {}", s),
            )),
        }
    }
}

pub async fn shard_manager_from_config(config: &XetConfig) -> Result<ShardFileManager> {
    let shard_manager = ShardFileManager::new(&config.merkledb_v2_session).await?;

    if config.merkledb_v2_cache.exists() {
        shard_manager
            .register_shards_by_path(&[&config.merkledb_v2_cache], true)
            .await?;
    } else if config.merkledb_v2_cache == PathBuf::default() {
        info!("No Merkle DB Cache specified.");
    } else {
        warn!(
            "Merkle DB Cache path {:?} does not exist, skipping registration.",
            config.merkledb_v2_cache
        );
    }

    Ok(shard_manager)
}

pub struct RemoteShardInterface {
    pub config: XetConfig,
    pub repo_salt: Option<RepoSalt>,

    pub cas: Option<Arc<dyn Staging + Send + Sync>>,
    pub smudge_query_policy: SmudgeQueryPolicy,
    pub shard_manager: Option<Arc<ShardFileManager>>,
    pub shard_client: Option<Arc<dyn ShardClientInterface>>,
    pub reconstruction_cache:
        Mutex<LruCache<merklehash::MerkleHash, (MDBFileInfo, Option<MerkleHash>)>>,

    // A gate on downloading and registering new shards.
    pub shard_downloads: Arc<singleflight::Group<(), GitXetRepoError>>,
}

impl RemoteShardInterface {
    /// Set up a lightweight version of this that can only use operations that query the remote server;
    /// anything that tries to download or upload shards will cause a runtime error.
    pub async fn new_query_only(config: &XetConfig) -> Result<Arc<Self>> {
        Self::new_impl(config, None, None, None).await
    }

    pub async fn new(
        config: &XetConfig,
        shard_manager: Arc<ShardFileManager>,
        cas: Arc<dyn Staging + Send + Sync>,
        repo_salt: RepoSalt,
    ) -> Result<Arc<Self>> {
        Self::new_impl(config, Some(shard_manager), Some(cas), Some(repo_salt)).await
    }

    async fn new_impl(
        config: &XetConfig,
        shard_manager: Option<Arc<ShardFileManager>>,
        cas: Option<Arc<dyn Staging + Send + Sync>>,
        repo_salt: Option<RepoSalt>,
    ) -> Result<Arc<Self>> {
        let cas_endpoint = config.cas_endpoint().await?;
        info!("data_processing: Cas endpoint = {:?}", cas_endpoint);

        let shard_client = {
            if config.smudge_query_policy != SmudgeQueryPolicy::LocalOnly {
                info!("data_processing: Setting up file reconstructor to query shard server.");
                let (user_id, _) = config.user.get_user_id();

                let shard_file_config = shard_client::ShardConnectionConfig {
                    endpoint: cas_endpoint,
                    user_id,
                    git_xet_version: GIT_XET_VERSION.to_string(),
                };

                Some(shard_client::from_config(shard_file_config).await?)
            } else {
                None
            }
        };

        let shard_manager = if config.smudge_query_policy != SmudgeQueryPolicy::ServerOnly
            && shard_manager.is_none()
        {
            Some(Arc::new(shard_manager_from_config(config).await?))
        } else {
            shard_manager
        };

        Ok(Arc::new(Self {
            config: config.clone(),
            repo_salt,
            smudge_query_policy: config.smudge_query_policy,
            shard_manager,
            shard_client,
            reconstruction_cache: Mutex::new(LruCache::new(FILE_RECONSTRUCTION_CACHE_SIZE)),
            cas,
            shard_downloads: Arc::new(singleflight::Group::new()),
        }))
    }

    pub fn cas(&self) -> Result<Arc<dyn Staging + Send + Sync>> {
        let Some(cas) = self.cas.clone() else {
            // Trigger error and backtrace
            Err(anyhow!("cas requested but has not been configured."))?;
            unreachable!();
        };

        Ok(cas)
    }

    pub fn shard_client(&self) -> Result<Arc<dyn ShardClientInterface>> {
        let Some(shard_client) = self.shard_client.clone() else {
            // Trigger error and backtrace
            Err(anyhow!(
                "shard_client requested but has not been configured."
            ))?;
            unreachable!();
        };

        Ok(shard_client)
    }

    pub fn shard_manager(&self) -> Result<Arc<ShardFileManager>> {
        let Some(shard_manager) = self.shard_manager.clone() else {
            // Trigger error and backtrace
            Err(anyhow!(
                "shard_manager requested but has not been configured."
            ))?;
            unreachable!();
        };

        Ok(shard_manager)
    }

    pub fn shard_prefix(&self) -> String {
        self.config.cas.shard_prefix()
    }

    pub fn shard_cache_dir(&self) -> &Path {
        &self.config.merkledb_v2_cache
    }

    pub fn repo_salt(&self) -> Result<RepoSalt> {
        let Some(salt) = self.repo_salt else {
            // Trigger error and backtrace
            Err(anyhow!(
                "ERROR: Repo salt requested but has not been configured."
            ))?;
            unreachable!();
        };
        Ok(salt)
    }

    async fn query_server_for_file_reconstruction_info(
        &self,
        file_hash: &merklehash::MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        // In this case, no remote to query
        if self.config.smudge_query_policy == SmudgeQueryPolicy::LocalOnly {
            return Ok(None);
        }

        Ok(self
            .shard_client()?
            .get_file_reconstruction_info(file_hash)
            .await?)
    }

    async fn get_file_reconstruction_info_impl(
        &self,
        file_hash: &merklehash::MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        match self.smudge_query_policy {
            SmudgeQueryPolicy::LocalFirst => {
                let local_info = self
                    .shard_manager
                    .as_ref()
                    .ok_or_else(|| {
                        MDBShardError::SmudgeQueryPolicyError(
                        "Require ShardFileManager for smudge query policy other than 'server_only'"
                            .to_owned(),
                    )
                    })?
                    .get_file_reconstruction_info(file_hash)
                    .await?;

                if local_info.is_some() {
                    Ok(local_info)
                } else {
                    Ok(self
                        .query_server_for_file_reconstruction_info(file_hash)
                        .await?)
                }
            }
            SmudgeQueryPolicy::ServerOnly => {
                self.query_server_for_file_reconstruction_info(file_hash)
                    .await
            }
            SmudgeQueryPolicy::LocalOnly => Ok(self
                .shard_manager
                .as_ref()
                .ok_or_else(|| {
                    MDBShardError::SmudgeQueryPolicyError(
                        "Require ShardFileManager for smudge query policy other than 'server_only'"
                            .to_owned(),
                    )
                })?
                .get_file_reconstruction_info(file_hash)
                .await?),
        }
    }

    pub async fn get_file_reconstruction_info(
        &self,
        file_hash: &merklehash::MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        {
            let mut reader = self.reconstruction_cache.lock().unwrap();
            if let Some(res) = reader.get(file_hash) {
                return Ok(Some(res.clone()));
            }
        }
        let response = self.get_file_reconstruction_info_impl(file_hash).await;
        match response {
            Ok(None) => Ok(None),
            Ok(Some(contents)) => {
                // we only cache real stuff
                self.reconstruction_cache
                    .lock()
                    .unwrap()
                    .put(*file_hash, contents.clone());
                Ok(Some(contents))
            }
            Err(e) => Err(e),
        }
    }

    /// Probes which shards provides dedup information for a chunk.
    /// Returns a list of shard hashes with key under 'prefix',
    /// Err(_) if an error occured.
    pub async fn get_dedup_shards(
        &self,
        chunk_hash: &[MerkleHash],
        salt: &RepoSalt,
    ) -> Result<Vec<MerkleHash>> {
        if chunk_hash.is_empty() {
            return Ok(vec![]);
        }

        if let Some(shard_client) = self.shard_client.as_ref() {
            debug!(
                "get_dedup_shards: querying for shards with chunk {:?}",
                chunk_hash[0]
            );
            Ok(shard_client
                .get_dedup_shards(&self.config.cas.shard_prefix(), chunk_hash, salt)
                .await?)
        } else {
            Ok(vec![])
        }
    }

    /// Convenience wrapper of above for single chunk query
    pub async fn query_dedup_shard_by_chunk(
        &self,
        chunk_hash: &MerkleHash,
        salt: &[u8; 32],
    ) -> Result<Option<MerkleHash>> {
        Ok(self.get_dedup_shards(&[*chunk_hash], salt).await?.pop())
    }

    pub fn download_and_register_shard_background(
        &self,
        shard_hash: &MerkleHash,
    ) -> Result<JoinHandle<Result<()>>> {
        let hex_key = shard_hash.hex();

        let prefix = self.config.cas.shard_prefix().to_owned();
        let cache_dir = self.config.merkledb_v2_cache.clone();
        let shard_hash = shard_hash.to_owned();
        let shard_downloads_sf = self.shard_downloads.clone();
        let shard_manager = self.shard_manager()?;
        let cas = self.cas()?;

        Ok(tokio::spawn(async move {
            if shard_manager.shard_is_registered(&shard_hash).await {
                info!("download_and_register_shard: Shard {shard_hash:?} is already registered.");
                return Ok(());
            }

            shard_downloads_sf
                .work(&hex_key, async move {
                    // Download the shard in question.
                    let (shard_file, _) =
                        mdb::download_shard(&cas, &prefix, &shard_hash, &cache_dir).await?;

                    shard_manager
                        .register_shards_by_path(&[shard_file], true)
                        .await?;

                    Ok(())
                })
                .await
                .0?;

            Ok(())
        }))
    }

    pub async fn download_and_register_shard(&self, shard_hash: &MerkleHash) -> Result<()> {
        self.download_and_register_shard_background(shard_hash)?
            .await?
    }

    pub async fn upload_and_register_shard_background(
        &self,
        shard: MDBShardFile,
    ) -> Result<JoinHandle<Result<()>>> {
        let salt = self.repo_salt()?;
        let cas = self.cas()?;
        let shard_client = self.shard_client()?;
        let shard_prefix = self.shard_prefix();

        Ok(tokio::spawn(async move {
            // 1. Upload directly to CAS.
            // 2. Sync to server.
            info!(
                "Uploading shard {shard_prefix}/{:?} from staging area to CAS.",
                &shard.shard_hash
            );

            let data = std::fs::read(&shard.path)?;
            let data_len = data.len();

            // Upload the shard.
            cas.put_bypass_stage(
                &shard_prefix,
                &shard.shard_hash,
                data,
                vec![data_len as u64],
            )
            .await?;

            info!(
                "Registering shard {shard_prefix}/{:?} with shard server.",
                &shard.shard_hash
            );

            // That succeeded, so if we made it to this point, we know that the shard is present in the CAS and the
            // register_shard routine should succeed.
            shard_client
                .register_shard(&shard_prefix, &shard.shard_hash, false, &salt)
                .await?;

            info!(
                "Shard {shard_prefix}/{:?} upload + sync successful.",
                &shard.shard_hash
            );

            Ok(())
        }))
    }

    pub async fn upload_and_register_shard(&self, shard: MDBShardFile) -> Result<()> {
        self.upload_and_register_shard_background(shard)
            .await?
            .await?
    }
}
