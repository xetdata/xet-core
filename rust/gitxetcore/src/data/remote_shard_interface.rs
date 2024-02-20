use std::path::PathBuf;
use std::{str::FromStr, sync::Arc};

use crate::config::XetConfig;
use crate::errors::{GitXetRepoError, Result};
use cas::singleflight;
use cas_client::Staging;
use lru::LruCache;

use mdb_shard::{
    error::MDBShardError, file_structs::MDBFileInfo, shard_file_manager::ShardFileManager,
    shard_file_reconstructor::FileReconstructor,
};
use merklehash::MerkleHash;
use shard_client::ShardClientInterface;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::constants::{FILE_RECONSTRUCTION_CACHE_SIZE, GIT_XET_VERSION};
use std::sync::Mutex;

use super::mdb;

#[derive(PartialEq, Default, Clone, Debug, Copy)]
pub enum SmudgeQueryPolicy {
    #[default]
    LocalFirst,
    ServerOnly,
    LocalOnly,
}

impl FromStr for SmudgeQueryPolicy {
    type Err = std::io::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
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
    pub async fn new_from_config(
        config: &XetConfig,
        shard_manager: Option<Arc<ShardFileManager>>,
        cas: Option<Arc<dyn Staging + Send + Sync>>,
    ) -> Result<Self> {
        info!("data_processing: Cas endpoint = {:?}", &config.cas.endpoint);

        let shard_client = {
            if config.smudge_query_policy != SmudgeQueryPolicy::LocalOnly {
                info!("data_processing: Setting up file reconstructor to query shard server.");
                let (user_id, _) = config.user.get_user_id();

                let shard_file_config = shard_client::ShardConnectionConfig {
                    endpoint: config.cas.endpoint.clone(),
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

        Ok(Self {
            config: config.clone(),
            smudge_query_policy: config.smudge_query_policy,
            shard_manager,
            shard_client,
            reconstruction_cache: Mutex::new(LruCache::new(FILE_RECONSTRUCTION_CACHE_SIZE)),
            cas,
            shard_downloads: Arc::new(singleflight::Group::new()),
        })
    }

    pub async fn new_local(shard_manager: Arc<ShardFileManager>) -> Result<Self> {
        Ok(Self {
            config: XetConfig::empty(),
            smudge_query_policy: SmudgeQueryPolicy::LocalOnly,
            shard_manager: Some(shard_manager),
            shard_client: None,
            cas: None,
            reconstruction_cache: Mutex::new(LruCache::new(FILE_RECONSTRUCTION_CACHE_SIZE)),
            shard_downloads: Arc::new(singleflight::Group::new()),
        })
    }

    pub async fn query_server(
        &self,
        file_hash: &merklehash::MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        if let Some(client) = &self.shard_client {
            Ok(client.get_file_reconstruction_info(file_hash).await?)
        } else {
            error!(
                "Runtime Error: File info requested from server when server is not initialized."
            );
            Ok(None)
        }
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
                    Ok(self.query_server(file_hash).await?)
                }
            }
            SmudgeQueryPolicy::ServerOnly => self.query_server(file_hash).await,
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
        salt: &[u8; 32],
    ) -> Result<Vec<MerkleHash>> {
        if let Some(shard_client) = self.shard_client.as_ref() {
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
    ) -> JoinHandle<Result<()>> {
        let hex_key = shard_hash.hex();

        let Some(cas) = self.cas.clone() else {
            info!("download_and_register_shard: CAS not specified, skipping download of shard {hex_key}.");
            return tokio::spawn(async { Ok(()) });
        };

        let Some(shard_manager) = self.shard_manager.clone() else {
            info!("download_and_register_shard: Shard manager not specified, skipping download and registration of shard {hex_key}.");
            return tokio::spawn(async { Ok(()) });
        };

        let prefix = self.config.cas.shard_prefix().to_owned();
        let cache_dir = self.config.merkledb_v2_cache.clone();
        let shard_hash = shard_hash.to_owned();
        let shard_downloads_sf = self.shard_downloads.clone();

        tokio::spawn(async move {
            shard_downloads_sf
                .work(&hex_key, async move {
                    if shard_manager.shard_is_registered(&shard_hash).await {
                        return Ok(());
                    }

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
        })
    }

    pub async fn download_and_register_shard(&self, shard_hash: &MerkleHash) -> Result<()> {
        self.download_and_register_shard_background(shard_hash)
            .await?
    }
}
