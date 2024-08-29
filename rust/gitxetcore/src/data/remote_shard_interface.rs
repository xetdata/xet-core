use super::cas_interface::create_shard_client;
use super::configurations::{FileQueryPolicy, StorageConfig};
use super::errors::{DataProcessingError, Result};
use super::mdb;
use super::shard_interface::create_shard_manager;
use crate::constants::FILE_RECONSTRUCTION_CACHE_SIZE;
use crate::git_integration::git_repo_salt::RepoSalt;

use cas::singleflight;
use cas_client::Staging;
use lru::LruCache;
use mdb_shard::{
    error::MDBShardError, file_structs::MDBFileInfo, shard_file_manager::ShardFileManager,
    shard_file_reconstructor::FileReconstructor, MDBShardFile,
};
use merklehash::MerkleHash;
use shard_client::ShardClientInterface;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, info};

pub struct RemoteShardInterface {
    pub file_query_policy: FileQueryPolicy,
    pub shard_prefix: String,
    pub shard_cache_directory: Option<PathBuf>,

    pub repo_salt: Option<RepoSalt>,

    pub cas: Option<Arc<dyn Staging + Send + Sync>>,
    pub shard_manager: Option<Arc<ShardFileManager>>,
    pub shard_client: Option<Arc<dyn ShardClientInterface>>,
    pub reconstruction_cache:
        Mutex<LruCache<merklehash::MerkleHash, (MDBFileInfo, Option<MerkleHash>)>>,

    // A gate on downloading and registering new shards.
    pub shard_downloads: Arc<singleflight::Group<(), DataProcessingError>>,
}

impl RemoteShardInterface {
    /// Set up a lightweight version of this that can only use operations that query the remote server;
    /// anything that tries to download or upload shards will cause a runtime error.
    pub async fn new_query_only(
        file_query_policy: FileQueryPolicy,
        shard_storage_config: &StorageConfig,
    ) -> Result<Arc<Self>> {
        Self::new(file_query_policy, shard_storage_config, None, None, None).await
    }

    pub async fn new(
        file_query_policy: FileQueryPolicy,
        shard_storage_config: &StorageConfig,
        shard_manager: Option<Arc<ShardFileManager>>,
        cas: Option<Arc<dyn Staging + Send + Sync>>,
        repo_salt: Option<RepoSalt>,
    ) -> Result<Arc<Self>> {
        let shard_client = {
            if file_query_policy != FileQueryPolicy::LocalOnly {
                debug!("data_processing: Setting up file reconstructor to query shard server.");
                create_shard_client(shard_storage_config).await.ok()
            } else {
                None
            }
        };

        let shard_manager =
            if file_query_policy != FileQueryPolicy::ServerOnly && shard_manager.is_none() {
                Some(Arc::new(create_shard_manager(shard_storage_config).await?))
            } else {
                shard_manager
            };

        Ok(Arc::new(Self {
            file_query_policy,
            shard_prefix: shard_storage_config.prefix.clone(),
            shard_cache_directory: shard_storage_config
                .cache_config
                .as_ref()
                .and_then(|cf| Some(cf.cache_directory.clone())),
            repo_salt,
            shard_manager,
            shard_client,
            reconstruction_cache: Mutex::new(LruCache::new(
                std::num::NonZero::new(FILE_RECONSTRUCTION_CACHE_SIZE).unwrap(),
            )),
            cas,
            shard_downloads: Arc::new(singleflight::Group::new()),
        }))
    }

    pub fn cas(&self) -> Result<Arc<dyn Staging + Send + Sync>> {
        let Some(cas) = self.cas.clone() else {
            // Trigger error and backtrace
            return Err(DataProcessingError::CASConfigError(
                "tried to contact CAS service but cas client was not configured".to_owned(),
            ))?;
        };

        Ok(cas)
    }

    pub fn shard_client(&self) -> Result<Arc<dyn ShardClientInterface>> {
        let Some(shard_client) = self.shard_client.clone() else {
            // Trigger error and backtrace
            return Err(DataProcessingError::FileQueryPolicyError(format!(
                "tried to contact Shard service but FileQueryPolicy was set to {:?}",
                self.file_query_policy
            )));
        };

        Ok(shard_client)
    }

    pub fn shard_manager(&self) -> Result<Arc<ShardFileManager>> {
        let Some(shard_manager) = self.shard_manager.clone() else {
            // Trigger error and backtrace
            return Err(DataProcessingError::FileQueryPolicyError(format!(
                "tried to use local Shards but FileQueryPolicy was set to {:?}",
                self.file_query_policy
            )));
        };

        Ok(shard_manager)
    }

    pub fn repo_salt(&self) -> Result<RepoSalt> {
        // repo salt is optional for dedup
        Ok(self.repo_salt.unwrap_or_default())
    }

    async fn query_server_for_file_reconstruction_info(
        &self,
        file_hash: &merklehash::MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        // In this case, no remote to query
        if self.file_query_policy == FileQueryPolicy::LocalOnly {
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
        match self.file_query_policy {
            FileQueryPolicy::LocalFirst => {
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
            FileQueryPolicy::ServerOnly => {
                self.query_server_for_file_reconstruction_info(file_hash)
                    .await
            }
            FileQueryPolicy::LocalOnly => Ok(self
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
                .get_dedup_shards(&self.shard_prefix, chunk_hash, salt)
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

        let prefix = self.shard_prefix.to_owned();

        let Some(cache_dir) = self.shard_cache_directory.clone() else {
            return Err(DataProcessingError::ShardConfigError(
                "cache directory not configured".to_owned(),
            ));
        };

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
                        mdb::download_shard(&cas, &prefix, &shard_hash, &cache_dir)
                            .await
                            .map_err(|e| DataProcessingError::InternalError(format!("{e:?}")))?;

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
        let shard_prefix = self.shard_prefix.clone();

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
