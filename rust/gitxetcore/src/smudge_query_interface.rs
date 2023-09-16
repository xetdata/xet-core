use std::path::PathBuf;
use std::{str::FromStr, sync::Arc};

use crate::config::XetConfig;
use async_trait::async_trait;
use lru::LruCache;

use mdb_shard::{
    error::MDBShardError, file_structs::MDBFileInfo, shard_file_manager::ShardFileManager,
    shard_file_reconstructor::FileReconstructor,
};
use merklehash::MerkleHash;
use tracing::{info, warn};

use crate::constants::FILE_RECONSTRUCTION_CACHE_SIZE;
use crate::data_processing_v2::GIT_XET_VERION;
use shard_client::GrpcShardClient;
use std::sync::Mutex;

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

pub async fn shard_manager_from_config(
    config: &XetConfig,
) -> Result<ShardFileManager, MDBShardError> {
    let shard_manager = ShardFileManager::new(&config.merkledb_v2_session).await?;

    if config.merkledb_v2_cache.exists() {
        shard_manager
            .register_shards_by_path(&[&config.merkledb_v2_cache], true)
            .await?;
    } else {
        if config.merkledb_v2_cache == PathBuf::default() {
            info!("No Merkle DB Cache specified.");
        } else {
            warn!(
                "Merkle DB Cache path {:?} does not exist, skipping registration.",
                config.merkledb_v2_cache
            );
        }
    }

    Ok(shard_manager)
}

pub struct FileReconstructionInterface {
    pub smudge_query_policy: SmudgeQueryPolicy,
    pub shard_manager: Arc<ShardFileManager>,
    pub shard_client: Option<GrpcShardClient>,
    pub reconstruction_cache:
        Mutex<LruCache<merklehash::MerkleHash, (MDBFileInfo, Option<MerkleHash>)>>,
}

impl FileReconstructionInterface {
    pub async fn new_from_config(
        config: &XetConfig,
        shard_manager: Arc<ShardFileManager>,
    ) -> Result<Self, MDBShardError> {
        info!("data_processing: Cas endpoint = {:?}", &config.cas.endpoint);

        let mut smudge_query_policy = config.smudge_query_policy;

        if smudge_query_policy != SmudgeQueryPolicy::LocalOnly
            && config.cas.endpoint.starts_with("local://")
        {
            info!("Config mismatch: Overriding smudge_query_policy due to local cas endpoint.");
            smudge_query_policy = SmudgeQueryPolicy::LocalOnly;
        }

        let shard_client = {
            if smudge_query_policy != SmudgeQueryPolicy::LocalOnly {
                info!("data_processing: Setting up file reconstructor to query shard server.");
                let (user_id, _) = config.user.get_user_id();

                let shard_file_config = shard_client::ShardConnectionConfig {
                    endpoint: config.cas.endpoint.clone(),
                    user_id,
                    git_xet_version: GIT_XET_VERION.to_string(),
                };

                Some(shard_client::GrpcShardClient::from_config(shard_file_config).await?)
            } else {
                None
            }
        };

        Ok(Self {
            smudge_query_policy,
            shard_manager,
            shard_client,
            reconstruction_cache: Mutex::new(LruCache::new(FILE_RECONSTRUCTION_CACHE_SIZE)),
        })
    }

    pub async fn new_local(shard_manager: Arc<ShardFileManager>) -> Result<Self, MDBShardError> {
        Ok(Self {
            smudge_query_policy: SmudgeQueryPolicy::LocalOnly,
            shard_manager,
            shard_client: None,
            reconstruction_cache: Mutex::new(LruCache::new(FILE_RECONSTRUCTION_CACHE_SIZE)),
        })
    }

    pub async fn query_server(
        &self,
        file_hash: &merklehash::MerkleHash,
    ) -> std::result::Result<Option<(MDBFileInfo, Option<MerkleHash>)>, MDBShardError> {
        if let Some(client) = &self.shard_client {
            client.get_file_reconstruction_info(file_hash).await
        } else {
            Err(MDBShardError::Other(
                "File info requested from server when server is not initialized.".to_owned(),
            ))
        }
    }
    async fn get_file_reconstruction_info_impl(
        &self,
        file_hash: &merklehash::MerkleHash,
    ) -> std::result::Result<Option<(MDBFileInfo, Option<MerkleHash>)>, MDBShardError> {
        match self.smudge_query_policy {
            SmudgeQueryPolicy::LocalFirst => {
                let local_info = self
                    .shard_manager
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
                .get_file_reconstruction_info(file_hash)
                .await?),
        }
    }
}

#[async_trait]
impl FileReconstructor for FileReconstructionInterface {
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &merklehash::MerkleHash,
    ) -> std::result::Result<Option<(MDBFileInfo, Option<MerkleHash>)>, MDBShardError> {
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
}
