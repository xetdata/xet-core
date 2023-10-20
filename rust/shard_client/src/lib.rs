use async_trait::async_trait;
use local_shard_client::LocalShardClient;
use mdb_shard::{error::Result, shard_file_reconstructor::FileReconstructor};
use merklehash::MerkleHash;
use std::{path::PathBuf, str::FromStr, sync::Arc};

mod local_shard_client;
mod shard_client;

// we reexport FileDataSequenceEntry
use crate::shard_client::GrpcShardClient;

pub use mdb_shard::file_structs::FileDataSequenceEntry;

/// Container for information required to set up and handle
/// Shard connections
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardConnectionConfig {
    pub endpoint: String,
    pub user_id: String,
    pub git_xet_version: String,
}

impl ShardConnectionConfig {
    /// creates a new ShardConnectionConfig with given endpoint and user_id
    pub fn new(endpoint: String, user_id: String, git_xet_version: String) -> Self {
        ShardConnectionConfig {
            endpoint,
            user_id,
            git_xet_version,
        }
    }
}

pub trait ShardClientInterface: RegistrationClient + FileReconstructor + Send + Sync {}

/// A Client to the Shard service. The shard service
/// provides for
/// 1. the ingestion of Shard information from CAS to the Shard service
/// 2. querying of file->reconstruction information
#[async_trait]
pub trait RegistrationClient {
    /// Requests the service to add a shard file currently stored in CAS under the prefix/hash
    async fn register_shard(&self, prefix: &str, hash: &MerkleHash, force: bool) -> Result<()>;
}

pub async fn from_config(
    shard_connection_config: ShardConnectionConfig,
) -> Result<Arc<dyn ShardClientInterface>> {
    let ret: Arc<dyn ShardClientInterface>;

    if let Some(local_path) = shard_connection_config.endpoint.strip_prefix("local://") {
        // Create a local config on this path.

        ret = Arc::new(LocalShardClient::new(PathBuf::from_str(local_path).unwrap()).await?);
    } else {
        ret = Arc::new(GrpcShardClient::from_config(shard_connection_config).await?)
    }

    Ok(ret)
}
