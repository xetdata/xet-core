#![allow(
    unknown_lints,
    renamed_and_removed_lints,
    clippy::blocks_in_conditions,
    clippy::blocks_in_if_conditions
)]
// use shard_client::GrpcShardClient;
use std::{path::PathBuf, str::FromStr, sync::Arc};

use async_trait::async_trait;
use tracing::info;

use local_shard_client::LocalShardClient;
// we reexport FileDataSequenceEntry
pub use mdb_shard::file_structs::FileDataSequenceEntry;
use mdb_shard::shard_dedup_probe::ShardDedupProber;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use merklehash::MerkleHash;

use crate::error::Result;

pub mod error;
mod global_dedup_table;
mod local_shard_client;
mod shard_client;

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

pub trait ShardClientInterface:
RegistrationClient + FileReconstructor + ShardDedupProber + Send + Sync
{}

/// A Client to the Shard service. The shard service
/// provides for
/// 1. the ingestion of Shard information from CAS to the Shard service
/// 2. querying of file->reconstruction information
#[async_trait(? Send)]
pub trait RegistrationClient {
    /// Requests the service to add a shard file currently stored in CAS under the prefix/hash
    async fn register_shard_v1(&self, prefix: &str, hash: &MerkleHash, force: bool) -> Result<()>;

    /// Requests the service to add a shard file currently stored in CAS under the prefix/hash,
    /// and add chunk->shard information to the global dedup service.
    async fn register_shard_with_salt(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        force: bool,
        salt: &[u8; 32],
    ) -> Result<()>;

    async fn register_shard(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        force: bool,
        salt: &[u8; 32],
    ) -> Result<()> {
        // Attempts to register a shard using the salted version; if that fails,
        // then reverts to the unsalted v1 version.
        if let Err(e) = self
            .register_shard_with_salt(prefix, hash, force, salt)
            .await
        {
            info!("register_shard: register_shard_with_salt had error {e:?}; reverting to register_shard_v1.");
            self.register_shard_v1(prefix, hash, force).await
        } else {
            Ok(())
        }
    }
}

pub async fn from_config(
    shard_connection_config: ShardConnectionConfig,
) -> Result<Arc<dyn ShardClientInterface>> {
    // let ret: Arc<dyn ShardClientInterface> = Arc::new(());
    // 
    // if let Some(local_path) = shard_connection_config.endpoint.strip_prefix("local://") {
    // Create a local config on this path.

    let ret = Arc::new(LocalShardClient::new(PathBuf::from_str(shard_connection_config.endpoint.strip_prefix("local://").unwrap()).unwrap()).await?);
    // }
    //     ret = Arc::new(GrpcShardClient::from_config(shard_connection_config).await?)
    // }

    Ok(ret)
}
