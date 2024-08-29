use super::configurations::{shard_storage_config_from, StorageConfig};
use super::errors::Result;
use crate::config::XetConfig;
use mdb_shard::ShardFileManager;
use tracing::warn;

pub async fn old_create_shard_manager(xet: &XetConfig) -> Result<ShardFileManager> {
    let shard_storage_config = shard_storage_config_from(xet).await?;
    create_shard_manager(&shard_storage_config).await
}

pub async fn create_shard_manager(
    shard_storage_config: &StorageConfig,
) -> Result<ShardFileManager> {
    let shard_session_directory = shard_storage_config
        .staging_directory
        .as_ref()
        .expect("Need shard staging directory to create ShardFileManager");
    let shard_cache_directory = &shard_storage_config
        .cache_config
        .as_ref()
        .expect("Need shard cache directory to create ShardFileManager")
        .cache_directory;

    let shard_manager = ShardFileManager::new(&shard_session_directory).await?;

    if shard_cache_directory.exists() {
        shard_manager
            .register_shards_by_path(&[shard_cache_directory], true)
            .await?;
    } else {
        warn!(
            "Merkle DB Cache path {:?} does not exist, skipping registration.",
            shard_cache_directory
        );
    }

    Ok(shard_manager)
}
