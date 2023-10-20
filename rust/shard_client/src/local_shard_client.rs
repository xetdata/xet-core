use async_trait::async_trait;
use cas_client::{Client, LocalClient};
use mdb_shard::error::{MDBShardError, Result};
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::MDBShardFile;
use mdb_shard::{shard_file_reconstructor::FileReconstructor, ShardFileManager};
use merklehash::MerkleHash;
use std::io::Cursor;
use std::path::PathBuf;

use crate::{RegistrationClient, ShardClientInterface};

/// This creates a persistent local shard client that simulates the shard server.  It
/// Is intended to use for testing interactions between local repos that would normally
/// require the use of the remote shard server.  
pub struct LocalShardClient {
    shard_manager: ShardFileManager,
    cas: LocalClient,
    shard_directory: PathBuf,
}

impl LocalShardClient {
    pub async fn new(cas_directory: PathBuf) -> Result<Self> {
        let shard_directory = cas_directory.join("shards");
        if !shard_directory.exists() {
            std::fs::create_dir_all(&shard_directory).map_err(|e| {
                MDBShardError::Other(format!(
                    "Error creating local shard directory {shard_directory:?}: {e:?}."
                ))
            })?;
        }

        let shard_manager = ShardFileManager::new(&shard_directory).await?;
        shard_manager
            .register_shards_by_path(&[&shard_directory], true)
            .await?;

        let cas = LocalClient::new(&cas_directory, false);

        Ok(LocalShardClient {
            shard_manager,
            cas,
            shard_directory,
        })
    }
}

#[async_trait]
impl FileReconstructor for LocalShardClient {
    /// Query the shard server for the file reconstruction info.
    /// Returns the FileInfo for reconstructing the file and the shard ID that
    /// defines the file info.
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        self.shard_manager
            .get_file_reconstruction_info(file_hash)
            .await
    }
}

#[async_trait]
impl RegistrationClient for LocalShardClient {
    async fn register_shard(&self, prefix: &str, hash: &MerkleHash, _force: bool) -> Result<()> {
        // Dump the shard from the CAS to the shard directory.  Go through the local client to unpack this.

        let shard_data = self.cas.get(prefix, hash).await.map_err(|e| {
            MDBShardError::Other(format!(
                "Error retrieving shard content from cas for local registration: {e:?}."
            ))
        })?;

        let shard = MDBShardFile::write_out_from_reader(
            &self.shard_directory,
            &mut Cursor::new(shard_data),
        )?;

        self.shard_manager.register_shards(&[shard], true).await?;

        Ok(())
    }
}

impl ShardClientInterface for LocalShardClient {}
