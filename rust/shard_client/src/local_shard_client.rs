use async_trait::async_trait;
use bincode::Options;
use cas::safeio::write_all_file_safe;
use cas_client::{Client, LocalClient};
use itertools::Itertools;
use mdb_shard::error::{MDBShardError, Result};
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::shard_dedup_probe::ShardDedupProber;
use mdb_shard::{shard_file_reconstructor::FileReconstructor, ShardFileManager};
use mdb_shard::{MDBShardFile, MDBShardInfo};
use merkledb::aggregate_hashes::with_salt;
use merklehash::MerkleHash;
use std::collections::HashMap;
use std::io::{BufReader, Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{RegistrationClient, ShardClientInterface};

struct LocalGlobalDedupTable {
    table: Arc<RwLock<HashMap<String, String>>>, // map of prefix/chunk_hash -> prefix/shard_hash
    path: PathBuf,
}

impl LocalGlobalDedupTable {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        if !path.as_ref().exists() {
            Ok(Self {
                table: Arc::new(RwLock::new(Default::default())),
                path: path.as_ref().into(),
            })
        } else {
            let reader = BufReader::new(std::fs::File::open(&path)?);
            let table = Self::decode(reader)?;

            Ok(Self {
                table: Arc::new(RwLock::new(table)),
                path: path.as_ref().into(),
            })
        }
    }

    pub async fn batch_add(
        &self,
        chunk_hashes: &[MerkleHash],
        shard_hash: &MerkleHash,
        prefix: &str,
        salt: &[u8; 32],
    ) {
        let mut table_write_guard = self.table.write().await;
        chunk_hashes.iter().for_each(|chunk| {
            let maybe_salted_chunk_hash = with_salt(chunk, salt).ok();
            if let Some(salted_chunk_hash) = maybe_salted_chunk_hash {
                table_write_guard.insert(
                    format!("{prefix}/{salted_chunk_hash}"),
                    format!("{prefix}/{shard_hash}"),
                );
            }
        });
    }

    pub async fn query(&self, salted_chunk_hash: &[MerkleHash], prefix: &str) -> Vec<MerkleHash> {
        let table_read_guard = self.table.read().await;

        salted_chunk_hash
            .iter()
            .filter_map(|chunk| {
                let key = format!("{prefix}/{chunk}");
                table_read_guard.get(&key).and_then(|val| {
                    // found key
                    let Some((v_prefix, v_shard)) = val.split('/').collect_tuple() else {
                        return None;
                    };
                    // parse correctly
                    if v_prefix == prefix {
                        // prefix match
                        MerkleHash::from_hex(v_shard).ok()
                    } else {
                        None
                    }
                })
            })
            .collect_vec()
    }

    fn decode(reader: impl Read) -> Result<HashMap<String, String>> {
        let options = bincode::DefaultOptions::new().with_fixint_encoding();
        options.deserialize_from(reader).map_err(|_| {
            MDBShardError::Other("Unable to deserialize a LocalGlobalDedupTable".into())
        })
    }

    async fn encode(&self, write: impl Write) -> Result<()> {
        let table_read_guard = self.table.read().await;
        let options = bincode::DefaultOptions::new().with_fixint_encoding();
        options
            .serialize_into(write, &table_read_guard.clone())
            .map_err(|_| MDBShardError::Other("Unable to serialize a LocalGlobalDedupTable".into()))
    }

    pub async fn flush(&self) -> Result<()> {
        let mut writer = Cursor::new(Vec::<u8>::new());

        self.encode(&mut writer).await?;

        write_all_file_safe(&self.path, &writer.into_inner())?;

        Ok(())
    }
}

/// This creates a persistent local shard client that simulates the shard server.  It
/// Is intended to use for testing interactions between local repos that would normally
/// require the use of the remote shard server.  
pub struct LocalShardClient {
    shard_manager: ShardFileManager,
    cas: LocalClient,
    shard_directory: PathBuf,
    global_dedup: LocalGlobalDedupTable,
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

        let global_dedup =
            LocalGlobalDedupTable::load(cas_directory.join("ddb").join("chunk2shard.db")).await?;

        Ok(LocalShardClient {
            shard_manager,
            cas,
            shard_directory,
            global_dedup,
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

    async fn register_shard_with_salt(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        force: bool,
        salt: &[u8; 32],
    ) -> Result<()> {
        self.register_shard(prefix, hash, force).await?;

        let Some(shard) = self.shard_manager.get_shard_handle(hash, false).await else {
            return Err(MDBShardError::ShardNotFound(*hash));
        };

        let mut shard_reader = shard.get_reader()?;

        let chunk_hashes = MDBShardInfo::read_cas_chunks_for_global_dedup(&mut shard_reader, 1024)?;

        self.global_dedup
            .batch_add(&chunk_hashes, hash, prefix, salt)
            .await;

        self.global_dedup.flush().await
    }
}

#[async_trait]
impl ShardDedupProber for LocalShardClient {
    async fn get_dedup_shards(
        &self,
        prefix: &str,
        chunk_hash: &[MerkleHash],
        salt: &[u8; 32],
    ) -> Result<Vec<MerkleHash>> {
        let salted_chunk_hash = chunk_hash
            .iter()
            .filter_map(|chunk| with_salt(&chunk, salt).ok())
            .collect_vec();
        Ok(self.global_dedup.query(&salted_chunk_hash, prefix).await)
    }
}

impl ShardClientInterface for LocalShardClient {}
