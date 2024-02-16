use crate::error::{Result, ShardClientError};
use bincode::Options;
use heed::types::*;
use heed::{Database, EnvOpenOptions};
use itertools::Itertools;
use merkledb::aggregate_hashes::with_salt;
use merklehash::MerkleHash;
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use tokio::sync::Mutex;

#[derive(Default, Debug, Serialize, Deserialize)]
struct ValueType {
    prefix: String,
    hash: MerkleHash,
}

impl ValueType {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = vec![];
        let options = bincode::DefaultOptions::new().with_fixint_encoding();
        options.serialize_into(&mut bytes, self).map_err(|_| {
            ShardClientError::DataParsingError(
                "Unable to serialize a global dedup ValueType".into(),
            )
        })?;

        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let options = bincode::DefaultOptions::new().with_fixint_encoding();
        options.deserialize_from(bytes).map_err(|_| {
            ShardClientError::DataParsingError(
                "Unable to deserialize a global dedup ValueType".into(),
            )
        })
    }
}

pub struct DiskBasedGlobalDedupTable {
    table: Arc<Mutex<DB>>, // map of prefix/chunk_hash -> prefix/shard_hash
}

impl DiskBasedGlobalDedupTable {
    pub fn open_or_create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        let opt = rusty_leveldb::Options {
            create_if_missing: true,
            ..Default::default()
        };

        let db = DB::open(path, opt)?;

        Ok(Self {
            table: Arc::new(Mutex::new(db)),
        })
    }

    pub async fn batch_add(
        &self,
        chunk_hashes: &[MerkleHash],
        shard_hash: &MerkleHash,
        prefix: &str,
        salt: &[u8; 32],
    ) -> Result<()> {
        let mut write_batch = WriteBatch::new();

        chunk_hashes.iter().for_each(|chunk| {
            let maybe_salted_chunk_hash = with_salt(chunk, salt).ok();
            if let Some(salted_chunk_hash) = maybe_salted_chunk_hash {
                let k = format!("{prefix}/{salted_chunk_hash}");
                let maybe_value = ValueType {
                    prefix: prefix.to_string(),
                    hash: *shard_hash,
                }
                .to_bytes();

                maybe_value
                    .iter()
                    .for_each(|v| write_batch.put(k.as_bytes(), v));
            }
        });

        let mut table_write_guard = self.table.lock().await;
        // write and sync to disk
        table_write_guard.write(write_batch, true)?;

        Ok(())
    }

    pub async fn query(&self, salted_chunk_hash: &[MerkleHash], prefix: &str) -> Vec<MerkleHash> {
        let mut table_read_guard = self.table.lock().await;

        salted_chunk_hash
            .iter()
            .filter_map(|chunk| {
                let k = format!("{prefix}/{chunk}");
                table_read_guard.get(k.as_bytes()).and_then(|value| {
                    // found key
                    let Ok(v) = ValueType::from_bytes(&value) else {
                        return None;
                    };
                    // parse correctly
                    if v.prefix == prefix {
                        // prefix match
                        Some(v.hash)
                    } else {
                        None
                    }
                })
            })
            .collect_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::DiskBasedGlobalDedupTable;
    use itertools::Itertools;
    use mdb_shard::shard_format::test_routines::rng_hash;
    use merkledb::aggregate_hashes::with_salt;
    use rand::{thread_rng, Rng};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_basic_insert_retrieval() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;

        let db_file = tempdir.path().join("db");

        let db = DiskBasedGlobalDedupTable::open_or_create(&db_file)?;

        let mut rng = thread_rng();

        let prefix = "default";
        let chunk_hash = rng_hash(rng.gen());
        let shard_hash = rng_hash(rng.gen());
        let salt: [u8; 32] = rng.gen();

        db.batch_add(&[chunk_hash], &shard_hash, "default", &salt)
            .await?;

        let query_shard = db.query(&[with_salt(&chunk_hash, &salt)?], prefix).await;

        assert_eq!(query_shard.len(), 1);
        assert_eq!(query_shard.first(), Some(&shard_hash));

        Ok(())
    }

    #[tokio::test]
    async fn test_multithread_insert_retrieval() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;

        let db_file = tempdir.path().join("db");

        let db = Arc::new(DiskBasedGlobalDedupTable::open_or_create(&db_file)?);

        let mut rng = thread_rng();
        let prefix = "default";
        let chunk_hashes = (0..10).map(|_| rng_hash(rng.gen())).collect_vec();
        let shard_hashes = (0..10).map(|_| rng_hash(rng.gen())).collect_vec();
        let salt: [u8; 32] = rng.gen();

        // insert to the db concurrently
        let handles = (0..10)
            .map(|i| {
                let chunk_hash = chunk_hashes[i];
                let shard_hash = shard_hashes[i];
                let db = db.clone();

                tokio::spawn(async move {
                    db.batch_add(&[chunk_hash], &shard_hash, prefix, &salt)
                        .await
                })
            })
            .collect_vec();

        for h in handles {
            let _ = h.await?;
        }

        // now examine that inserts succeeded
        for i in 0..10 {
            let chunk_hash = chunk_hashes[i];
            let shard_hash = shard_hashes[i];
            let query_shard = db.query(&[with_salt(&chunk_hash, &salt)?], prefix).await;

            assert_eq!(query_shard.len(), 1);
            assert_eq!(query_shard.first(), Some(&shard_hash));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_db_instance_insert_retrieval() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;

        let db_file = tempdir.path().join("db");

        let mut rng = thread_rng();
        let prefix = "default";
        let chunk_hashes = (0..1000).map(|_| rng_hash(rng.gen())).collect_vec();
        let shard_hashes = (0..10).map(|_| rng_hash(rng.gen())).collect_vec();
        let salt: [u8; 32] = rng.gen();

        // insert to the db concurrently
        let handles = (0..10)
            .map(|i| {
                let chunk_hashes = chunk_hashes[i * 100..(i + 1) * 100].to_vec();
                let shard_hash = shard_hashes[i];
                let db_file = db_file.clone();

                tokio::spawn(async move {
                    let db = DiskBasedGlobalDedupTable::open_or_create(&db_file).unwrap();
                    db.batch_add(&chunk_hashes, &shard_hash, prefix, &salt)
                        .await
                })
            })
            .collect_vec();

        for h in handles {
            let _ = h.await?;
        }

        // now examine that inserts succeeded
        let db = DiskBasedGlobalDedupTable::open_or_create(&db_file)?;
        for i in 0..10 {
            let shard_hash = shard_hashes[i];

            for chunk_hash in &chunk_hashes[i * 100..(i + 1) * 100] {
                let query_shard = db.query(&[with_salt(&chunk_hash, &salt)?], prefix).await;

                assert_eq!(query_shard.len(), 1);
                assert_eq!(query_shard.first(), Some(&shard_hash));
            }
        }

        Ok(())
    }
}
