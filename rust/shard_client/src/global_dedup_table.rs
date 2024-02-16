use crate::error::{Result, ShardClientError};
use bincode::Options;
use heed::types::*;
use heed::{Database, EnvOpenOptions};
use itertools::Itertools;
use merkledb::aggregate_hashes::with_salt;
use merklehash::MerkleHash;
use std::collections::HashMap;
use std::{path::Path, sync::Arc};
use tokio::sync::{Mutex, RwLock};
use tracing::info;

type DB = heed::Database<MerkleHash, MerkleHash>;

pub struct DiskBasedGlobalDedupTable {
    env: heed::Env,
    table: RwLock<HashMap<String, Arc<DB>>>, // map of chunk_hash -> shard_hash
}

impl DiskBasedGlobalDedupTable {
    pub fn open_or_create(path: impl AsRef<Path>) -> Result<Self> {
        let db_path = path.as_ref().join("global_shard_dedup.db");
        info!("Using {db_path:?} as path to global shard dedup database.");

        std::fs::create_dir_all(&db_path)?;
        let env = EnvOpenOptions::new().open(&db_path)?;

        Ok(Self {
            env,
            table: RwLock::new(HashMap::new()),
        })
    }

    async fn get_db(&self, prefix: &str) -> Result<Arc<DB>> {
        if let Some(db) = self.table.read().await.get(prefix).cloned() {
            return Ok(db);
        } else {
            let mut write_lock = self.table.write().await;

            match write_lock.entry(prefix.to_owned()) {
                std::collections::hash_map::Entry::Occupied(db) => Ok(db.get().clone()),
                std::collections::hash_map::Entry::Vacant(entry_ref) => {
                    let db = Arc::new(self.env.create_database(Some(prefix))?);
                    entry_ref.insert(db.clone());
                    Ok(db)
                }
            }
        }
    }

    pub async fn batch_add(
        &self,
        chunk_hashes: &[MerkleHash],
        shard_hash: &MerkleHash,
        prefix: &str,
        salt: &[u8; 32],
    ) -> Result<()> {
        let db = self.get_db(prefix).await?;

        let mut write_txn = self.env.write_txn()?;

        chunk_hashes.iter().for_each(|chunk| {
            let maybe_salted_chunk_hash = with_salt(chunk, salt).ok();
            if let Some(salted_chunk_hash) = maybe_salted_chunk_hash {
                db.put(&mut write_txn, salted_chunk_hash, shard_hash)?;
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
