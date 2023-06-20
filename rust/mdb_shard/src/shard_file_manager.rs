use crate::error::Result;
use crate::shard_handle::MDBShardFile;
use crate::utils::{shard_file_name, temp_shard_file_name};
use merklehash::MerkleHash;
use std::collections::HashSet;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error};

use crate::shard_file::MDB_SHARD_MIN_TARGET_SIZE;
use crate::{
    cas_structs::*, file_structs::*, shard_file::MDBShardInfo, shard_in_memory::MDBInMemoryShard,
};

/// A wrapper struct for the in-memory shard to make sure that it gets flushed on teardown.
#[derive(Debug)]
struct MDBShardFlushGuard {
    shard: MDBInMemoryShard,
    session_directory: PathBuf,
}

impl Drop for MDBShardFlushGuard {
    fn drop(&mut self) {
        if self.shard.is_empty() {
            return;
        }
        // Flush everything to a shard.
        let temp_file_name = self.session_directory.join(temp_shard_file_name());

        match self.shard.write_to_shard_file(&temp_file_name) {
            Err(e) => {
                error!("Error flushing reconstruction data on shutdown: {e:?}");
            }
            Ok(shard_hash) => {
                let full_file_name = self.session_directory.join(shard_file_name(&shard_hash));
                if let Err(e) = std::fs::rename(&temp_file_name, full_file_name) {
                    error!("Error flushing reconstruction data on shutdown (rename): {e:?}");
                }
            }
        };
    }
}

#[derive(Debug)]
pub struct ShardFileManager {
    shard_file_lookup: Arc<RwLock<Vec<MDBShardFile>>>,
    current_state: Arc<RwLock<MDBShardFlushGuard>>,
    write_directory: PathBuf,
    target_shard_min_size: u64,
}

/// Shard file manager to manage all the shards.  It is fully thread-safe and async enabled.
///
/// Usage:
///
/// // Session directory is where it stores shard and shard state.
/// let mut mng = ShardFileManager::new("<session_directory>")
///
/// // Add other known shards with register_shards.
/// mng.register_shards(&[other shard, directories, etc.])?;
///
/// // Run queries, add data, etc. with get_file_reconstruction_info, chunk_hash_dedup_query,
/// add_cas_block, add_file_reconstruction_info.
///
/// // Finalize by calling process_session_directory
/// let new_shards = mdb.process_session_directory()?;
///
/// // new_shards is the list of new shards for this session.
///
impl ShardFileManager {
    /// Construct a new shard file manager that uses session_directory as the temporary dumping  
    pub async fn new(write_directory: &Path) -> Result<Self> {
        let s = Self {
            shard_file_lookup: Arc::new(RwLock::new(Vec::new())),
            current_state: Arc::new(RwLock::new(MDBShardFlushGuard {
                shard: MDBInMemoryShard::default(),
                session_directory: std::fs::canonicalize(write_directory)?,
            })),
            write_directory: write_directory.to_path_buf(),
            target_shard_min_size: MDB_SHARD_MIN_TARGET_SIZE,
        };

        s.register_shards_by_path(&[&s.write_directory]).await?;

        Ok(s)
    }

    /// Sets the target value of a shard file size.  By default, it is given by MDB_SHARD_MIN_TARGET_SIZE
    pub fn set_target_shard_min_size(&mut self, s: u64) {
        self.target_shard_min_size = s;
    }

    /// Registers all the files in a directory with filenames matching the names
    /// of an MerkleDB shard.
    pub async fn register_shards_by_path(&self, paths: &[&Path]) -> Result<()> {
        let mut new_shards = Vec::new();
        for p in paths {
            new_shards.append(&mut MDBShardFile::load_all(p)?);
        }

        self.register_shards(new_shards).await
    }

    pub async fn register_shards(&self, new_shards: Vec<MDBShardFile>) -> Result<()> {
        // Filter out the shards that we already know about.
        let current_shards;
        {
            let current_lookup = self.shard_file_lookup.read().await;
            current_shards =
                HashSet::<MerkleHash>::from_iter(current_lookup.iter().map(|si| si.shard_hash));
        }

        let mut current_lookup = self.shard_file_lookup.write().await;

        current_lookup.extend(
            new_shards
                .into_iter()
                .filter(|new_si| !current_shards.contains(&new_si.shard_hash)),
        );

        Ok(())
    }

    async fn query_all_shards<Ret, F>(&self, func: &mut F) -> Result<Option<Ret>>
    where
        Ret: Send + Sync,
        F: FnMut(&MDBShardInfo, &mut BufReader<std::fs::File>, &str) -> Result<Option<Ret>>,
    {
        // Need to hold the read lock until all the shards have been fully queried.
        let current_shards = self.shard_file_lookup.read().await;

        // TODO: make this parallel.  Doing so, at least with the futures::stream library,
        // causes really weird Send / Sync errors.  BLEH.
        for si in current_shards.iter() {
            let f = std::fs::File::open(&si.path).unwrap();
            let mut reader = BufReader::with_capacity(2048, f);

            if let Some(fi) = func(&si.shard, &mut reader, si.path.to_str().unwrap())? {
                return Ok(Some(fi));
            }
        }

        Ok(None)
    }

    // Given a file pointer, returns the information needed to reconstruct the file.
    // The information is stored in the destination vector dest_results.  The function
    // returns true if the file hash was found, and false otherwise.
    pub async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<MDBFileInfo>> {
        if *file_hash == MerkleHash::default() {
            return Ok(Some(MDBFileInfo {
                metadata: FileDataSequenceHeader::new(MerkleHash::default(), 0),
                segments: Vec::default(),
            }));
        }

        // First attempt the in-memory version of this.
        {
            let lg = self.current_state.read().await;
            let file_info = lg.shard.get_file_reconstruction_info(file_hash);
            if file_info.is_some() {
                return Ok(file_info);
            }
        }
        self.query_all_shards(&mut |si, f, file_name| {
            debug!("Querying for hash {file_hash:?} in {file_name:?}.");
            si.get_file_reconstruction_info(f, file_hash)
        })
        .await
    }

    // Performs a query of chunk hashes against known chunk hashes, matching
    // as many of the values in query_hashes as possible.  It returns the number
    // of entries matched from the input hashes, the CAS block hash of the match,
    // and the range matched from that block.
    pub async fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> Result<Option<(usize, FileDataSequenceEntry)>> {
        // First attempt the in-memory version of this.
        {
            let lg = self.current_state.read().await;
            let ret = lg.shard.chunk_hash_dedup_query(query_hashes);
            if ret.is_some() {
                return Ok(ret);
            }
        }

        self.query_all_shards(&mut |si, f, file_name| {
            debug!("Querying for hash {:?} in {file_name:?}.", &query_hashes[0]);
            si.chunk_hash_dedup_query(f, query_hashes)
        })
        .await
    }

    /// Add CAS info to the in-memory state.
    pub async fn add_cas_block(&self, cas_block_contents: MDBCASInfo) -> Result<()> {
        let mut lg = self.current_state.write().await;

        if lg.shard.shard_file_size() + cas_block_contents.num_bytes() >= self.target_shard_min_size
        {
            self.flush_internal(&mut lg).await?;
        }

        lg.shard.add_cas_block(cas_block_contents)?;

        Ok(())
    }

    /// Add file reconstruction info to the in-memory state.
    pub async fn add_file_reconstruction_info(&self, file_info: MDBFileInfo) -> Result<()> {
        let mut lg = self.current_state.write().await;

        if lg.shard.shard_file_size() + file_info.num_bytes() >= self.target_shard_min_size {
            self.flush_internal(&mut lg).await?;
        }

        lg.shard.add_file_reconstruction_info(file_info)?;

        Ok(())
    }

    /// Flush the current state of the in-memory lookups to a shard in the session directory,
    /// returning the hash of the shard and the file written, or None if no file was written.
    pub async fn flush(&self) -> Result<Option<(MerkleHash, PathBuf)>> {
        let current_state = self.current_state.clone();

        let mut lg = current_state.write().await;

        self.flush_internal(&mut lg).await
    }

    async fn flush_internal<'a>(
        &'a self,
        mem_shard: &mut tokio::sync::RwLockWriteGuard<'a, MDBShardFlushGuard>,
    ) -> Result<Option<(MerkleHash, PathBuf)>> {
        if mem_shard.shard.is_empty() {
            return Ok(None);
        }

        // First, create a temporary shard structure in that directory.
        let temp_file_name = self.write_directory.join(temp_shard_file_name());

        let shard_hash = mem_shard.shard.write_to_shard_file(&temp_file_name)?;

        let full_file_name = self.write_directory.join(shard_file_name(&shard_hash));

        std::fs::rename(&temp_file_name, &full_file_name)?;

        // Now register the new shard and flush things.
        self.register_shards_by_path(&[&full_file_name]).await?;

        mem_shard.shard = MDBInMemoryShard::default();

        Ok(Some((shard_hash, full_file_name)))
    }

    /// Calculate the total materialized bytes (before deduplication) tracked by the manager,
    /// including in-memory state and on-disk shards.
    pub async fn calculate_total_materialized_bytes(&self) -> Result<u64> {
        let mut bytes = 0;
        {
            let lg = self.current_state.read().await;
            bytes += lg.shard.materialized_bytes();
        }

        self.query_all_shards(&mut |si, f, _| {
            bytes += si.materialized_bytes(f)?;
            Ok(None as Option<()>)
        })
        .await?;

        Ok(bytes)
    }

    /// Calculate the total stored bytes tracked (after deduplication) tracked by the manager,
    /// including in-memory state and on-disk shards.
    pub async fn calculate_total_stored_bytes(&self) -> Result<u64> {
        let mut bytes = 0;
        {
            let lg = self.current_state.read().await;
            bytes += lg.shard.stored_bytes();
        }

        self.query_all_shards(&mut |si, f, _| {
            bytes += si.stored_bytes(f)?;
            Ok(None as Option<()>)
        })
        .await?;

        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {

    use std::io::Read;

    use crate::{
        cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader},
        file_structs::FileDataSequenceHeader,
        merging::consolidate_shards_in_directory,
        shard_file::test_routines::{rng_hash, simple_hash},
        utils::parse_shard_filename,
    };

    use super::*;
    use crate::error::Result;
    use merklehash::compute_data_hash;
    use more_asserts::assert_lt;
    use rand::prelude::*;
    use tempdir::TempDir;

    #[allow(clippy::type_complexity)]
    pub async fn fill_with_specific_shard(
        shard: &mut ShardFileManager,
        in_mem_shard: &mut MDBInMemoryShard,
        cas_nodes: &[(u64, &[(u64, u32)])],
        file_nodes: &[(u64, &[(u64, (u32, u32))])],
    ) -> Result<()> {
        for (hash, chunks) in cas_nodes {
            let mut cas_block = Vec::<_>::new();
            let mut pos = 0;

            for (h, s) in chunks.iter() {
                cas_block.push(CASChunkSequenceEntry::new(simple_hash(*h), *s, pos));
                pos += *s;
            }
            let cas_info = MDBCASInfo {
                metadata: CASChunkSequenceHeader::new(simple_hash(*hash), chunks.len(), pos),
                chunks: cas_block,
            };

            shard.add_cas_block(cas_info.clone()).await?;

            in_mem_shard.add_cas_block(cas_info)?;
        }

        for (file_hash, segments) in file_nodes {
            let file_contents: Vec<_> = segments
                .iter()
                .map(|(h, (lb, ub))| {
                    FileDataSequenceEntry::new(simple_hash(*h), *ub - *lb, *lb, *ub)
                })
                .collect();
            let file_info = MDBFileInfo {
                metadata: FileDataSequenceHeader::new(simple_hash(*file_hash), segments.len()),
                segments: file_contents,
            };

            shard
                .add_file_reconstruction_info(file_info.clone())
                .await?;

            in_mem_shard.add_file_reconstruction_info(file_info)?;
        }

        Ok(())
    }

    async fn fill_with_random_shard(
        shard: &mut ShardFileManager,
        in_mem_shard: &mut MDBInMemoryShard,
        seed: u64,
        cas_block_sizes: &[usize],
        file_chunk_range_sizes: &[usize],
    ) -> Result<()> {
        // generate the cas content stuff.
        let mut rng = StdRng::seed_from_u64(seed);

        for cas_block_size in cas_block_sizes {
            let mut chunks = Vec::<_>::new();
            let mut pos = 0u32;

            for _ in 0..*cas_block_size {
                chunks.push(CASChunkSequenceEntry::new(
                    rng_hash(rng.gen()),
                    rng.gen_range(10000..20000),
                    pos,
                ));
                pos += rng.gen_range(10000..20000);
            }
            let metadata = CASChunkSequenceHeader::new(rng_hash(rng.gen()), *cas_block_size, pos);
            let mdb_cas_info = MDBCASInfo { metadata, chunks };

            shard.add_cas_block(mdb_cas_info.clone()).await?;
            in_mem_shard.add_cas_block(mdb_cas_info)?;
        }

        for file_block_size in file_chunk_range_sizes {
            let file_hash = rng_hash(rng.gen());

            let segments: Vec<_> = (0..*file_block_size)
                .map(|_| {
                    let lb = rng.gen_range(0..10000);
                    let ub = lb + rng.gen_range(0..10000);
                    FileDataSequenceEntry::new(rng_hash(rng.gen()), ub - lb, lb, ub)
                })
                .collect();

            let metadata = FileDataSequenceHeader::new(file_hash, *file_block_size);

            let file_info = MDBFileInfo { metadata, segments };

            shard
                .add_file_reconstruction_info(file_info.clone())
                .await?;

            in_mem_shard.add_file_reconstruction_info(file_info)?;
        }
        Ok(())
    }

    pub async fn verify_mdb_shards_match(
        mdb: &ShardFileManager,
        mem_shard: &MDBInMemoryShard,
    ) -> Result<()> {
        // Now, test that the results on queries from the
        for (k, cas_block) in mem_shard.cas_content.iter() {
            // Go through and test queries on both the in-memory shard and the
            // serialized shard, making sure that they match completely.

            for i in 0..cas_block.chunks.len() {
                // Test the dedup query over a few hashes in which all the
                // hashes queried are part of the cas_block.
                let query_hashes_1: Vec<MerkleHash> = cas_block.chunks
                    [i..(i + 3).min(cas_block.chunks.len())]
                    .iter()
                    .map(|c| c.chunk_hash)
                    .collect();
                let n_items_to_read = query_hashes_1.len();

                // Also test the dedup query over a few hashes in which some of the
                // hashes are part of the query, and the last is not.
                let mut query_hashes_2 = query_hashes_1.clone();
                query_hashes_2.push(rng_hash(1000000 + i as u64));

                let lb = cas_block.chunks[i].chunk_byte_range_start;
                let ub = if i + 3 >= cas_block.chunks.len() {
                    cas_block.metadata.num_bytes_in_cas
                } else {
                    cas_block.chunks[i + 3].chunk_byte_range_start
                };

                for query_hashes in [&query_hashes_1, &query_hashes_2] {
                    let result_m = mem_shard.chunk_hash_dedup_query(query_hashes).unwrap();

                    let result_f = mdb.chunk_hash_dedup_query(query_hashes).await?.unwrap();

                    // Returns a tuple of (num chunks matched, FileDataSequenceEntry)
                    assert_eq!(result_m.0, n_items_to_read);
                    assert_eq!(result_f.0, n_items_to_read);

                    // Make sure it gives the correct CAS block hash as the second part of the
                    assert_eq!(result_m.1.cas_hash, *k);
                    assert_eq!(result_f.1.cas_hash, *k);

                    // Make sure the bounds are correct
                    assert_eq!(
                        (
                            result_m.1.chunk_byte_range_start,
                            result_m.1.chunk_byte_range_end
                        ),
                        (lb, ub)
                    );
                    assert_eq!(
                        (
                            result_f.1.chunk_byte_range_start,
                            result_f.1.chunk_byte_range_end
                        ),
                        (lb, ub)
                    );

                    // Make sure everything else equal.
                    assert_eq!(result_m, result_f);
                }
            }
        }

        // Test get file reconstruction info.
        // Against some valid hashes,
        let mut query_hashes: Vec<MerkleHash> =
            mem_shard.file_content.iter().map(|file| *file.0).collect();
        // and a few (very likely) invalid somes.
        for i in 0..3 {
            query_hashes.push(rng_hash(1000000 + i as u64));
        }

        for k in query_hashes.iter() {
            let result_m = mem_shard.get_file_reconstruction_info(k);
            let result_f = mdb.get_file_reconstruction_info(k).await?;

            // Make sure two queries return same results.
            assert_eq!(result_m, result_f);

            // Make sure retriving the expected file.
            if result_m.is_some() {
                assert_eq!(result_m.unwrap().metadata.file_hash, *k);
                assert_eq!(result_f.unwrap().metadata.file_hash, *k);
            }
        }

        // Make sure manager correctly tracking repo size.
        assert_eq!(
            mdb.calculate_total_materialized_bytes().await?,
            mem_shard.materialized_bytes()
        );
        assert_eq!(
            mdb.calculate_total_stored_bytes().await?,
            mem_shard.stored_bytes()
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_basic_retrieval() -> Result<()> {
        let tmp_dir = TempDir::new("gitxet_shard_test_1")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        {
            let mut mdb = ShardFileManager::new(tmp_dir.path()).await?;

            fill_with_specific_shard(
                &mut mdb,
                &mut mdb_in_mem,
                &[(0, &[(11, 5)])],
                &[(100, &[(200, (0, 5))])],
            )
            .await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;

            let (h_ref, out_file) = mdb.flush().await?.unwrap();

            // Make sure it still stays
            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;

            // Verify that the hash is correct
            let h1 = parse_shard_filename(out_file.to_str().unwrap());
            assert_eq!(h1, Some(h_ref));

            let mut file_reader = std::fs::File::open(out_file)?;

            // Verify we got the hashes correct.
            let mut data = Vec::new();
            file_reader.read_to_end(&mut data)?;

            let h2 = compute_data_hash(&data[..]);
            assert_eq!(h2, h_ref);
        }
        {
            // Now, make sure that this happens if this directory is opened up
            let mut mdb2 = ShardFileManager::new(tmp_dir.path()).await?;

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem).await?;

            // Now add some more, based on this directory
            fill_with_random_shard(
                &mut mdb2,
                &mut mdb_in_mem,
                0,
                &[1, 5, 10, 8],
                &[4, 3, 5, 9, 4, 6],
            )
            .await?;

            verify_mdb_shards_match(&mdb2, &mdb_in_mem).await?;

            // Now, merge shards in the background.
            let merged_shards =
                consolidate_shards_in_directory(tmp_dir.path(), MDB_SHARD_MIN_TARGET_SIZE)?;

            assert_eq!(merged_shards.len(), 1);
            for si in merged_shards {
                assert!(si.path.exists());
                assert!(si.path.to_str().unwrap().contains(&si.shard_hash.hex()))
            }

            verify_mdb_shards_match(&mdb2, &mdb_in_mem).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_larger_simulated() -> Result<()> {
        let tmp_dir = TempDir::new("gitxet_shard_test_2")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();
        let mut mdb = ShardFileManager::new(tmp_dir.path()).await?;

        for i in 0..10 {
            fill_with_random_shard(
                &mut mdb,
                &mut mdb_in_mem,
                i,
                &[1, 5, 10, 8],
                &[4, 3, 5, 9, 4, 6],
            )
            .await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;

            let (h_ref, out_file) = mdb.flush().await?.unwrap();

            // Make sure it still stays
            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;

            // Verify that the hash is correct
            let h1 = parse_shard_filename(out_file.to_str().unwrap());
            assert_eq!(h1, Some(h_ref));

            let mut file_reader = std::fs::File::open(out_file)?;

            // Verify we got the hashes correct.
            let mut data = Vec::new();
            file_reader.read_to_end(&mut data)?;

            let h2 = compute_data_hash(&data[..]);
            assert_eq!(h2, h_ref);

            mdb.flush().await?;

            // Now, make sure that this happens if this directory is opened up
            let mdb2 = ShardFileManager::new(tmp_dir.path()).await?;

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_process_session_management() -> Result<()> {
        let tmp_dir = TempDir::new("gitxet_shard_test_3").unwrap();
        let mut mdb_in_mem = MDBInMemoryShard::default();

        for sesh in 0..3 {
            for i in 0..10 {
                {
                    let mut mdb = ShardFileManager::new(tmp_dir.path()).await.unwrap();
                    fill_with_random_shard(
                        &mut mdb,
                        &mut mdb_in_mem,
                        100 * sesh + i,
                        &[1, 5, 10, 8],
                        &[4, 3, 5, 9, 4, 6],
                    )
                    .await
                    .unwrap();

                    verify_mdb_shards_match(&mdb, &mdb_in_mem).await.unwrap();

                    let (h_ref, out_file) = mdb.flush().await.unwrap().unwrap();

                    // Make sure it still stays together
                    verify_mdb_shards_match(&mdb, &mdb_in_mem).await.unwrap();

                    // Verify that the hash is correct
                    let h1 = parse_shard_filename(out_file.to_str().unwrap());
                    assert_eq!(h1, Some(h_ref));

                    let mut file_reader = std::fs::File::open(out_file).unwrap();

                    // Verify we got the hashes correct.
                    let mut data = Vec::new();
                    file_reader.read_to_end(&mut data).unwrap();

                    let h2 = compute_data_hash(&data[..]);
                    assert_eq!(h2, h_ref);

                    mdb.flush().await.unwrap();

                    verify_mdb_shards_match(&mdb, &mdb_in_mem).await.unwrap();
                }
            }

            {
                let merged_shards =
                    consolidate_shards_in_directory(tmp_dir.path(), MDB_SHARD_MIN_TARGET_SIZE)
                        .unwrap();

                assert_eq!(merged_shards.len(), 1);

                for si in merged_shards {
                    assert!(si.path.exists());
                    assert!(si.path.to_str().unwrap().contains(&si.shard_hash.hex()))
                }
            }

            {
                // Now, make sure that this happens if this directory is opened up
                let mdb2 = ShardFileManager::new(tmp_dir.path()).await.unwrap();

                verify_mdb_shards_match(&mdb2, &mdb_in_mem).await.unwrap();
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_size_threshholds() -> Result<()> {
        let tmp_dir = TempDir::new("gitxet_shard_test_4")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        const T: u64 = 4096;

        for i in 0..5 {
            let mut mdb = ShardFileManager::new(tmp_dir.path()).await?;
            mdb.set_target_shard_min_size(T); // Set the targe shard size really low
            fill_with_random_shard(&mut mdb, &mut mdb_in_mem, i, &[5; 25], &[5; 25])
                .await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;

            let (h_ref, out_file) = mdb.flush().await?.unwrap();

            // Make sure it still stays together
            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;

            // Verify that the hash is correct
            let h1 = parse_shard_filename(out_file.to_str().unwrap());
            assert_eq!(h1, Some(h_ref));

            let mut file_reader = std::fs::File::open(out_file)?;

            // Verify we got the hashes correct.
            let mut data = Vec::new();
            file_reader.read_to_end(&mut data)?;

            let h2 = compute_data_hash(&data[..]);
            assert_eq!(h2, h_ref);

            mdb.flush().await?;
        }

        // Now, do a new shard that has less
        let mut last_num_files = None;
        let mut target_size = T;
        loop {
            // Now, make sure that this happens if this directory is opened up
            let mut mdb2 = ShardFileManager::new(tmp_dir.path()).await?;
            mdb2.set_target_shard_min_size(target_size);

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem).await?;

            let merged_shards = consolidate_shards_in_directory(tmp_dir.path(), target_size)?;

            for si in merged_shards.iter() {
                assert!(si.path.exists());
                assert!(si.path.to_str().unwrap().contains(&si.shard_hash.hex()))
            }

            let n_merged_shards = merged_shards.len();

            if n_merged_shards == 1 {
                break;
            }

            if let Some(n) = last_num_files {
                assert_lt!(n_merged_shards, n);
            }

            last_num_files = Some(n_merged_shards);

            // So the shards will all be consolidated in the next round.
            target_size *= 2;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_teardown() -> Result<()> {
        let tmp_dir = TempDir::new("gitxet_shard_test_1")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        {
            let mut mdb = ShardFileManager::new(tmp_dir.path()).await?;

            fill_with_specific_shard(
                &mut mdb,
                &mut mdb_in_mem,
                &[(0, &[(11, 5)])],
                &[(100, &[(200, (0, 5))])],
            )
            .await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;
            // Note, no flush
        }

        {
            // Now, make sure that this happens if this directory is opened up
            let mdb2 = ShardFileManager::new(tmp_dir.path()).await?;

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem).await?;
        }

        Ok(())
    }
}
