use cas::output_bytes;
use cas_client::*;
use futures::prelude::stream::*;
use lazy::lazy_pathlist_config::LazyPathListConfigFile;
use lazy::lazy_rule_config::LazyStrategy;
use mdb_shard::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo};
use mdb_shard::error::MDBShardError;
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use mdb_shard::intershard_reference_structs::IntershardReferenceSequence;
use mdb_shard::shard_file_handle::MDBShardFile;
use mdb_shard::shard_file_manager::ShardFileManager;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use merkledb::aggregate_hashes::{cas_node_hash, file_node_hash};
use merkledb::constants::TARGET_CAS_BLOCK_SIZE;
use merkledb::*;
use merklehash::MerkleHash;
use parutils::{tokio_par_for_each, BatchedAsyncIterator, BufferedAsyncIterator};
use progress_reporting::DataProgressReporter;
use std::clone::Clone;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::mem::take;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tracing::{debug, error, info, info_span, warn};
use tracing_futures::Instrument;

use super::mdb::download_shard;
use super::small_file_determination::{check_passthrough_status, PassThroughFileStatus};
use super::smudge_query_interface::{
    shard_manager_from_config, RemoteShardInterface, SmudgeQueryPolicy,
};
use super::*;
use crate::config::XetConfig;
use crate::constants::*;
use crate::errors::{convert_cas_error, GitXetRepoError, Result};
use crate::git_integration::git_repo_salt::read_repo_salt_by_dir;
use crate::stream::data_iterators::AsyncDataIterator;
use crate::summaries::*;

#[derive(Default)]
struct CASDataAggregator {
    data: Vec<u8>,
    chunks: Vec<(MerkleHash, (usize, usize))>,
    // The file info of files that are still being processed.
    // As we're building this up, we assume that all files that do not have a size in the header are
    // not finished yet and thus cannot be uploaded.
    //
    // All the cases the default hash for a cas info entry will be filled in with the cas hash for
    // an entry once the cas block is finalized and uploaded.  These correspond to the indices given
    // alongwith the file info.
    // This tuple contains the file info (which may be modified), the divisions in the chunks corresponding
    // to this file, and the dedup origin tracking.
    pending_file_info: Vec<(MDBFileInfo, Vec<usize>, HashMap<MerkleHash, usize>)>,
}

/// Manages the translation of files between the
/// MerkleDB / pointer file format and the materialized version.
///
/// This class handles the clean and smudge options.
pub struct PointerFileTranslatorV2 {
    shard_manager: Arc<ShardFileManager>,
    remote_shards: Arc<RemoteShardInterface>,
    summarydb: Arc<Mutex<WholeRepoSummary>>,
    cas: Arc<dyn Staging + Send + Sync>,
    prefix: String,
    small_file_threshold: usize,

    cas_data: Arc<Mutex<CASDataAggregator>>,

    repo_salt: Option<[u8; REPO_SALT_LEN]>,

    cfg: XetConfig,

    lazyconfig: Option<LazyPathListConfigFile>,
}

impl PointerFileTranslatorV2 {
    /// Constructor
    pub async fn from_config(config: &XetConfig) -> Result<Self> {
        let cas_client = create_cas_client(config).await?;

        let in_repo = config.repo_path_if_present.is_some();

        let summarydb = if in_repo {
            Arc::new(Mutex::new(
                WholeRepoSummary::load_or_recreate_from_git(
                    config,
                    &config.summarydb,
                    GIT_NOTES_SUMMARIES_REF_NAME,
                )
                .await?,
            ))
        } else {
            Arc::new(Mutex::new(WholeRepoSummary::empty(&PathBuf::default())))
        };

        let repo_salt = if in_repo {
            read_repo_salt_by_dir(config.repo_path()?, config)?
        } else {
            None
        };

        let shard_manager = Arc::new(shard_manager_from_config(config).await?);

        let remote_shards = Arc::new(
            RemoteShardInterface::new_from_config(config, Some(shard_manager.clone())).await?,
        );

        let lazyconfig = if let Some(f) = config.lazy_config.as_ref() {
            Some(LazyPathListConfigFile::load_smudge_list_from_file(f, false).await?)
        } else {
            None
        };

        // let axe = Axe::new("DataPipeline", &config.clone(), None).await.ok();
        Ok(Self {
            shard_manager: shard_manager.clone(),
            remote_shards,
            summarydb,
            cas: cas_client,
            prefix: config.cas.prefix.clone(),
            small_file_threshold: config.cas.size_threshold,
            cas_data: Arc::new(Default::default()),
            repo_salt,
            cfg: config.clone(),
            lazyconfig,
        })
    }

    pub fn in_repo(&self) -> bool {
        self.cfg.repo_path_if_present.is_some()
    }

    pub fn set_repo_salt(&mut self, repo_salt: &[u8]) {
        let mut data = [0u8; REPO_SALT_LEN];
        data.copy_from_slice(repo_salt);
        self.repo_salt = Some(data);
    }

    pub async fn refresh(&self) -> Result<()> {
        if self.in_repo() {
            let summarydb = WholeRepoSummary::load_or_recreate_from_git(
                &self.cfg,
                &self.cfg.summarydb,
                GIT_NOTES_SUMMARIES_REF_NAME,
            )
            .await?;

            *self.summarydb.lock().await = summarydb;
        }

        // See if there are any un-registered shards.
        self.shard_manager
            .register_shards_by_path(&[&self.cfg.merkledb_v2_session], false)
            .await?;
        // See if there are any un-registered shards.
        self.shard_manager
            .register_shards_by_path(&[&self.cfg.merkledb_v2_cache], true)
            .await?;

        Ok(())
    }

    /// New temporary
    #[cfg(test)]
    pub async fn new_temporary(temp_dir: &Path) -> Result<Self> {
        let shard_manager = Arc::new(ShardFileManager::new(temp_dir).await?);
        let file_reconstructor =
            Arc::new(RemoteShardInterface::new_local(shard_manager.clone()).await?);
        let summarydb = Arc::new(Mutex::new(WholeRepoSummary::empty(&PathBuf::default())));
        let localclient = LocalClient::default();
        let cas = Arc::new(StagingClient::new(Arc::new(localclient), temp_dir));

        Ok(Self {
            shard_manager: shard_manager.clone(),
            remote_shards: file_reconstructor,
            summarydb,
            cas,
            prefix: "".into(),
            small_file_threshold: SMALL_FILE_THRESHOLD,
            cas_data: Arc::new(Default::default()),
            repo_salt: Some(Default::default()),
            cfg: XetConfig::empty(),
            lazyconfig: None,
        })
    }

    pub fn get_config(&self) -> XetConfig {
        self.cfg.clone()
    }

    pub fn get_cas(&self) -> Arc<dyn Staging + Send + Sync> {
        self.cas.clone()
    }

    pub fn get_prefix(&self) -> String {
        self.prefix.clone()
    }

    pub fn get_summarydb(&self) -> Arc<Mutex<WholeRepoSummary>> {
        self.summarydb.clone()
    }

    pub fn get_shard_manager(&self) -> Arc<ShardFileManager> {
        self.shard_manager.clone()
    }

    pub fn get_repo_salt(&self) -> Result<&[u8; REPO_SALT_LEN]> {
        self.repo_salt.as_ref().ok_or_else(|| {
             GitXetRepoError::RepoSaltUnavailable(
                if self.in_repo() {
                 "Error reading repo salt from current repository; some operations unavailable.  Current repository at possibly not configured for use with git xet.".to_owned()
            } else {
     "Operations requiring a repo salt are not available outside of a repository configured for use with git xet.".to_owned()
            })
    })
    }

    pub async fn upload_cas_staged(&self, retain: bool) -> Result<()> {
        self.cas
            .upload_all_staged(MAX_CONCURRENT_UPLOADS, retain)
            .await
            .or_else(convert_cas_error)
    }

    pub async fn clean_file(
        &self,
        path: &Path,
        reader: impl AsyncDataIterator + 'static,
    ) -> Result<Vec<u8>> {
        self.clean_file_and_report_progress(path, reader, &None)
            .await
    }

    /// Opens the current shard if it's present already, otherwise downloads the shard first.
    pub async fn open_or_fetch_shard(&self, shard_hash: &MerkleHash) -> Result<MDBShardFile> {
        if let Some(sfi) = self.shard_manager.get_shard_handle(shard_hash, false).await {
            Ok(sfi)
        } else {
            let (shard_path, _) = download_shard(
                &self.cfg,
                &self.cas,
                shard_hash,
                &self.cfg.merkledb_v2_cache,
            )
            .await?;
            let new_shard_sfi_v = self
                .shard_manager
                .register_shards_by_path(&[&shard_path], true)
                .await?;
            debug_assert_eq!(new_shard_sfi_v.len(), 1);
            Ok(new_shard_sfi_v.last().unwrap().clone())
        }
    }

    /// Fetches all the shards in the shard hints that correspond to a given file hash.
    pub async fn get_hinted_shard_list_for_file(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<IntershardReferenceSequence> {
        // First, get the shard corresponding to the file hash

        let Some((_, shard_hash_opt)) = self
            .remote_shards
            .get_file_reconstruction_info(file_hash)
            .await?
        else {
            warn!("get_hinted_shard_list_for_file: file reconstruction not found; ignoring.");
            return Ok(<_>::default());
        };

        let Some(shard_hash) = shard_hash_opt else {
            info!("get_hinted_shard_list_for_file: file reconstruction found in non-permanent shard, ignoring.");
            return Ok(<_>::default());
        };

        debug!("Retrieving shard hints associated with {shard_hash:?}");
        let shard_file = self.open_or_fetch_shard(&shard_hash).await?;

        Ok(shard_file.get_intershard_references()?)
    }

    /** Fetch new shards from cas to local cache and register them.  
     */

    pub async fn fetch_and_add_shards(&self, shards: Vec<MerkleHash>) -> Result<()> {
        if shards.is_empty() {
            return Ok(());
        }

        tokio_par_for_each(shards, MAX_CONCURRENT_DOWNLOADS, |sh, _| async move {
            if self.shard_manager.shard_is_registered(&sh).await {
                return Ok::<(), GitXetRepoError>(());
            }

            let (p, _) =
                download_shard(&self.cfg, &self.cas, &sh, &self.cfg.merkledb_v2_cache).await?;
            self.shard_manager
                .register_shards_by_path(&[&p], true)
                .await?;

            Ok(())
        })
        .await
        .map_err(|e| match e {
            parutils::ParallelError::JoinError => {
                GitXetRepoError::InternalError(anyhow::anyhow!("Join Error"))
            }
            parutils::ParallelError::TaskError(e) => e,
        })?;

        Ok(())
    }

    /**  Cleans the file.
     */
    pub async fn clean_file_and_report_progress(
        &self,
        path: &Path,
        mut reader: impl AsyncDataIterator + 'static,
        progress_indicator: &Option<Arc<DataProgressReporter>>,
    ) -> Result<Vec<u8>> {
        // Now, test whether to pass this file through or not.
        let starting_data = {
            match check_passthrough_status(&mut reader, self.small_file_threshold).await? {
                PassThroughFileStatus::ChunkFile(starting_data) => starting_data,
                PassThroughFileStatus::PassFileThrough(file_data) => {
                    // In this cases, we're done, and here is the file data.
                    return Ok(file_data);
                }
            }
        };

        // Now, start chunking.
        let raw_data_iter =
            BufferedAsyncIterator::new_with_starting_data(starting_data, reader, None);

        let mut generator =
            BufferedAsyncIterator::new(async_chunk_target_default(raw_data_iter), Some(4096));
        let mut bytes_cleaned: usize = 0;

        // TODO: This span isn't quite accurate as we hold it across `await` calls.
        //       We should probably refactor this code to better support tracing the time spent.
        let span = info_span!("chunk_file");
        let chunk_scope = span.enter();

        let mut cas_data = CASDataAggregator::default();

        let mut file_hashes = Vec::<(MerkleHash, usize)>::new();
        let mut file_info = Vec::<FileDataSequenceEntry>::new();
        let mut current_cas_file_info_indices = Vec::<usize>::new();
        let mut file_size = 0;
        let mut current_cas_block_hashes = HashMap::<MerkleHash, usize>::new();

        let mut shard_dedup_tracker = HashMap::<MerkleHash, usize>::new();

        // Now get started on whatever analyzers are needed.
        let mut analyzers = FileAnalyzers::default();

        debug!("Including analyzers for path {:?}", &path);
        let mut analyzers_active = false;
        let ext = path.extension();
        if ext == Some(OsStr::new("csv")) {
            info!("Including CSV analyzer (file extension .csv)");
            analyzers.csv = Some(CSVAnalyzer::new(self.cfg.log.silent_summary, b','));
            analyzers_active = true;
        } else if ext == Some(OsStr::new("tsv")) {
            info!("Including CSV analyzer (file extension .tsv)");
            analyzers.csv = Some(CSVAnalyzer::new(self.cfg.log.silent_summary, b'\t'));
            analyzers_active = true;
        }

        // Create a container for the analyzers so we can give it to the background thread and get it back.
        let mut analyzer_holder = if analyzers_active {
            Some(analyzers)
        } else {
            None
        };

        loop {
            // Grab as many blocks of chunks as are available.
            let chunks = Arc::new(generator.next_batch(Some(128)).await?);

            if chunks.is_empty() {
                // We are done.
                break;
            }

            file_hashes.extend(chunks.iter().map(|(c, b)| (c.hash, b.len())));

            let chunk_hashes = Vec::from_iter(chunks.iter().map(|(c, _)| c.hash));

            // Start up the analyzer on a background thread.

            let analyzer_bg = analyzer_holder.take();
            let chunks_bg = chunks.clone();
            let bytes_cleaned_bg = bytes_cleaned;
            let path_bg = path.to_owned();
            let analyzer_process_handle = tokio::spawn(async move {
                let mut bytes_cleaned = bytes_cleaned_bg;
                if let Some(mut analyzers) = analyzer_bg {
                    for (_, bytes) in chunks_bg.iter() {
                        analyzers.process_chunk(&bytes[..], &path_bg, bytes_cleaned);
                        bytes_cleaned += bytes.len();
                    }
                    Some(analyzers)
                } else {
                    None
                }
            });

            let mut cur_idx = 0;

            while cur_idx < chunks.len() {
                let mut n_bytes = 0;

                if let Some((n_deduped, fse)) = self
                    .shard_manager
                    .chunk_hash_dedup_query(
                        &chunk_hashes[cur_idx..],
                        Some(&mut shard_dedup_tracker),
                    )
                    .await?
                {
                    // We found one or more chunk hashes present in a cas block somewhere.

                    // Update all the metrics.
                    for i in cur_idx..(cur_idx + n_deduped) {
                        n_bytes += chunks[i].1.len();
                    }
                    file_size += n_bytes;
                    bytes_cleaned += n_bytes;

                    // Do we modify the previous entry as this is the next logical chunk, or do we
                    // start a new entry?
                    if !file_info.is_empty()
                        && file_info.last().unwrap().cas_hash == fse.cas_hash
                        && file_info.last().unwrap().chunk_byte_range_end
                            == fse.chunk_byte_range_start
                    {
                        // This block is the contiguous continuation of the last entry
                        let last_entry = file_info.last_mut().unwrap();
                        last_entry.unpacked_segment_bytes += n_bytes as u32;
                        last_entry.chunk_byte_range_end += n_bytes as u32;
                    } else {
                        // This block is new
                        file_info.push(fse);
                    }

                    cur_idx += n_deduped;
                } else {
                    let (chunk, bytes) = &chunks[cur_idx];

                    n_bytes = chunks[cur_idx].1.len();
                    file_size += n_bytes;
                    bytes_cleaned += n_bytes;

                    // This is new data.
                    let add_new_data;

                    if let Some(idx) = current_cas_block_hashes.get(&chunk.hash) {
                        let (_, (data_lb, data_ub)) = cas_data.chunks[*idx];

                        // This chunk will get the CAS hash updated when the local CAS block
                        // is full and registered.
                        current_cas_file_info_indices.push(file_info.len());

                        file_info.push(FileDataSequenceEntry::new(
                            MerkleHash::default(),
                            n_bytes,
                            data_lb,
                            data_ub,
                        ));
                        add_new_data = false;
                    } else if !file_info.is_empty()
                        && file_info.last().unwrap().cas_hash == MerkleHash::default()
                        && file_info.last().unwrap().chunk_byte_range_end as usize
                            == cas_data.data.len()
                    {
                        // This is the next chunk in the CAS block
                        // we're building, in which case we can just modify the previous entry.
                        let last_entry = file_info.last_mut().unwrap();
                        last_entry.unpacked_segment_bytes += n_bytes as u32;
                        last_entry.chunk_byte_range_end += n_bytes as u32;
                        add_new_data = true;
                    } else {
                        // This block is unrelated to the previous one.
                        // This chunk will get the CAS hash updated when the local CAS block
                        // is full and registered.
                        current_cas_file_info_indices.push(file_info.len());

                        file_info.push(FileDataSequenceEntry::new(
                            MerkleHash::default(),
                            n_bytes,
                            cas_data.data.len(),
                            cas_data.data.len() + n_bytes,
                        ));
                        add_new_data = true;
                    }

                    if add_new_data {
                        // Add in the chunk and cas information.
                        current_cas_block_hashes.insert(chunk.hash, cas_data.chunks.len());

                        cas_data.chunks.push((
                            chunk.hash,
                            (cas_data.data.len(), cas_data.data.len() + n_bytes),
                        ));
                        cas_data.data.extend(bytes);

                        if cas_data.data.len() > TARGET_CAS_BLOCK_SIZE {
                            let cas_hash = self.register_new_cas_block(&mut cas_data).await?;

                            for i in current_cas_file_info_indices.iter() {
                                file_info[*i].cas_hash = cas_hash;
                            }
                            current_cas_file_info_indices.clear();
                            current_cas_block_hashes.clear();
                        }
                    }

                    // Next round.
                    cur_idx += 1;
                }

                if let Some(pi) = progress_indicator {
                    pi.set_active(true);
                    pi.register_progress(None, Some(n_bytes));
                }
            }

            // Capture the analyzer info
            analyzer_holder = analyzer_process_handle.await?;
        }

        let file_hash = file_node_hash(&file_hashes, self.get_repo_salt()?)?;

        // Is it registered already?
        let file_already_registered = match self.remote_shards.smudge_query_policy {
            SmudgeQueryPolicy::LocalFirst | SmudgeQueryPolicy::LocalOnly => self
                .remote_shards
                .shard_manager
                .as_ref()
                .ok_or_else(|| {
                    MDBShardError::SmudgeQueryPolicyError(
                        "Require ShardFileManager for smudge query policy other than 'server_only'"
                            .to_owned(),
                    )
                })?
                .get_file_reconstruction_info(&file_hash)
                .await?
                .is_some(),
            super::smudge_query_interface::SmudgeQueryPolicy::ServerOnly => false,
        };

        if !file_already_registered {
            // Put an accumulated data into the struct-wide cas block for building a future chunk.
            let mut cas_data_accumulator = self.cas_data.lock().await;

            let shift = cas_data_accumulator.data.len() as u32;
            cas_data_accumulator.data.append(&mut cas_data.data);
            cas_data_accumulator.chunks.append(&mut cas_data.chunks);
            let new_file_info = MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, file_info.len()),
                segments: file_info
                    .into_iter()
                    .map(|fi| {
                        // If it's in this new cas chunk, shift everything.
                        let s = if fi.cas_hash == MerkleHash::default() {
                            shift
                        } else {
                            0
                        };

                        let mut new_fi = fi;
                        new_fi.chunk_byte_range_start += s;
                        new_fi.chunk_byte_range_end += s;

                        new_fi
                    })
                    .collect(),
            };
            cas_data_accumulator.pending_file_info.push((
                new_file_info,
                current_cas_file_info_indices,
                shard_dedup_tracker,
            ));

            if cas_data_accumulator.data.len() >= TARGET_CAS_BLOCK_SIZE {
                let mut new_cas_data = take(cas_data_accumulator.deref_mut());
                drop(cas_data_accumulator); // Release the lock.
                self.register_new_cas_block(&mut new_cas_data).await?;
            } else {
                drop(cas_data_accumulator);
            }
        }
        // we only add to the counters if we see changes
        FILTER_BYTES_CLEANED.inc_by(bytes_cleaned as u64);

        drop(chunk_scope);

        let span = info_span!("to_pointerfile");
        let _scope = span.enter();

        let pointer_file: PointerFile =
            PointerFile::init_from_info(path.to_str().unwrap(), &file_hash.hex(), file_size as u64);

        // For each of the analyzers, add data to the notes as appropriate.
        let key = file_hash.hex();
        let summarydb_arc = self.summarydb.clone();
        let mut summarydb = summarydb_arc.lock().await;
        let existing_file_summary = summarydb.entry(key.clone()).or_default();
        if let Some(mut analyzers) = analyzer_holder {
            if let Some(new_file_summary) = analyzers.finalize(path) {
                existing_file_summary.merge_in(new_file_summary, &key);
            }
        }

        Ok(pointer_file.to_string().as_bytes().to_vec())
    }

    async fn register_new_cas_block(&self, cas_data: &mut CASDataAggregator) -> Result<MerkleHash> {
        let cas_hash = cas_node_hash(&cas_data.chunks[..])?;
        let metadata =
            CASChunkSequenceHeader::new(cas_hash, cas_data.chunks.len(), cas_data.data.len());

        let mut pos = 0;
        let chunks: Vec<_> = cas_data
            .chunks
            .iter()
            .map(|(h, (bytes_lb, bytes_ub))| {
                let size = bytes_ub - bytes_lb;
                let result = CASChunkSequenceEntry::new(*h, size, pos);
                pos += size;
                result
            })
            .collect();

        let cas_info = MDBCASInfo { metadata, chunks };

        let mut chunk_boundaries: Vec<u64> = Vec::with_capacity(cas_data.chunks.len());
        let mut running_sum = 0;

        for (_, s) in cas_data.chunks.iter() {
            running_sum += s.1 - s.0;
            chunk_boundaries.push(running_sum as u64);
        }

        if !cas_info.chunks.is_empty() {
            self.shard_manager.add_cas_block(cas_info).await?;

            self.cas
                .put(
                    &self.prefix,
                    &cas_hash,
                    take(&mut cas_data.data),
                    chunk_boundaries,
                )
                .await?;
        } else {
            debug_assert_eq!(cas_hash, MerkleHash::default());
        }

        // Now register any new files as needed.
        for (mut fi, chunk_hash_indices, shard_dedup_tracking) in
            take(&mut cas_data.pending_file_info)
        {
            for i in chunk_hash_indices {
                debug_assert_eq!(fi.segments[i].cas_hash, MerkleHash::default());
                fi.segments[i].cas_hash = cas_hash;
            }

            self.shard_manager
                .add_file_reconstruction_info(fi, Some(shard_dedup_tracking))
                .await?;
        }

        FILTER_CAS_BYTES_PRODUCED.inc_by(running_sum as u64);

        cas_data.data.clear();
        cas_data.chunks.clear();
        cas_data.pending_file_info.clear();

        Ok(cas_hash)
    }

    /// To be called after a collection of clean_file calls.
    /// Can be safely called even if no cleaning happened.
    pub async fn finalize_cleaning(&self) -> Result<()> {
        self.summarydb.lock().await.flush()?;

        {
            let mut global_cas_data = self.cas_data.lock().await;
            self.register_new_cas_block(&mut global_cas_data).await?;
        }
        // TODO: when we have aggregated CAS stuff, handle that.
        self.cas.flush().await?;
        Ok(())
    }

    pub fn print_stats(&self) {
        let bytes_cleaned = FILTER_BYTES_CLEANED.get();
        let cas_bytes_produced = FILTER_CAS_BYTES_PRODUCED.get();
        if bytes_cleaned > 0 {
            let ratio: f64 = 100.0 * cas_bytes_produced as f64 / bytes_cleaned as f64;
            eprintln!(
                "{} added, stored {} ({:.1}% reduction)",
                output_bytes(bytes_cleaned as usize),
                output_bytes(cas_bytes_produced as usize),
                100.0 - ratio
            );
        }
    }

    async fn data_from_chunks_to_writer(
        &self,
        chunks: Vec<ObjectRange>,
        writer: &mut impl std::io::Write,
    ) -> Result<()> {
        let mut bytes_smudged: u64 = 0;
        let mut strm = iter(chunks.into_iter().map(|objr| {
            let prefix = self.prefix.clone();
            cas_interface::get_from_cas(
                &self.cas,
                prefix,
                objr.hash,
                (objr.start as u64, objr.end as u64),
            )
        }))
        .buffered(MAX_CONCURRENT_DOWNLOADS);

        while let Some(buf) = strm.next().await {
            let buf = buf?;
            bytes_smudged += buf.len() as u64;
            let s = info_span!("write_chunk");
            let _ = s.enter();
            writer.write_all(&buf)?;
        }

        FILTER_BYTES_SMUDGED.inc_by(bytes_smudged);

        Ok(())
    }

    async fn data_from_chunks_to_mpsc(
        &self,
        chunks: Vec<ObjectRange>,
        writer: &Sender<Result<Vec<u8>>>,
        ready: &Option<watch::Sender<bool>>,
        progress_indicator: &Option<Arc<DataProgressReporter>>,
    ) -> Result<usize> {
        let mut cas_bytes_retrieved = 0;

        let mut strm = iter(chunks.into_iter().map(|objr| {
            let prefix = self.prefix.clone();
            cas_interface::get_from_cas(
                &self.cas,
                prefix,
                objr.hash,
                (objr.start as u64, objr.end as u64),
            )
        }))
        .buffered(MAX_CONCURRENT_DOWNLOADS);
        let mut is_first = true;
        while let Some(buf) = strm.next().await {
            let buf = buf?;
            let buf_len = buf.len();
            cas_bytes_retrieved += buf.len();
            writer.send(Ok(buf)).await.map_err(|_| {
                GitXetRepoError::Other("Unable to send smudge result as channel has closed".into())
            })?;
            if is_first {
                if let Some(is_ready) = ready {
                    let _ = is_ready.send(true);
                    is_first = false;
                }
            }
            if let Some(pi) = progress_indicator {
                pi.set_active(true);
                pi.register_progress(None, Some(buf_len));
            }
        }
        // nothing was written. we flag first too
        if is_first {
            if let Some(is_ready) = ready {
                let _ = is_ready.send(true);
                // is_first = false; // TODO: should we remove this? it isn't used...
            }
        }
        Ok(cas_bytes_retrieved)
    }

    /// Smudges a file reading a pointer file from reader, and writing
    /// the hydrated output to the writer.
    ///
    /// If passthrough is false, this function will fail on an invalid pointer
    /// file returning an Err.
    ///
    /// If passthrough is true, a failed parse of the pointer file or
    /// a failed lookup of the file reconstruction information will pass
    /// through all the contents directly to the writer.
    pub async fn smudge_file(
        &self,
        path: &PathBuf,
        mut reader: impl AsyncDataIterator,
        writer: &mut impl std::io::Write,
        passthrough: bool,
        range: Option<(usize, usize)>,
    ) -> Result<()> {
        info!("Smudging file {:?}", &path);

        let (fi, data) =
            pointer_file_from_reader(path, &mut reader, self.cfg.force_no_smudge).await?;

        if let Some(ptr) = fi {
            let result = self
                .smudge_file_from_pointer(path, &ptr, writer, range)
                .await;

            if let Err(GitXetRepoError::FileReconstructionFailed(_)) = &result {
                error!(
                    "File reconstruction failed for file {path:?}, hash={}",
                    &ptr.hash_string()
                );
                if range.is_some() || !passthrough {
                    return result;
                } else {
                    info!("Passing through pointer file after failed reconstruction lookup.");
                }
            } else {
                return result;
            }
        } else {
            // Now, the file gets passed through.
            if passthrough {
                info!("{:?} is not a valid pointer file. Passing through", path);
            } else {
                error!("Invalid Pointer File");
                return Err(GitXetRepoError::Other("Invalid Pointer File".into()));
            }
        }

        // this did not parse as a pointer file. We dump it straight
        // back out to the writer
        // we first dump the data we tried to parse as a pointer
        match range {
            // we have been supplied a range to write, so write the requested byte range
            Some((start, end)) => {
                // we expect callers to validate the range, but just in case, check it anyway.
                if end < start {
                    let msg = format!("End range value requested ({end}) is less than start range value ({start})");
                    error!(msg);
                    return Err(GitXetRepoError::Other(msg));
                }

                let mut st = start;
                let mut dat = data;

                // skip ahead to the start of the requested range
                while st > 0 {
                    let skipped = std::cmp::min(st, dat.len());
                    st -= skipped;
                    if skipped < dat.len() {
                        dat = (dat[skipped..]).to_vec();
                    } else {
                        dat = reader.next().await?.ok_or_else(|| {
                            GitXetRepoError::Other(
                                "Start range value requested is larger than the file size".into(),
                            )
                        })?;
                    }
                }

                // write the rest of the bytes in the range
                let mut len = end - start;
                while len > 0 {
                    let write = std::cmp::min(len, dat.len());
                    writer.write_all(&dat[0..write])?;

                    match reader.next().await? {
                        Some(buf) => dat = buf,
                        None => break,
                    }
                    len -= write;
                }
            }
            // we haven't been given a range, so write out all bytes
            None => {
                writer.write_all(&data)?;
                // then loop over the reader writing straight out to writer
                while let Some(data) = reader.next().await? {
                    writer.write_all(&data)?;
                }
            }
        }
        Ok(())
    }
    /// Performs a prefetch heuristic assuming that the user wll be reading at
    /// the provided start position,
    ///
    /// The file is cut into chunks of PREFETCH_WINDOW_SIZE_MB.
    /// The prefetch will start a task to download the chunk which contains
    /// the byte X.
    ///
    /// Returns true if a prefetch was started, and false otherwise
    pub async fn prefetch(&self, _pointer: &PointerFile, _start: u64) -> Result<bool> {
        // TODO: implement
        Ok(false)
    }

    /// Given an Vec<ObjectRange> describing a series of range of bytes,
    /// slice a subrange. This does not check limits and may return shorter
    /// results if the slice goes past the end of the range.
    fn slice_object_range(v: &[ObjectRange], mut start: usize, mut len: usize) -> Vec<ObjectRange> {
        let mut ret: Vec<ObjectRange> = Vec::new();
        for i in v.iter() {
            let ilen = i.end - i.start;
            // we have not gotten to the start of the range
            if start > 0 && start >= ilen {
                // start is still after this range
                start -= ilen;
            } else {
                // either start == 0, or start < packet len.
                // Either way, we need some or all of this packet
                // and after this packet start must be = 0
                let packet_start = i.start + start;
                // the maximum length allowed is how far to end of the packet
                // OR the actual slice length requested which ever is shorter.
                let max_length_allowed = std::cmp::min(i.end - packet_start, len);
                ret.push(ObjectRange {
                    hash: i.hash,
                    start: packet_start,
                    end: packet_start + max_length_allowed,
                });
                start = 0;
                len -= max_length_allowed;
            }
            if len == 0 {
                break;
            }
        }
        ret
    }

    /// Smudges a file reading a pointer file from reader, and writing
    /// all results including errors to the writer MPSC channel
    ///
    /// If the reader is not a pointer file, we passthrough the contents
    /// to the writer.
    pub async fn smudge_file_to_mpsc(
        &self,
        path: &Path,
        mut reader: impl AsyncDataIterator,
        writer: &Sender<Result<Vec<u8>>>,
        ready: &Option<watch::Sender<bool>>,
        progress_indicator: &Option<Arc<DataProgressReporter>>,
    ) -> usize {
        info!("Smudging file {:?}", &path);
        let print_err = |e| {
            error!("Unable to send smudge error {e:?} as channel has closed");
            e
        };

        let (fi, data) =
            match pointer_file_from_reader(path, &mut reader, self.cfg.force_no_smudge).await {
                Ok(b) => b,
                Err(e) => {
                    let _ = writer.send(Err(e)).await.map_err(print_err);
                    return 0;
                }
            };

        match fi {
            Some(ptr) => {
                if let Some(lazy) = &self.lazyconfig {
                    let rule = lazy.match_rule(path);
                    if rule == LazyStrategy::POINTER {
                        // we dump the pointer file
                        if let Some(ready_signal) = ready {
                            let _ = ready_signal.send(true);
                        }
                        let _ = writer.send(Ok(data)).await.map_err(print_err);
                        return 0;
                    }
                }
                self.smudge_file_from_pointer_to_mpsc(path, &ptr, writer, ready, progress_indicator)
                    .await
            }
            None => {
                info!("{:?} is not a valid pointer file. Passing through", path);
                if let Some(ready_signal) = ready {
                    let _ = ready_signal.send(true);
                }
                // this did not parse as a pointer file. We dump it straight
                // back out to the writer
                // we first dump the data we tried to parse as a pointer
                if writer.send(Ok(data)).await.map_err(print_err).is_err() {
                    return 0;
                }
                // then loop over the reader writing straight out to writer
                loop {
                    match reader.next().await {
                        Ok(Some(data)) => {
                            // we have data. write it
                            if writer.send(Ok(data)).await.map_err(print_err).is_err() {
                                return 0;
                            }
                        }
                        Ok(None) => {
                            // EOF. quit
                            break;
                        }
                        Err(e) => {
                            // error, try to dump it into writer and quit
                            let _ = writer.send(Err(e)).await.map_err(print_err);
                            return 0;
                        }
                    };
                }
                0
            }
        }
    }

    pub async fn derive_blocks(&self, hash: &MerkleHash) -> Result<Vec<ObjectRange>> {
        if let Some((file_info, _shard_hash)) = self
            .remote_shards
            .get_file_reconstruction_info(hash)
            .await?
        {
            Ok(file_info
                .segments
                .into_iter()
                .map(|s| ObjectRange {
                    hash: s.cas_hash,
                    start: s.chunk_byte_range_start as usize,
                    end: s.chunk_byte_range_end as usize,
                })
                .collect())
        } else {
            error!("File Reconstruction info for hash {hash:?} not found.");
            Err(GitXetRepoError::HashNotFound)
        }
    }
    pub async fn smudge_file_from_pointer(
        &self,
        path: &Path,
        pointer: &PointerFile,
        writer: &mut impl std::io::Write,
        range: Option<(usize, usize)>,
    ) -> Result<()> {
        self.smudge_file_from_hash(Some(path.to_path_buf()), &pointer.hash()?, writer, range)
            .await
    }

    pub async fn smudge_file_from_hash(
        &self,
        path: Option<PathBuf>,
        file_id: &MerkleHash,
        writer: &mut impl std::io::Write,
        range: Option<(usize, usize)>,
    ) -> Result<()> {
        if let Some(p) = &path {
            info!("Smudging file {p:?}");
        }

        let blocks = self
            .derive_blocks(file_id)
            .instrument(info_span!("derive_blocks"))
            .await?;

        let ranged_blocks = match range {
            Some((start, end)) => {
                // we expect callers to validate the range, but just in case, check it anyway.
                if end < start {
                    let msg = format!(
                        "End range value requested ({end}) is less than start range value ({start})"
                    );
                    error!(msg);
                    return Err(GitXetRepoError::Other(msg));
                }
                Self::slice_object_range(&blocks, start, end - start)
            }
            None => blocks,
        };

        self.data_from_chunks_to_writer(ranged_blocks, writer)
            .await?;

        if let Some(p) = &path {
            debug!("Done smudging file {p:?}");
        }

        Ok(())
    }

    /// This function does not return, but any results are sent
    /// through the mpsc channel
    pub async fn smudge_file_from_pointer_to_mpsc(
        &self,
        path: &Path,
        pointer: &PointerFile,
        writer: &Sender<Result<Vec<u8>>>,
        ready: &Option<watch::Sender<bool>>,
        progress_indicator: &Option<Arc<DataProgressReporter>>,
    ) -> usize {
        info!("Smudging file {:?}", &path);

        let Ok(hash) = pointer.hash() else {
            error!(
                "Unable to parse hash {:?} in pointer file for path {:?}",
                pointer.hash_string(),
                path
            );
            return 0;
        };

        let blocks = match self.derive_blocks(&hash).await {
            Ok(b) => b,
            Err(e) => {
                if let Err(e) = writer.send(Err(e)).await {
                    error!("Unable to send smudge error {:?} as channel has closed", e);
                }
                return 0;
            }
        };

        match self
            .data_from_chunks_to_mpsc(blocks, writer, ready, progress_indicator)
            .await
        {
            Ok(r) => {
                debug!("Done smudging file {:?}", &path);
                r
            }
            Err(e) => {
                if let Some(is_ready) = ready {
                    let _ = is_ready.send(true);
                }
                if let Err(e) = writer.send(Err(e)).await {
                    error!("Unable to send smudge error {:?} as channel has closed", e);
                }
                0
            }
        }
    }

    async fn flush(&self) -> Result<()> {
        self.shard_manager.flush().await?;
        Ok(())
    }

    /// To be called at the end of a batch of clean/smudge operations.
    /// Commits all MerkleDB changes to disk.
    pub async fn finalize(&self) -> Result<()> {
        self.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use merkledb::constants::TARGET_CDC_CHUNK_SIZE;
    use tempfile::TempDir;

    use super::data_processing_v1::PointerFileTranslatorV1;
    use crate::constants::*;
    use crate::stream::data_iterators::AsyncFileIterator;

    use super::*;

    #[tokio::test]
    async fn test_smudge_passthrough() {
        // build an input of "hello world"
        let input_bytes: Vec<u8> = "hello world".bytes().collect();
        let input = std::io::Cursor::new(input_bytes.clone());
        let async_input = AsyncFileIterator::new(input, GIT_MAX_PACKET_SIZE);

        // make a translator
        let stagedir = TempDir::new().unwrap();
        let repo = PointerFileTranslatorV1::new_temporary(stagedir.path());

        // smudge the input with passthrough flag set
        let mut output = std::io::Cursor::new(Vec::new());
        repo.smudge_file(&PathBuf::new(), async_input, &mut output, true, None)
            .await
            .unwrap();
        // result should be identical
        let mut output_bytes: Vec<u8> = Vec::new();
        output.set_position(0);
        output.read_to_end(&mut output_bytes).unwrap();
        assert_eq!(input_bytes, output_bytes);
    }

    #[tokio::test]
    async fn test_smudge_passthrough_with_range() {
        // build an input of "hello world"
        let input_bytes: Vec<u8> = "hello world".bytes().collect();
        let input = std::io::Cursor::new(input_bytes.clone());
        let async_input = AsyncFileIterator::new(input, GIT_MAX_PACKET_SIZE);

        // make a translator
        let stagedir = TempDir::new().unwrap();
        let repo = PointerFileTranslatorV1::new_temporary(stagedir.path());

        // smudge the input with passthrough flag set
        let mut output = std::io::Cursor::new(Vec::new());
        repo.smudge_file(
            &PathBuf::new(),
            async_input,
            &mut output,
            true,
            Some((0, 3)),
        )
        .await
        .unwrap();
        // result should be identical
        let mut output_bytes: Vec<u8> = Vec::new();
        output.set_position(0);
        output.read_to_end(&mut output_bytes).unwrap();

        let expected_bytes: Vec<u8> = "hel".bytes().collect();
        assert_eq!(expected_bytes, output_bytes);
    }

    #[tokio::test]
    async fn test_clean_smudge_round_trip_no_small_file() {
        // build an input of "hello world"
        let input_bytes: Vec<u8> = "hello world".bytes().collect();
        let input = std::io::Cursor::new(input_bytes.clone());
        let async_input = AsyncFileIterator::new(input, GIT_MAX_PACKET_SIZE);

        // make a translator, disabling small file
        let stagedir = TempDir::new().unwrap();
        let mut repo = PointerFileTranslatorV2::new_temporary(stagedir.path())
            .await
            .unwrap();
        repo.small_file_threshold = 0;

        // clean the file
        let cleaned = repo.clean_file(&PathBuf::new(), async_input).await.unwrap();
        repo.finalize_cleaning().await.unwrap();
        // check that the cleaned file parses correctly
        let ptr_file = PointerFile::init_from_string(std::str::from_utf8(&cleaned).unwrap(), "");
        assert!(ptr_file.is_valid());

        {
            let clean_cursor = std::io::Cursor::new(cleaned.clone());
            let async_clean_input = AsyncFileIterator::new(clean_cursor, GIT_MAX_PACKET_SIZE);
            // smudge without passthrough flagged
            let mut smudged = std::io::Cursor::new(Vec::new());
            repo.smudge_file(
                &PathBuf::new(),
                async_clean_input,
                &mut smudged,
                false,
                None,
            )
            .await
            .unwrap();
            // result should be identical
            smudged.set_position(0);
            let mut smudged_bytes: Vec<u8> = Vec::new();
            smudged.read_to_end(&mut smudged_bytes).unwrap();
            assert_eq!(input_bytes, smudged_bytes);
        }
        {
            let clean_cursor = std::io::Cursor::new(cleaned.clone());
            let async_clean_input = AsyncFileIterator::new(clean_cursor, GIT_MAX_PACKET_SIZE);
            // smudge with passthrough flagged
            // Since this is a valid pointer file, we should smudge as expected
            let mut smudged = std::io::Cursor::new(Vec::new());
            repo.smudge_file(&PathBuf::new(), async_clean_input, &mut smudged, true, None)
                .await
                .unwrap();
            // result should be identical
            smudged.set_position(0);
            let mut smudged_bytes: Vec<u8> = Vec::new();
            smudged.read_to_end(&mut smudged_bytes).unwrap();
            assert_eq!(input_bytes, smudged_bytes);
        }
        {
            let clean_cursor = std::io::Cursor::new(cleaned.clone());
            let async_clean_input = AsyncFileIterator::new(clean_cursor, GIT_MAX_PACKET_SIZE);
            // smudge with passthrough flagged
            // Since this is a valid pointer file, we should smudge as expected
            let mut smudged = std::io::Cursor::new(Vec::new());
            repo.smudge_file(
                &PathBuf::new(),
                async_clean_input,
                &mut smudged,
                true,
                Some((3, 6)),
            )
            .await
            .unwrap();
            // result should be identical
            smudged.set_position(0);
            let mut smudged_bytes: Vec<u8> = Vec::new();
            smudged.read_to_end(&mut smudged_bytes).unwrap();
            assert_eq!("lo ".bytes().collect::<Vec<u8>>(), smudged_bytes);
        }
    }
    #[tokio::test]
    async fn test_clean_smudge_round_trip_with_constant_file() {
        // build an input of "hello world" repeated
        let input_bytes: Vec<u8> = "hello world! "
            .repeat(2 * TARGET_CDC_CHUNK_SIZE) // make sure it repeats enough times to chunk properly
            .bytes()
            .collect();
        let input = std::io::Cursor::new(input_bytes.clone());
        let async_input = AsyncFileIterator::new(input, GIT_MAX_PACKET_SIZE);

        // make a translator
        let stagedir = TempDir::new().unwrap();
        let repo = PointerFileTranslatorV2::new_temporary(stagedir.path())
            .await
            .unwrap();

        // clean the file
        let cleaned = repo.clean_file(&PathBuf::new(), async_input).await.unwrap();
        repo.finalize_cleaning().await.unwrap();

        let clean_cursor = std::io::Cursor::new(cleaned.clone());
        let async_clean_input = AsyncFileIterator::new(clean_cursor, GIT_MAX_PACKET_SIZE);
        // smudge with passthrough flagged
        let mut smudged = std::io::Cursor::new(Vec::new());
        repo.smudge_file(&PathBuf::new(), async_clean_input, &mut smudged, true, None)
            .await
            .unwrap();
        // result should be identical
        smudged.set_position(0);
        let mut smudged_bytes: Vec<u8> = Vec::new();
        smudged.read_to_end(&mut smudged_bytes).unwrap();
        assert_eq!(input_bytes, smudged_bytes);

        let clean_cursor = std::io::Cursor::new(cleaned.clone());
        let async_clean_input = AsyncFileIterator::new(clean_cursor, GIT_MAX_PACKET_SIZE);
        // smudge with passthrough flagged
        let mut smudged = std::io::Cursor::new(Vec::new());
        repo.smudge_file(
            &PathBuf::new(),
            async_clean_input,
            &mut smudged,
            true,
            Some((3, 6)),
        )
        .await
        .unwrap();
        // result should be identical
        smudged.set_position(0);
        let mut smudged_bytes: Vec<u8> = Vec::new();
        smudged.read_to_end(&mut smudged_bytes).unwrap();
        assert_eq!("lo ".bytes().collect::<Vec<u8>>(), smudged_bytes);
    }
    #[tokio::test]
    async fn test_clean_smudge_round_trip_with_small_file() {
        // build an input of "hello world"
        let input_bytes: Vec<u8> = "hello world".bytes().collect();
        let input = std::io::Cursor::new(input_bytes.clone());
        let async_input = AsyncFileIterator::new(input, GIT_MAX_PACKET_SIZE);

        // make a translator
        let stagedir = TempDir::new().unwrap();
        let repo = PointerFileTranslatorV2::new_temporary(stagedir.path())
            .await
            .unwrap();

        // clean the file
        let cleaned = repo.clean_file(&PathBuf::new(), async_input).await.unwrap();
        repo.finalize_cleaning().await.unwrap();

        let clean_cursor = std::io::Cursor::new(cleaned.clone());
        let async_clean_input = AsyncFileIterator::new(clean_cursor, GIT_MAX_PACKET_SIZE);
        // smudge with passthrough flagged
        let mut smudged = std::io::Cursor::new(Vec::new());
        repo.smudge_file(&PathBuf::new(), async_clean_input, &mut smudged, true, None)
            .await
            .unwrap();
        // result should be identical
        smudged.set_position(0);
        let mut smudged_bytes: Vec<u8> = Vec::new();
        smudged.read_to_end(&mut smudged_bytes).unwrap();
        assert_eq!(input_bytes, smudged_bytes);

        let clean_cursor = std::io::Cursor::new(cleaned.clone());
        let async_clean_input = AsyncFileIterator::new(clean_cursor, GIT_MAX_PACKET_SIZE);
        // smudge with passthrough flagged
        let mut smudged = std::io::Cursor::new(Vec::new());
        repo.smudge_file(
            &PathBuf::new(),
            async_clean_input,
            &mut smudged,
            true,
            Some((3, 6)),
        )
        .await
        .unwrap();
        // result should be identical
        smudged.set_position(0);
        let mut smudged_bytes: Vec<u8> = Vec::new();
        smudged.read_to_end(&mut smudged_bytes).unwrap();
        assert_eq!("lo ".bytes().collect::<Vec<u8>>(), smudged_bytes);
    }

    #[tokio::test]
    async fn test_clean_smudge_round_trip_with_bad_file_lookup() {
        // build an input of "hello world"
        let input_bytes: Vec<u8> = "hello world".bytes().collect();
        let input = std::io::Cursor::new(input_bytes.clone());
        let async_input = AsyncFileIterator::new(input, GIT_MAX_PACKET_SIZE);

        // make a translator
        let stagedir = TempDir::new().unwrap();
        let repo = PointerFileTranslatorV2::new_temporary(stagedir.path())
            .await
            .unwrap();

        // clean the file
        let cleaned = repo.clean_file(&PathBuf::new(), async_input).await.unwrap();
        repo.finalize_cleaning().await.unwrap();

        let clean_cursor = std::io::Cursor::new(cleaned.clone());
        let async_clean_input = AsyncFileIterator::new(clean_cursor, GIT_MAX_PACKET_SIZE);

        // smudge with passthrough flagged
        let mut smudged = std::io::Cursor::new(Vec::new());
        repo.smudge_file(&PathBuf::new(), async_clean_input, &mut smudged, true, None)
            .await
            .unwrap();

        // result should be identical
        smudged.set_position(0);
        let mut smudged_bytes: Vec<u8> = Vec::new();
        smudged.read_to_end(&mut smudged_bytes).unwrap();
        assert_eq!(input_bytes, smudged_bytes);

        let clean_cursor = std::io::Cursor::new(cleaned.clone());
        let async_clean_input = AsyncFileIterator::new(clean_cursor, GIT_MAX_PACKET_SIZE);

        // Now attempt with the lookup cleared.
        repo.shard_manager.clear().await;

        // smudge with passthrough flagged
        let mut smudged = std::io::Cursor::new(Vec::new());
        repo.smudge_file(&PathBuf::new(), async_clean_input, &mut smudged, true, None)
            .await
            .unwrap();

        // result should be identical to the pointer file
        smudged.set_position(0);
        let mut smudged_bytes: Vec<u8> = Vec::new();
        smudged.read_to_end(&mut smudged_bytes).unwrap();
        assert_eq!(cleaned, smudged_bytes);
    }
    #[tokio::test]
    async fn test_clean_smudge_round_trip_with_small_file_range() {
        // build an input of "hello world"
        let input_bytes: Vec<u8> = "hello world hi how are ya".bytes().collect();
        let input = std::io::Cursor::new(input_bytes.clone());
        let async_input = AsyncFileIterator::new(input, GIT_MAX_PACKET_SIZE);

        // make a translator
        let stagedir = TempDir::new().unwrap();
        let repo = PointerFileTranslatorV1::new_temporary(stagedir.path());

        // clean the file
        let cleaned = repo.clean_file(&PathBuf::new(), async_input).await.unwrap();
        repo.finalize_cleaning().await.unwrap();

        let clean_cursor = std::io::Cursor::new(cleaned.clone());
        let async_clean_input = AsyncFileIterator::new(clean_cursor, GIT_MAX_PACKET_SIZE);
        // smudge with passthrough flagged
        let mut smudged = std::io::Cursor::new(Vec::new());
        repo.smudge_file(
            &PathBuf::new(),
            async_clean_input,
            &mut smudged,
            true,
            Some((0, 3)),
        )
        .await
        .unwrap();
        // result should be identical
        smudged.set_position(0);
        let mut smudged_bytes: Vec<u8> = Vec::new();
        smudged.read_to_end(&mut smudged_bytes).unwrap();
        let expected_range: Vec<u8> = "hel".bytes().collect();
        assert_eq!(expected_range, smudged_bytes);

        let clean_cursor = std::io::Cursor::new(cleaned.clone());
        let async_clean_input = AsyncFileIterator::new(clean_cursor, GIT_MAX_PACKET_SIZE);
        let mut smudged = std::io::Cursor::new(Vec::new());
        repo.smudge_file(
            &PathBuf::new(),
            async_clean_input,
            &mut smudged,
            true,
            Some((4, 8)),
        )
        .await
        .unwrap();
        // result should be identical
        smudged.set_position(0);
        let mut smudged_bytes: Vec<u8> = Vec::new();
        smudged.read_to_end(&mut smudged_bytes).unwrap();
        let expected_range = "o wo".bytes().collect::<Vec<u8>>();
        assert_eq!(expected_range, smudged_bytes);

        let clean_cursor = std::io::Cursor::new(cleaned.clone());
        let async_clean_input = AsyncFileIterator::new(clean_cursor, GIT_MAX_PACKET_SIZE);
        let mut smudged = std::io::Cursor::new(Vec::new());
        repo.smudge_file(
            &PathBuf::new(),
            async_clean_input,
            &mut smudged,
            true,
            Some((23, 100)),
        )
        .await
        .unwrap();
        // result should be identical
        smudged.set_position(0);
        let mut smudged_bytes: Vec<u8> = Vec::new();
        smudged.read_to_end(&mut smudged_bytes).unwrap();
        let expected_range = "ya".bytes().collect::<Vec<u8>>();
        assert_eq!(expected_range, smudged_bytes);
    }

    #[tokio::test]
    async fn test_clean_zero_byte_no_small_file() {
        // build an input of "hello world"
        let input_bytes: Vec<u8> = Vec::new();
        let input = std::io::Cursor::new(input_bytes.clone());
        let async_input = AsyncFileIterator::new(input, GIT_MAX_PACKET_SIZE);

        // make a translator
        // we set the small file threshold to 0
        let stagedir = TempDir::new().unwrap();
        let mut repo = PointerFileTranslatorV2::new_temporary(stagedir.path())
            .await
            .unwrap();
        repo.small_file_threshold = 0;

        // clean the file
        let cleaned = repo.clean_file(&PathBuf::new(), async_input).await.unwrap();
        repo.finalize_cleaning().await.unwrap();
        // check that the cleaned file parses correctly
        let ptr_file = PointerFile::init_from_string(std::str::from_utf8(&cleaned).unwrap(), "");
        // the empty file has a merklehash of 0s
        assert_eq!(ptr_file.hash().unwrap(), MerkleHash::default());
        assert!(ptr_file.is_valid());
    }
    #[tokio::test]
    async fn test_clean_zero_byte_with_small_file() {
        // build an input of "hello world"
        let input_bytes: Vec<u8> = Vec::new();
        let input = std::io::Cursor::new(input_bytes.clone());
        let async_input = AsyncFileIterator::new(input, GIT_MAX_PACKET_SIZE);

        // make a translator
        let stagedir = TempDir::new().unwrap();
        let repo = PointerFileTranslatorV2::new_temporary(stagedir.path())
            .await
            .unwrap();

        // clean the file
        let cleaned = repo.clean_file(&PathBuf::new(), async_input).await.unwrap();
        repo.finalize_cleaning().await.unwrap();
        assert_eq!(cleaned, input_bytes);
    }
}
