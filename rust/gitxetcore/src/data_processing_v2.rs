use std::clone::Clone;
use std::collections::HashMap;
use std::ffi::OsStr;

use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cas::output_bytes;
use cas_client::*;
use futures::prelude::stream::*;
use mdb_shard::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo};
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use mdb_shard::shard_file_manager::ShardFileManager;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use merkledb::aggregate_hashes::{cas_node_hash, file_node_hash};
use merkledb::constants::TARGET_CAS_BLOCK_SIZE;
use merkledb::*;
use merklehash::MerkleHash;
use parutils::tokio_par_for_each;
use pointer_file::PointerFile;
use progress_reporting::DataProgressReporter;
use std::mem::take;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tracing::{debug, error, info, info_span};
use tracing_futures::Instrument;

use crate::async_iterator_with_putback::AsyncIteratorWithPutBack;
use crate::config::XetConfig;
use crate::constants::*;
use crate::errors::{convert_cas_error, GitXetRepoError, Result};
use crate::git_integration::git_repo::{read_repo_salt, REPO_SALT_LEN};
use crate::merkledb_shard_plumb::download_shard;
use crate::small_file_determination::is_small_file;
use crate::smudge_query_interface::{
    shard_manager_from_config, FileReconstructionInterface, SmudgeQueryPolicy,
};
use crate::summaries::analysis::FileAnalyzers;
use crate::summaries::csv::CSVAnalyzer;
use crate::summaries_plumb::WholeRepoSummary;

pub use crate::data_processing::*;

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
    pending_file_info: Vec<(MDBFileInfo, Vec<usize>)>,
}

/// Manages the translation of files between the
/// MerkleDB / pointer file format and the materialized version.
///
/// This class handles the clean and smudge options.
pub struct PointerFileTranslatorV2 {
    shard_manager: Arc<ShardFileManager>,
    file_reconstructor: Arc<FileReconstructionInterface>,
    summarydb: Arc<Mutex<WholeRepoSummary>>,
    cas: Arc<dyn Staging + Send + Sync>,
    prefix: String,
    small_file_threshold: usize,

    cas_data: Arc<Mutex<CASDataAggregator>>,

    repo_salt: [u8; REPO_SALT_LEN],

    cfg: XetConfig,
}

impl PointerFileTranslatorV2 {
    /// Constructor
    pub async fn from_config(config: &XetConfig) -> Result<Self> {
        let cas_client = create_cas_client(config).await?;

        let summarydb = Arc::new(Mutex::new(
            WholeRepoSummary::load_or_recreate_from_git(
                config,
                &config.summarydb,
                GIT_NOTES_SUMMARIES_REF_NAME,
            )
            .await?,
        ));

        let mut repo_salt = [0u8; REPO_SALT_LEN];
        let salt = read_repo_salt(config.repo_path()?)?;
        repo_salt.copy_from_slice(&salt);

        let shard_manager = Arc::new(shard_manager_from_config(config).await?);

        let file_reconstructor = Arc::new(
            FileReconstructionInterface::new_from_config(config, shard_manager.clone()).await?,
        );

        // let axe = Axe::new("DataPipeline", &config.clone(), None).await.ok();
        Ok(Self {
            shard_manager: shard_manager.clone(),
            file_reconstructor,
            summarydb,
            cas: cas_client,
            prefix: config.cas.prefix.clone(),
            small_file_threshold: SMALL_FILE_THRESHOLD,
            cas_data: Arc::new(Default::default()),
            repo_salt,
            cfg: config.clone(),
        })
    }

    pub async fn refresh(&self) -> Result<()> {
        let summarydb = WholeRepoSummary::load_or_recreate_from_git(
            &self.cfg,
            &self.cfg.summarydb,
            GIT_NOTES_SUMMARIES_REF_NAME,
        )
        .await?;

        *self.summarydb.lock().await = summarydb;

        // See if there are any un-registered shards.
        self.shard_manager
            .register_shards_by_path(&[&self.cfg.merkledb_v2_session, &self.cfg.merkledb_v2_cache])
            .await?;

        Ok(())
    }

    /// New temporary
    #[cfg(test)]
    pub async fn new_temporary(temp_dir: &Path) -> Result<Self> {
        let shard_manager = Arc::new(ShardFileManager::new(temp_dir).await?);
        let file_reconstructor =
            Arc::new(FileReconstructionInterface::new_local(shard_manager.clone()).await?);
        let summarydb = Arc::new(Mutex::new(WholeRepoSummary::empty(&PathBuf::default())));
        let localclient = LocalClient::default();
        let cas = Arc::new(StagingClient::new(Arc::new(localclient), temp_dir));

        Ok(Self {
            shard_manager: shard_manager.clone(),
            file_reconstructor,
            summarydb,
            cas,
            prefix: "".into(),
            small_file_threshold: SMALL_FILE_THRESHOLD,
            cas_data: Arc::new(Default::default()),
            repo_salt: Default::default(),
            cfg: XetConfig::empty(),
        })
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

    pub async fn upload_cas_staged(&self, retain: bool) -> Result<()> {
        self.cas
            .upload_all_staged(MAX_CONCURRENT_UPLOADS, retain)
            .await
            .or_else(convert_cas_error)
    }

    pub async fn clean_file(
        &self,
        path: &Path,
        reader: impl AsyncIterator + Send + Sync,
    ) -> Result<Vec<u8>> {
        self.clean_file_and_report_progress(path, reader, &None)
            .await
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

            let p = download_shard(&self.cfg, &self.cas, &sh, &self.cfg.merkledb_v2_cache).await?;
            self.shard_manager.register_shards_by_path(&[&p]).await?;

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
        mut reader: impl AsyncIterator + Send + Sync,
        progress_indicator: &Option<Arc<Mutex<(bool, DataProgressReporter)>>>,
    ) -> Result<Vec<u8>> {
        // First initialize any analyzers needed.
        let mut analyzers = FileAnalyzers::default();

        debug!("Including analyzers for path {:?}", &path);

        if path.extension() == Some(OsStr::new("csv")) {
            info!("Including CSV analyzer (file extension .csv)");
            analyzers.csv = Some(CSVAnalyzer::default());
        }

        // we consume up to SMALL_FILE_THRESHOLD
        let mut tempbuf: Vec<Vec<u8>> = Vec::new();
        let mut readlen: usize = 0;
        let mut eofed: bool = false;

        while readlen < self.small_file_threshold {
            match reader.next().await? {
                Some(buf) => {
                    readlen += buf.len();
                    tempbuf.push(buf);
                }
                None => {
                    eofed = true;
                    break;
                }
            }
        }
        // We have read till the small file threshold.
        // Read 1 more packet to try to determine if we have hit an EOF.
        if !eofed {
            match reader.next().await? {
                Some(buf) => {
                    tempbuf.push(buf);
                }
                None => {
                    eofed = true;
                }
            }
        }

        // make a put back reader and put everything we consumed
        // as the putback buffer
        let mut reader = AsyncIteratorWithPutBack::new(reader);
        for i in tempbuf.into_iter() {
            reader.putback(&i);
        }

        let small_file_bytes = reader.peek_putback();
        if self.small_file_threshold > 0
            && eofed
            && is_small_file(small_file_bytes, self.small_file_threshold)
        {
            info!("{:?} under the small file threshold", path);
            // this is a small file!!
            // just flush it
            let chunk = reader.flush_putback();
            return Ok(chunk);
        }

        let mut generator = async_chunk_target_default(&mut reader);
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

        // TODO: Put in a fixed size buffer here where we can just do file hash queries if
        // the file is less than a certain threshhold without having to do chunk dedup queries.

        // TODO: If we have a fixed size buffer, tracking the full file hash, it may be that the
        // file already exists, in which case we don't need to try to dedup any of the chunks.
        // For small-ish files, this could be way more efficient.
        loop {
            match generator.next().await {
                GenType::Yielded((chunk, mut bytes)) => {
                    file_hashes.push((chunk.hash, bytes.len()));

                    // Run through any analyzers, if appropriate
                    analyzers.process_chunk(&bytes[..], path, bytes_cleaned);

                    let n_bytes = bytes.len();
                    file_size += n_bytes;
                    bytes_cleaned += n_bytes;

                    if let Some((_dedup_result, fse)) = self
                        .shard_manager
                        .chunk_hash_dedup_query(&[chunk.hash])
                        .await?
                    {
                        // We found the chunk hash present in a cas block somewhere.

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
                    } else {
                        // This is new data.
                        let add_new_data;

                        if let Some(idx) = current_cas_block_hashes.get(&chunk.hash) {
                            let (_, (data_lb, data_ub)) = cas_data.chunks[*idx];

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
                            cas_data.data.append(&mut bytes);

                            if cas_data.data.len() > TARGET_CAS_BLOCK_SIZE {
                                let cas_hash = self.register_new_cas_block(&mut cas_data).await?;

                                for i in current_cas_file_info_indices.iter() {
                                    file_info[*i].cas_hash = cas_hash;
                                }
                                current_cas_file_info_indices.clear();
                                current_cas_block_hashes.clear();
                            }
                        }
                    }

                    if let Some(pi) = progress_indicator {
                        let mut pi_ref = pi.lock().await;
                        pi_ref.0 = true;
                        pi_ref.1.register_progress(None, n_bytes);
                    }
                }
                GenType::Complete(Err(e)) => {
                    return Err(e.into());
                }
                GenType::Complete(Ok(())) => {
                    break;
                }
            }
        }

        let file_hash = file_node_hash(&file_hashes, &self.repo_salt)?;

        // Is it registered already?
        let file_already_registered = match self.file_reconstructor.smudge_query_policy {
            SmudgeQueryPolicy::LocalFirst | SmudgeQueryPolicy::LocalOnly => self
                .file_reconstructor
                .shard_manager
                .get_file_reconstruction_info(&file_hash)
                .await?
                .is_some(),
            crate::smudge_query_interface::SmudgeQueryPolicy::ServerOnly => false,
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
            cas_data_accumulator
                .pending_file_info
                .push((new_file_info, current_cas_file_info_indices));

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
        if let Some(new_file_summary) = analyzers.finalize(path) {
            existing_file_summary.merge_in(new_file_summary, &key);
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
        for (mut fi, chunk_hash_indices) in take(&mut cas_data.pending_file_info) {
            for i in chunk_hash_indices {
                debug_assert_eq!(fi.segments[i].cas_hash, MerkleHash::default());
                fi.segments[i].cas_hash = cas_hash;
            }

            self.shard_manager.add_file_reconstruction_info(fi).await?;
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
        Ok(())
    }

    pub fn print_stats(&self) {
        let bytes_cleaned = FILTER_BYTES_CLEANED.get();
        let cas_bytes_produced = FILTER_CAS_BYTES_PRODUCED.get();
        if bytes_cleaned > 0 {
            let ratio: f64 = 100.0 * cas_bytes_produced as f64 / bytes_cleaned as f64;
            eprintln!(
                "{} added, Deduped to {}. Ratio: {:.1}%",
                output_bytes(bytes_cleaned as usize),
                output_bytes(cas_bytes_produced as usize),
                ratio
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
            get_from_cas(
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
        progress_indicator: &Option<Arc<Mutex<(bool, DataProgressReporter)>>>,
    ) -> Result<usize> {
        let mut cas_bytes_retrieved = 0;

        let mut strm = iter(chunks.into_iter().map(|objr| {
            let prefix = self.prefix.clone();
            get_from_cas(
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
                let mut pi_ref = pi.lock().await;
                pi_ref.0 = true;
                pi_ref.1.register_progress(None, buf_len);
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
    /// If passthrough is set, a failed parse of the pointer file will pass
    /// through all the contents directly to the writer.
    pub async fn smudge_file(
        &self,
        path: &PathBuf,
        mut reader: impl AsyncIterator,
        writer: &mut impl std::io::Write,
        passthrough: bool,
        range: Option<(usize, usize)>,
    ) -> Result<()> {
        info!("Smudging file {:?}", &path);

        let (fi, data) =
            pointer_file_from_reader(path, &mut reader, self.cfg.force_no_smudge).await?;
        match fi {
            Some(ptr) => {
                self.smudge_file_from_pointer(path, &ptr, writer, range)
                    .await
            }
            None => {
                if !passthrough {
                    error!("Invalid Pointer File");
                    return Err(GitXetRepoError::Other("Invalid Pointer File".into()));
                }
                info!("{:?} is not a valid pointer file. Passing through", path);
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
                                        "Start range value requested is larger than the file size"
                                            .into(),
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
        }
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
        mut reader: impl AsyncIterator,
        writer: &Sender<Result<Vec<u8>>>,
        ready: &Option<watch::Sender<bool>>,
        progress_indicator: &Option<Arc<Mutex<(bool, DataProgressReporter)>>>,
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
                            let _ = writer.send(Err(e.into())).await.map_err(print_err);
                            return 0;
                        }
                    };
                }
                0
            }
        }
    }

    pub async fn derive_blocks(&self, pointer: &PointerFile) -> Result<Vec<ObjectRange>> {
        let hash = MerkleHash::from_hex(pointer.hash()).map_err(|e| {
            GitXetRepoError::StreamParseError(format!("Error getting hex hash value: {e:?}"))
        })?;

        if let Some((file_info, _shard_hash)) = self
            .file_reconstructor
            .get_file_reconstruction_info(&hash)
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
        path: &PathBuf,
        pointer: &PointerFile,
        writer: &mut impl std::io::Write,
        range: Option<(usize, usize)>,
    ) -> Result<()> {
        info!("Smudging file {:?}", &path);

        let blocks = self
            .derive_blocks(pointer)
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

        debug!("Done smudging file {:?}", &path);

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
        progress_indicator: &Option<Arc<Mutex<(bool, DataProgressReporter)>>>,
    ) -> usize {
        info!("Smudging file {:?}", &path);

        let blocks = match self.derive_blocks(pointer).await {
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

    use crate::async_file_iterator::*;
    use crate::constants::*;
    use crate::data_processing_v1::PointerFileTranslatorV1;

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
        assert_eq!(*ptr_file.hash(), MerkleHash::default().hex());
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
