use std::clone::Clone;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cas::output_bytes;
use cas_client::Staging;
use futures::prelude::stream::*;
use lru::LruCache;
use merkledb::constants::TARGET_CAS_BLOCK_SIZE;
use merkledb::prelude_v2::*;
use merkledb::*;
use merklehash::MerkleHash;
use pointer_file::PointerFile;
use progress_reporting::DataProgressReporter;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tracing::{debug, error, info, info_span};
use tracing_futures::Instrument;

use crate::async_iterator_with_putback::AsyncIteratorWithPutBack;
use crate::config::XetConfig;
use crate::constants::{
    CURRENT_VERSION, DERIVE_BLOCKS_CACHE_COUNT, GIT_NOTES_SUMMARIES_REF_NAME,
    MAX_CONCURRENT_DOWNLOADS, MAX_CONCURRENT_PREFETCHES, MAX_CONCURRENT_PREFETCH_DOWNLOADS,
    MAX_CONCURRENT_UPLOADS, PREFETCH_TRACK_COUNT, PREFETCH_WINDOW_SIZE_BYTES, SMALL_FILE_THRESHOLD,
};
use crate::data_processing::{
    create_cas_client, data_from_chunks_to_writer, get_from_cas, pointer_file_from_reader,
    slice_object_range,
};
use crate::errors::{convert_cas_error, GitXetRepoError, Result};
use crate::small_file_determination::is_small_file;
use crate::summaries::analysis::FileAnalyzers;
use crate::summaries::csv::CSVAnalyzer;
use crate::summaries_plumb::WholeRepoSummary;

use lazy_static::lazy_static;
use prometheus::{register_int_counter, IntCounter};

lazy_static! {
    pub static ref FILTER_CAS_BYTES_PRODUCED: IntCounter = register_int_counter!(
        "filter_process_cas_bytes_produced",
        "Number of CAS bytes produced during cleaning"
    )
    .unwrap();
    pub static ref FILTER_BYTES_CLEANED: IntCounter =
        register_int_counter!("filter_process_bytes_cleaned", "Number of bytes cleaned").unwrap();
    pub static ref FILTER_BYTES_SMUDGED: IntCounter =
        register_int_counter!("filter_process_bytes_smudged", "Number of bytes smudged").unwrap();
    pub static ref GIT_XET_VERION: String =
        std::env::var("XET_VERSION").unwrap_or_else(|_| CURRENT_VERSION.to_string());
}
#[allow(clippy::borrowed_box)]
async fn upload_to_cas(
    prefix: &str,
    casnodes: Vec<MerkleNode>,
    casroot: &MerkleNode,
    cas: &Arc<dyn Staging + Send + Sync>,
    buf: Vec<u8>,
) -> Result<()> {
    let mut chunk_boundaries: Vec<u64> = Vec::new();
    let mut running_sum = 0;

    for leaf in casnodes {
        running_sum += leaf.len();
        chunk_boundaries.push(running_sum as u64);
    }

    cas.put(prefix, casroot.hash(), buf, chunk_boundaries)
        .await?;

    Ok(())
}

#[derive(Default)]
struct CasAccumulator {
    /// Buffer used to track Xorb contents across invocations to clean_file
    /// to allow us to combine a bunch of small files into a single marge Xorb.
    casbuf: Vec<u8>,
    casnodes: Vec<MerkleNode>,
}

type PrefetchJoinType = tokio::task::JoinHandle<()>;

/// Manages the translation of files between the
/// MerkleDB / pointer file format and the materialized version.
///
/// This class handles the clean and smudge options.
pub struct PointerFileTranslatorV1 {
    initial_mdb_sequence_number: u64,
    pub mdb: Mutex<MerkleMemDB>,
    summarydb: Arc<Mutex<WholeRepoSummary>>,
    cas: Arc<dyn Staging + Send + Sync>,
    prefix: String,
    small_file_threshold: usize,
    cas_accumulator: Arc<Mutex<CasAccumulator>>,
    prefetches: Arc<Mutex<HashMap<(MerkleHash, u64), PrefetchJoinType>>>,
    prefetched: Arc<Mutex<LruCache<(MerkleHash, u64), ()>>>,
    derive_blocks_cache: Mutex<LruCache<MerkleHash, Vec<ObjectRange>>>,
    cfg: XetConfig,
}

impl PointerFileTranslatorV1 {
    /// Constructor
    pub async fn from_config(config: &XetConfig) -> Result<Self> {
        let cas = create_cas_client(config).await?;
        let mut mdb = MerkleMemDB::open(&config.merkledb)?;
        // autosync on drop is the cause for some ctrl-c resilience issues
        // as this means that on certain non-panicing IO errors
        // (like say out of disk space errors) which we propagate back in Result,
        // we may still persist the MerkleDB state.
        mdb.autosync_on_drop(false);
        let summarydb = Arc::new(Mutex::new(
            WholeRepoSummary::load_or_recreate_from_git(
                config,
                &config.summarydb,
                GIT_NOTES_SUMMARIES_REF_NAME,
            )
            .await?,
        ));

        // let axe = Axe::new("DataPipeline", &config.clone(), None).await.ok();
        Ok(Self {
            initial_mdb_sequence_number: mdb.get_sequence_number(),
            mdb: Mutex::new(mdb),
            summarydb,
            cas,
            prefix: config.cas.prefix.clone(),
            small_file_threshold: SMALL_FILE_THRESHOLD,
            cas_accumulator: Arc::new(Mutex::new(CasAccumulator::default())),
            prefetches: Arc::new(Mutex::new(HashMap::new())),
            prefetched: Arc::new(Mutex::new(LruCache::new(PREFETCH_TRACK_COUNT))),
            derive_blocks_cache: Mutex::new(LruCache::new(DERIVE_BLOCKS_CACHE_COUNT)),
            cfg: config.clone(),
        })
    }
    /// Creates a PointerFileTranslator that has ephemeral DBs
    /// (MerkleDB and SummaryDB) but still respects the rest of the config.
    pub async fn from_config_ephemeral(config: &XetConfig) -> Result<Self> {
        let cas = create_cas_client(config).await?;
        let mdb = MerkleMemDB::default();
        let summarydb = Arc::new(Mutex::new(WholeRepoSummary::empty(&PathBuf::default())));

        Ok(Self {
            initial_mdb_sequence_number: mdb.get_sequence_number(),
            mdb: Mutex::new(mdb),
            summarydb,
            cas,
            prefix: config.cas.prefix.clone(),
            small_file_threshold: SMALL_FILE_THRESHOLD,
            cas_accumulator: Arc::new(Mutex::new(CasAccumulator::default())),
            prefetches: Arc::new(Mutex::new(HashMap::new())),
            prefetched: Arc::new(Mutex::new(LruCache::new(PREFETCH_TRACK_COUNT))),
            derive_blocks_cache: Mutex::new(LruCache::new(DERIVE_BLOCKS_CACHE_COUNT)),
            cfg: config.clone(),
        })
    }

    pub async fn refresh(&self) -> Result<()> {
        let mut mdb = MerkleMemDB::open(&self.cfg.merkledb)?;
        // autosync on drop is the cause for some ctrl-c resilience issues
        // as this means that on certain non-panicing IO errors
        // (like say out of disk space errors) which we propagate back in Result,
        // we may still persist the MerkleDB state.
        mdb.autosync_on_drop(false);
        let summarydb = WholeRepoSummary::load_or_recreate_from_git(
            &self.cfg,
            &self.cfg.summarydb,
            GIT_NOTES_SUMMARIES_REF_NAME,
        )
        .await?;

        *self.mdb.lock().await = mdb;
        *self.summarydb.lock().await = summarydb;

        Ok(())
    }

    async fn try_flush_accumulator(&self, acc: &mut CasAccumulator, force: bool) -> Result<usize> {
        let mut cas_bytes_produced = 0;
        if !acc.casnodes.is_empty() && (force || acc.casbuf.len() >= TARGET_CAS_BLOCK_SIZE) {
            cas_bytes_produced += acc.casbuf.len();
            let cnodes = std::mem::take(&mut acc.casnodes);
            let buf = std::mem::take(&mut acc.casbuf);
            let mut mdb = self.mdb.lock().await;
            let casroot = mdb.merge_to_cas(&cnodes);
            drop(mdb);
            upload_to_cas(&self.prefix, cnodes, &casroot, &self.cas, buf).await?;
        }
        Ok(cas_bytes_produced)
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

        let mut filenodes: Vec<MerkleNode> = Vec::new();

        let mut generator = async_chunk_target_default(&mut reader);
        let mut bytes_cleaned: usize = 0;
        let mut cas_bytes_produced: usize = 0;
        // TODO: This span isn't quite accurate as we hold it across `await` calls.
        //       We should probably refactor this code to better support tracing the time spent.
        let span = info_span!("chunk_file");
        let chunk_scope = span.enter();

        // we accumulate a local cas node buffer before flushing it
        // to CAS all at once
        let mut localacc = CasAccumulator::default();

        loop {
            match generator.next().await {
                GenType::Yielded((chunk, bytes)) => {
                    let mut mdb = self.mdb.lock().await;
                    let (node, new) = mdb.add_chunk(&chunk);
                    drop(mdb);
                    bytes_cleaned += node.len();
                    filenodes.push(node.clone());
                    if new {
                        localacc.casnodes.push(node.clone());
                        localacc.casbuf.extend_from_slice(&bytes[..]);
                        cas_bytes_produced +=
                            self.try_flush_accumulator(&mut localacc, false).await?;
                    }
                    // Run through any analyzers, if appropriate
                    analyzers.process_chunk(&bytes[..], path, bytes_cleaned);

                    if let Some(pi) = progress_indicator {
                        let mut pi_ref = pi.lock().await;
                        pi_ref.0 = true;
                        pi_ref.1.register_progress(None, node.len());
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
        // stuff any remaining locally accumulated stuff into the global accumulator
        {
            let mut casacc = self.cas_accumulator.lock().await;
            casacc.casnodes.extend_from_slice(&localacc.casnodes[..]);
            casacc.casbuf.extend_from_slice(&localacc.casbuf[..]);
            cas_bytes_produced += self.try_flush_accumulator(&mut casacc, false).await?;
        }

        // we only add to the counters if we see changes
        FILTER_BYTES_CLEANED.inc_by(bytes_cleaned as u64);
        FILTER_CAS_BYTES_PRODUCED.inc_by(cas_bytes_produced as u64);

        drop(chunk_scope);

        let span = info_span!("to_pointerfile");
        let _scope = span.enter();

        let filenode = if !filenodes.is_empty() {
            let mut mdb = self.mdb.lock().await;
            mdb.merge_to_file(&filenodes)
        } else {
            MerkleNode::default()
        };
        let pointer_file: PointerFile = PointerFile::init_from_info(
            path.to_str().unwrap(),
            &filenode.hash().hex(),
            filenode.len() as u64,
        );

        // For each of the analyzers, add data to the notes as appropriate.
        let key = filenode.hash().hex();
        let mut summarydb = self.summarydb.lock().await;
        let existing_file_summary = summarydb.entry(key.clone()).or_default();
        if let Some(new_file_summary) = analyzers.finalize(path) {
            existing_file_summary.merge_in(new_file_summary, &key);
        }

        Ok(pointer_file.to_string().as_bytes().to_vec())
    }

    /// To be called after a collection of clean_file calls.
    /// Can be safely called even if no cleaning happened.
    pub async fn finalize_cleaning(&self) -> Result<()> {
        self.summarydb.lock().await.flush()?;
        let mut casacc = self.cas_accumulator.lock().await;
        let cas_bytes_produced = self.try_flush_accumulator(&mut casacc, true).await?;
        FILTER_CAS_BYTES_PRODUCED.inc_by(cas_bytes_produced as u64);
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

    /// Performs a prefetch heuristic assuming that the user wll be reading at
    /// the provided start position,
    ///
    /// The file is cut into chunks of PREFETCH_WINDOW_SIZE_MB.
    /// The prefetch will start a task to download the chunk which contains
    /// the byte X.
    ///
    /// Returns true if a prefetch was started, and false otherwise
    pub async fn prefetch(&self, pointer: &PointerFile, start: u64) -> Result<bool> {
        let chunknum = start / PREFETCH_WINDOW_SIZE_BYTES;
        let start = chunknum * PREFETCH_WINDOW_SIZE_BYTES;
        if start >= pointer.filesize() {
            return Ok(true);
        }
        let hash = MerkleHash::from_hex(pointer.hash())
            .map_err(|_| GitXetRepoError::StreamParseError("Unable to parse hash".to_string()))?;
        // if we are prefetching, or we have prefetched recently, return ok
        if self.prefetched.lock().await.contains(&(hash, chunknum)) {
            return Ok(false);
        }
        {
            let cur_prefetches = self.prefetches.lock().await;
            if cur_prefetches.contains_key(&(hash, chunknum))
                || cur_prefetches.len() >= MAX_CONCURRENT_PREFETCHES
            {
                return Ok(false);
            }
        }

        // unlock to compute the blocks. This may take a while
        let blocks = self
            .derive_blocks(pointer)
            .instrument(info_span!("derive_blocks"))
            .await?;
        let blocks =
            slice_object_range(&blocks, start as usize, PREFETCH_WINDOW_SIZE_BYTES as usize);

        // re-lock and re-valiate the cur-prefetches
        {
            let mut cur_prefetches = self.prefetches.lock().await;
            if cur_prefetches.contains_key(&(hash, chunknum))
                || cur_prefetches.len() >= MAX_CONCURRENT_PREFETCH_DOWNLOADS
            {
                return Ok(false);
            }
            debug!("Prefetching {:?}", (hash, chunknum));
            let prefix = self.prefix.clone();
            let prefetches = self.prefetches.clone();
            let prefetched = self.prefetched.clone();
            let cas = self.cas.clone();
            let task = tokio::task::spawn(async move {
                let mut strm = iter(blocks.into_iter().map(|objr| {
                    let prefix = prefix.clone();
                    get_from_cas(
                        &cas,
                        prefix,
                        objr.hash,
                        (objr.start as u64, objr.end as u64),
                    )
                }))
                .buffered(MAX_CONCURRENT_PREFETCH_DOWNLOADS);
                // throw away all the stream
                while strm.next().await.is_some() {}
                debug!("Prefetching Complete {:?}", (hash, chunknum));
                prefetches.lock().await.remove(&(hash, chunknum));
                prefetched.lock().await.put((hash, chunknum), ());
            });
            cur_prefetches.insert((hash, chunknum), task);
        }
        Ok(true)
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
        if let Some(res) = self.derive_blocks_cache.lock().await.get(&hash) {
            return Ok(res.clone());
        }

        let mut block_v = {
            let mdb = self.mdb.lock().await;

            debug!("Extracting object for hash {:?}", pointer.hash());
            let node = mdb.find_node(&hash).ok_or(GitXetRepoError::HashNotFound)?;
            mdb.reconstruct_from_cas(&[node])?
        };
        if block_v.len() != 1 {
            return Err(GitXetRepoError::Other(format!(
                "Unable to reconstruct CAS information for hash {:?}",
                pointer.hash()
            )));
        }
        let res = std::mem::take(&mut block_v[0].1);
        self.derive_blocks_cache.lock().await.put(hash, res.clone());
        Ok(res)
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
                slice_object_range(&blocks, start, end - start)
            }
            None => blocks,
        };

        data_from_chunks_to_writer(&self.cas, self.prefix.clone(), ranged_blocks, writer).await?;

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
        let mut mdb = self.mdb.lock().await;
        // Only flush it if it's actually changed.
        if mdb.get_sequence_number() != self.initial_mdb_sequence_number {
            mdb.flush()?;
        }
        Ok(())
    }

    /// To be called at the end of a batch of clean/smudge operations.
    /// Commits all MerkleDB changes to disk.
    pub async fn finalize(&self) -> Result<()> {
        self.flush().await?;
        Ok(())
    }

    /// Reload the MerkleDB from disk to memory.
    pub async fn reload_mdb(&self) {
        let mut mdb = self.mdb.lock().await;
        let mdbpath = mdb.get_path().to_path_buf();
        if let Ok(newdb) = MerkleMemDB::open(mdbpath) {
            mdb.assign_from(&newdb);
        }
    }

    /// Only needed for testing
    #[cfg(test)]
    pub fn new_temporary(stage_path: &Path) -> Self {
        let mdb = MerkleMemDB::default();
        let summarydb = Arc::new(Mutex::new(WholeRepoSummary::empty(&PathBuf::default())));

        let localclient = cas_client::LocalClient::default();
        let cas = cas_client::new_staging_client(localclient, Some(stage_path));

        Self {
            initial_mdb_sequence_number: 0,
            mdb: Mutex::new(mdb),
            summarydb,
            cas,
            prefix: "".into(),
            small_file_threshold: SMALL_FILE_THRESHOLD,
            cas_accumulator: Arc::new(Mutex::new(CasAccumulator::default())),
            prefetches: Arc::new(Mutex::new(HashMap::new())),
            prefetched: Arc::new(Mutex::new(LruCache::new(PREFETCH_TRACK_COUNT))),
            derive_blocks_cache: Mutex::new(LruCache::new(DERIVE_BLOCKS_CACHE_COUNT)),
            cfg: XetConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use tempfile::TempDir;

    use crate::async_file_iterator::*;
    use crate::constants::*;

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
        let mut repo = PointerFileTranslatorV1::new_temporary(stagedir.path());
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
    async fn test_clean_smudge_round_trip_with_small_file() {
        // build an input of "hello world"
        let input_bytes: Vec<u8> = "hello world".bytes().collect();
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
        let mut repo = PointerFileTranslatorV1::new_temporary(stagedir.path());
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
        let repo = PointerFileTranslatorV1::new_temporary(stagedir.path());

        // clean the file
        let cleaned = repo.clean_file(&PathBuf::new(), async_input).await.unwrap();
        repo.finalize_cleaning().await.unwrap();
        assert_eq!(cleaned, input_bytes);
    }

    #[tokio::test]
    async fn test_slicing() {
        let h1 = merklehash::compute_data_hash("1".as_bytes());
        let h2 = merklehash::compute_data_hash("2".as_bytes());
        let h3 = merklehash::compute_data_hash("3".as_bytes());
        let h4 = merklehash::compute_data_hash("4".as_bytes());
        let v = vec![
            ObjectRange {
                hash: h1,
                start: 100,
                end: 1000,
            },
            ObjectRange {
                hash: h2,
                start: 200,
                end: 201,
            },
            ObjectRange {
                hash: h3,
                start: 200,
                end: 300,
            },
            ObjectRange {
                hash: h4,
                start: 0,
                end: 800,
            },
        ];
        {
            let ret = slice_object_range(&v, 0, 500);
            assert_eq!(ret.len(), 1);
            assert_eq!(ret[0].hash, h1);
            assert_eq!(ret[0].start, 100);
            assert_eq!(ret[0].end, 600);
        }
        {
            let ret = slice_object_range(&v, 0, 1000);
            assert_eq!(ret.len(), 3);
            assert_eq!(ret[0].hash, h1);
            assert_eq!(ret[0].start, 100);
            assert_eq!(ret[0].end, 1000);
            assert_eq!(ret[1].hash, h2);
            assert_eq!(ret[1].start, 200);
            assert_eq!(ret[1].end, 201);
            assert_eq!(ret[2].hash, h3);
            assert_eq!(ret[2].start, 200);
            assert_eq!(ret[2].end, 299);
        }
        {
            let ret = slice_object_range(&v, 899, 3);
            assert_eq!(ret.len(), 3);
            assert_eq!(ret[0].hash, h1);
            assert_eq!(ret[0].start, 999);
            assert_eq!(ret[0].end, 1000);
            assert_eq!(ret[1].hash, h2);
            assert_eq!(ret[1].start, 200);
            assert_eq!(ret[1].end, 201);
            assert_eq!(ret[2].hash, h3);
            assert_eq!(ret[2].start, 200);
            assert_eq!(ret[2].end, 201);
        }
        {
            let ret = slice_object_range(&v, 901, 200);
            assert_eq!(ret.len(), 2);
            assert_eq!(ret[0].hash, h3);
            assert_eq!(ret[0].start, 200);
            assert_eq!(ret[0].end, 300);
            assert_eq!(ret[1].hash, h4);
            assert_eq!(ret[1].start, 0);
            assert_eq!(ret[1].end, 100);
        }
        {
            // past the end slice
            let ret = slice_object_range(&v, 1800, 10);
            assert_eq!(ret.len(), 1);
            assert_eq!(ret[0].hash, h4);
            assert_eq!(ret[0].start, 799);
            assert_eq!(ret[0].end, 800);
        }
        {
            // past the end slice
            let ret = slice_object_range(&v, 1801, 10);
            assert_eq!(ret.len(), 0);
        }
    }
}
