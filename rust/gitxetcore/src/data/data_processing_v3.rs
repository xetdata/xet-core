use crate::config::XetConfig;
use crate::constants::SMALL_FILE_THRESHOLD;
use crate::data::pointer_file_from_reader;
use crate::git_integration::git_repo_salt::RepoSalt;
use crate::stream::data_iterators::AsyncDataIterator;
use crate::summaries::WholeRepoSummary;

use super::cas_interface::get_from_cas;
use super::clean::Cleaner;
use super::shard_interface::create_shard_manager;
use super::{configurations::*, create_cas_client, remote_shard_interface::RemoteShardInterface};
use super::{errors::*, PointerFile, FILTER_BYTES_SMUDGED, FILTER_CAS_BYTES_PRODUCED};
use cas_client::Staging;
use common_constants::{MAX_CONCURRENT_DOWNLOADS, MAX_CONCURRENT_UPLOADS};
use futures::stream::iter;
use futures::StreamExt;
use mdb_shard::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo};
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::ShardFileManager;
use merkledb::aggregate_hashes::cas_node_hash;
use merkledb::ObjectRange;
use merklehash::MerkleHash;
use progress_reporting::DataProgressReporter;
use std::mem::take;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::{watch, Mutex};
use tracing::{debug, error, info, info_span};
use tracing_futures::Instrument;

#[derive(Default, Debug)]
pub struct CASDataAggregator {
    pub data: Vec<u8>,
    pub chunks: Vec<(MerkleHash, (usize, usize))>,
    // The file info of files that are still being processed.
    // As we're building this up, we assume that all files that do not have a size in the header are
    // not finished yet and thus cannot be uploaded.
    //
    // All the cases the default hash for a cas info entry will be filled in with the cas hash for
    // an entry once the cas block is finalized and uploaded.  These correspond to the indices given
    // alongwith the file info.
    // This tuple contains the file info (which may be modified) and the divisions in the chunks corresponding
    // to this file.
    pub pending_file_info: Vec<(MDBFileInfo, Vec<usize>)>,
}

impl CASDataAggregator {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && self.chunks.is_empty() && self.pending_file_info.is_empty()
    }
}

/// Manages the translation of files between the
/// MerkleDB / pointer file format and the materialized version.
///
/// This class handles the clean and smudge options.
pub struct PointerFileTranslatorV3 {
    /* ----- Configurations ----- */
    config: TranslatorConfig,

    /* ----- Utils ----- */
    shard_manager: Arc<ShardFileManager>,
    remote_shards: Arc<RemoteShardInterface>,
    cas: Arc<dyn Staging + Send + Sync>,

    /* ----- Deduped data shared across files ----- */
    global_cas_data: Arc<Mutex<CASDataAggregator>>,

    /* ----- Deprecated configurations ----- */
    xet: XetConfig,
}

// Constructors
impl PointerFileTranslatorV3 {
    pub async fn new(config: TranslatorConfig) -> Result<PointerFileTranslatorV3> {
        let cas_client = create_cas_client(&config.cas_storage_config, &config.repo_info).await?;

        let shard_manager = Arc::new(create_shard_manager(&config.shard_storage_config).await?);

        let remote_shards = {
            if let Some(dedup) = &config.dedup_config {
                RemoteShardInterface::new(
                    config.file_query_policy,
                    &config.shard_storage_config,
                    Some(shard_manager.clone()),
                    Some(cas_client.clone()),
                    dedup.repo_salt,
                )
                .await?
            } else {
                RemoteShardInterface::new_query_only(
                    config.file_query_policy,
                    &config.shard_storage_config,
                )
                .await?
            }
        };

        Ok(Self {
            config,
            shard_manager,
            remote_shards,
            cas: cas_client,
            global_cas_data: Default::default(),
            xet: XetConfig::empty(),
        })
    }
}

/// Clean operations
impl PointerFileTranslatorV3 {
    /// Start to clean one file. When cleaning multiple files, each file should
    /// be associated with one Cleaner. This allows to launch multiple clean task
    /// simultaneously.
    /// The caller is responsible for memory usage management, the parameter "buffer_size"
    /// indicates the maximum number of Vec<u8> in the internal buffer.
    pub async fn start_clean(
        &self,
        buffer_size: usize,
        file_name: Option<&Path>,
    ) -> Result<Arc<Cleaner>> {
        let Some(ref dedup) = self.config.dedup_config else {
            return Err(DataProcessingError::DedupConfigError(
                "empty dedup config".to_owned(),
            ));
        };

        Cleaner::new(
            dedup.small_file_threshold,
            matches!(dedup.global_dedup_policy, GlobalDedupPolicy::Always),
            self.config.cas_storage_config.prefix.clone(),
            dedup.repo_salt,
            self.shard_manager.clone(),
            self.remote_shards.clone(),
            self.cas.clone(),
            self.global_cas_data.clone(),
            buffer_size,
            file_name,
        )
        .await
    }

    pub async fn finalize_cleaning(&self) -> Result<()> {
        // flush accumulated CAS data.
        let mut cas_data_accumulator = self.global_cas_data.lock().await;
        let mut new_cas_data = take(cas_data_accumulator.deref_mut());
        drop(cas_data_accumulator); // Release the lock.

        if !new_cas_data.is_empty() {
            register_new_cas_block(
                &mut new_cas_data,
                &self.shard_manager,
                &self.cas,
                &self.config.cas_storage_config.prefix,
            )
            .await?;
        }

        debug_assert!(new_cas_data.is_empty());

        self.cas.flush().await?;

        // flush accumulated memory shard.
        self.shard_manager.flush().await?;
        Ok(())
    }
}

/// Clean operation helpers
pub async fn register_new_cas_block(
    cas_data: &mut CASDataAggregator,
    shard_manager: &Arc<ShardFileManager>,
    cas: &Arc<dyn Staging + Send + Sync>,
    cas_prefix: &str,
) -> Result<MerkleHash> {
    let cas_hash = cas_node_hash(&cas_data.chunks[..]);

    let raw_bytes_len = cas_data.data.len();
    // We now assume that the server will compress Xorbs using lz4,
    // without actually compressing the data client-side.
    // The accounting logic will be moved to server-side in the future.
    let compressed_bytes_len = lz4::block::compress(
        &cas_data.data,
        Some(lz4::block::CompressionMode::DEFAULT),
        false,
    )
    .map(|out| out.len())
    .unwrap_or(raw_bytes_len)
    .min(raw_bytes_len);

    let metadata = CASChunkSequenceHeader::new_with_compression(
        cas_hash,
        cas_data.chunks.len(),
        raw_bytes_len,
        compressed_bytes_len,
    );

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
        shard_manager.add_cas_block(cas_info).await?;

        cas.put(
            cas_prefix,
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

        shard_manager.add_file_reconstruction_info(fi).await?;
    }

    FILTER_CAS_BYTES_PRODUCED.inc_by(compressed_bytes_len as u64);

    cas_data.data.clear();
    cas_data.chunks.clear();
    cas_data.pending_file_info.clear();

    Ok(cas_hash)
}

/// Smudge operations
impl PointerFileTranslatorV3 {
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
            Err(DataProcessingError::HashNotFound)
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
                    return Err(DataProcessingError::ParameterError(msg));
                }
                slice_object_range(&blocks, start, end - start)
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

    async fn data_from_chunks_to_writer(
        &self,
        chunks: Vec<ObjectRange>,
        writer: &mut impl std::io::Write,
    ) -> Result<()> {
        let mut bytes_smudged: u64 = 0;
        let mut strm = iter(chunks.into_iter().map(|objr| {
            let prefix = self.config.cas_storage_config.prefix.clone();
            get_from_cas(
                &self.cas,
                prefix,
                objr.hash,
                (objr.start as u64, objr.end as u64),
            )
        }))
        .buffered(*MAX_CONCURRENT_DOWNLOADS);

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
}

/// Smudge operation helpers

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

// Helpers for old PFT compatibility
impl PointerFileTranslatorV3 {
    pub async fn from_config(
        config: &XetConfig,
        repo_salt: RepoSalt,
    ) -> Result<PointerFileTranslatorV3> {
        let translator_config = translator_config_from(config, Some(repo_salt)).await?;
        let mut pft = PointerFileTranslatorV3::new(translator_config).await?;
        pft.xet = config.clone();

        Ok(pft)
    }

    pub async fn from_config_smudge_only(config: &XetConfig) -> Result<PointerFileTranslatorV3> {
        let translator_config = translator_config_from(config, None).await?;
        let mut pft = PointerFileTranslatorV3::new(translator_config).await?;
        pft.xet = config.clone();

        Ok(pft)
    }

    pub async fn new_temporary(temp_dir: &Path) -> Result<PointerFileTranslatorV3> {
        let translator_config = TranslatorConfig {
            file_query_policy: FileQueryPolicy::LocalOnly,
            cas_storage_config: StorageConfig {
                endpoint: Endpoint::FileSystem(temp_dir.join("cas")),
                auth: Auth {
                    user_id: "".into(),
                    login_id: "".into(),
                },
                prefix: "".into(),
                cache_config: None,
                staging_directory: Some(temp_dir.into()),
            },
            shard_storage_config: StorageConfig {
                endpoint: Endpoint::FileSystem(temp_dir.join("cas")),
                auth: Auth {
                    user_id: "".into(),
                    login_id: "".into(),
                },
                prefix: "-merkledb".into(),
                cache_config: Some(CacheConfig {
                    cache_directory: temp_dir.into(),
                    cache_size: 0,
                    cache_blocksize: 0,
                }),
                staging_directory: Some(temp_dir.into()),
            },
            dedup_config: Some(DedupConfig {
                repo_salt: None,
                small_file_threshold: SMALL_FILE_THRESHOLD,
                global_dedup_policy: GlobalDedupPolicy::Never,
            }),
            repo_info: None,
            smudge_config: Default::default(),
        };

        PointerFileTranslatorV3::new(translator_config).await
    }

    pub fn set_enable_global_dedup_queries(&mut self, enable: bool) {
        if let Some(dedup_config) = &mut self.config.dedup_config {
            dedup_config.global_dedup_policy = match enable {
                true => GlobalDedupPolicy::Always,
                false => GlobalDedupPolicy::Never,
            }
        }
    }

    pub async fn refresh(&self) -> Result<()> {
        let shard_config = &self.config.shard_storage_config;
        if let Some(ref cache) = shard_config.cache_config {
            self.shard_manager
                .register_shards_by_path(&[&cache.cache_directory], true)
                .await?;
        }
        if let Some(ref staging) = shard_config.staging_directory {
            self.shard_manager
                .register_shards_by_path(&[staging], false)
                .await?;
        }

        Ok(())
    }

    pub fn get_cas(&self) -> Arc<dyn Staging + Send + Sync> {
        self.cas.clone()
    }

    pub fn get_prefix(&self) -> String {
        self.config.cas_storage_config.prefix.clone()
    }

    pub fn get_summarydb(&self) -> Arc<Mutex<WholeRepoSummary>> {
        Default::default()
    }

    pub async fn upload_cas_staged(&self, retain: bool) -> Result<()> {
        self.cas
            .upload_all_staged(*MAX_CONCURRENT_UPLOADS, retain)
            .await?;

        Ok(())
    }

    pub fn print_stats(&self) {
        // Noop
    }

    pub async fn clean_file(
        &self,
        path: &Path,
        reader: impl AsyncDataIterator + 'static,
    ) -> Result<Vec<u8>> {
        self.clean_file_and_report_progress(path, reader, &None)
            .await
    }

    pub async fn clean_file_and_report_progress(
        &self,
        path: &Path,
        mut reader: impl AsyncDataIterator + 'static,
        _: &Option<Arc<DataProgressReporter>>,
    ) -> Result<Vec<u8>> {
        let cleaner = self.start_clean(4096, Some(path)).await?;

        while let Some(data) = reader
            .next()
            .await
            .map_err(|e| DataProcessingError::InternalError(format!("{e:?}")))?
        {
            cleaner.add_bytes(data).await?;
        }

        cleaner.result().await.map(|pf| pf.as_bytes().to_vec())
    }

    pub async fn smudge_file(
        &self,
        _path: &Path,
        mut _reader: impl AsyncDataIterator,
        _writer: &mut impl std::io::Write,
        _passthrough: bool,
        _range: Option<(usize, usize)>,
    ) -> Result<()> {
        // Noop
        Ok(())
    }

    pub async fn smudge_file_to_mpsc(
        &self,
        path: &Path,
        mut reader: impl AsyncDataIterator,
        writer: &Sender<crate::errors::Result<Vec<u8>>>,
        ready: &Option<watch::Sender<bool>>,
        progress_indicator: &Option<Arc<DataProgressReporter>>,
    ) -> usize {
        info!("Smudging file {:?}", &path);
        let print_err = |e| {
            error!("Unable to send smudge error {e:?} as channel has closed");
            e
        };

        let (fi, data) = match pointer_file_from_reader(
            path,
            &mut reader,
            self.config.smudge_config.force_no_smudge,
        )
        .await
        {
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
                            let _ = writer.send(Err(e)).await.map_err(print_err);
                            return 0;
                        }
                    };
                }
                0
            }
        }
    }

    /// This function does not return, but any results are sent
    /// through the mpsc channel
    pub async fn smudge_file_from_pointer_to_mpsc(
        &self,
        path: &Path,
        pointer: &PointerFile,
        writer: &Sender<crate::errors::Result<Vec<u8>>>,
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
                if let Err(e) = writer.send(Err(e.into())).await {
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

    async fn data_from_chunks_to_mpsc(
        &self,
        chunks: Vec<ObjectRange>,
        writer: &Sender<crate::errors::Result<Vec<u8>>>,
        ready: &Option<watch::Sender<bool>>,
        progress_indicator: &Option<Arc<DataProgressReporter>>,
    ) -> crate::errors::Result<usize> {
        let mut cas_bytes_retrieved = 0;

        let mut strm = iter(chunks.into_iter().map(|objr| {
            let prefix = self.config.cas_storage_config.prefix.clone();
            get_from_cas(
                &self.cas,
                prefix,
                objr.hash,
                (objr.start as u64, objr.end as u64),
            )
        }))
        .buffered(*MAX_CONCURRENT_DOWNLOADS);
        let mut is_first = true;
        while let Some(buf) = strm.next().await {
            let buf = buf?;
            let buf_len = buf.len();
            cas_bytes_retrieved += buf.len();
            writer.send(Ok(buf)).await.map_err(|_| {
                DataProcessingError::InternalError(
                    "Unable to send smudge result as channel has closed".into(),
                )
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

    pub async fn finalize(&self) -> Result<()> {
        self.finalize_cleaning().await?;
        Ok(())
    }

    pub async fn prefetch(&self, _pointer: &PointerFile, _start: u64) -> Result<bool> {
        // Noop
        Ok(false)
    }

    pub fn repo_salt(&self) -> Result<RepoSalt> {
        Ok(self
            .config
            .dedup_config
            .as_ref()
            .and_then(|dedup| dedup.repo_salt)
            .unwrap_or_default())
    }

    pub fn get_shard_manager(&self) -> Arc<ShardFileManager> {
        self.shard_manager.clone()
    }

    pub fn get_config(&self) -> XetConfig {
        self.xet.clone()
    }
}
