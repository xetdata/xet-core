use crate::config::XetConfig;
use crate::constants::{LOCAL_CAS_SCHEME, MAX_CONCURRENT_DOWNLOADS, POINTER_FILE_LIMIT};
use crate::data_processing_v1::PointerFileTranslatorV1;
pub use crate::data_processing_v1::{
    FILTER_BYTES_CLEANED, FILTER_BYTES_SMUDGED, FILTER_CAS_BYTES_PRODUCED, GIT_XET_VERION,
};
use crate::data_processing_v2::PointerFileTranslatorV2;
use crate::errors::{GitXetRepoError, Result};
use crate::git_integration::GitXetRepo;
use crate::merkledb_shard_plumb::get_mdb_version;
use crate::summaries_plumb::WholeRepoSummary;
use cas_client::{
    new_staging_client, new_staging_client_with_progressbar, CachingClient, LocalClient,
    RemoteClient, Staging,
};
use futures::prelude::stream::*;
use mdb_shard::shard_version::ShardVersion;
use merkledb::{AsyncIterator, ObjectRange};
use merklehash::MerkleHash;
use pointer_file::PointerFile;
use progress_reporting::DataProgressReporter;
use std::env::current_dir;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tracing::{error, info, info_span};

pub async fn create_cas_client(config: &XetConfig) -> Result<Arc<dyn Staging + Send + Sync>> {
    info!(
        "CAS staging directory located at: {:?}.",
        &config.staging_path
    );

    let endpoint = &config.cas.endpoint;
    let (user_id, _) = &config.user.get_user_id();
    let auth = &config.user.get_login_id();
    let repo_paths = GitXetRepo::get_remote_urls(config.repo_path().ok().map(|x| x.as_path()))
        .unwrap_or_else(|_| vec!["".to_string()]);

    if let Some(fs_path) = endpoint.strip_prefix(LOCAL_CAS_SCHEME) {
        info!("Using local CAS with path: {:?}.", endpoint);
        let mut path = PathBuf::from_str(fs_path)
            .map_err(|_| GitXetRepoError::InvalidLocalCasPath(fs_path.to_string()))?;
        if !path.is_absolute() {
            path = current_dir()?.join(path);
        }
        let client = LocalClient::new(&path, false);
        Ok(new_staging_client_with_progressbar(
            client,
            config.staging_path.as_deref(),
        ))
    } else if config.cache.enabled {
        let cacheclient_result = CachingClient::new(
            RemoteClient::from_config(
                endpoint,
                user_id,
                auth,
                repo_paths.clone(),
                GIT_XET_VERION.clone(),
            )
            .await,
            &config.cache.path,
            config.cache.size,
            config.cache.blocksize,
        );
        match cacheclient_result {
            Ok(cacheclient) => {
                info!(
                    "Using Caching CAS with endpoint {:?}, prefix {:?}, caching at {:?}.",
                    &endpoint, &config.cas.prefix, &config.cache.path
                );
                Ok(new_staging_client_with_progressbar(
                    cacheclient,
                    config.staging_path.as_deref(),
                ))
            }
            Err(e) => {
                error!(
                    "Unable to use caching CAS due to: {:?}; Falling back to non-caching CAS with endpoint: {:?}.",
                    &e, &endpoint
                );
                let remote_client = RemoteClient::from_config(
                    endpoint,
                    user_id,
                    auth,
                    repo_paths.clone(),
                    GIT_XET_VERION.clone(),
                )
                .await;
                Ok(new_staging_client_with_progressbar(
                    remote_client,
                    config.staging_path.as_deref(),
                ))
            }
        }
    } else {
        info!("Using non-caching CAS with endpoint: {:?}.", &endpoint);
        let remote_client = RemoteClient::from_config(
            endpoint,
            user_id,
            auth,
            repo_paths.clone(),
            GIT_XET_VERION.clone(),
        )
        .await;
        Ok(new_staging_client(
            remote_client,
            config.staging_path.as_deref(),
        ))
    }
}

/**  Wrapper to consolidate the logic for retrieving from CAS.   
 */
pub async fn get_from_cas(
    cas: &Arc<dyn Staging + Send + Sync>,
    prefix: String,
    hash: MerkleHash,
    ranges: (u64, u64),
) -> Result<Vec<u8>> {
    if ranges.0 == ranges.1 {
        return Ok(Vec::new());
    }
    let mut query_result = cas
        .get_object_range(&prefix, &hash, vec![ranges])
        .await
        .map_err(|e| GitXetRepoError::Other(format!("Error fetching Xorb {hash:?}: {e:?}.")))?;
    Ok(std::mem::take(&mut query_result[0]))
}

/// Given an Vec<ObjectRange> describing a series of range of bytes,
/// slice a subrange. This does not check limits and may return shorter
/// results if the slice goes past the end of the range.
pub fn slice_object_range(v: &[ObjectRange], mut start: usize, mut len: usize) -> Vec<ObjectRange> {
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

/// Writes a collection of chunks from a Vec<ObjectRange> to a writer.
pub async fn data_from_chunks_to_writer(
    cas: &Arc<dyn Staging + Send + Sync>,
    prefix: String,
    chunks: Vec<ObjectRange>,
    writer: &mut impl std::io::Write,
) -> Result<()> {
    let mut bytes_smudged: u64 = 0;
    let mut strm = iter(chunks.into_iter().map(|objr| {
        let prefix = prefix.clone();
        get_from_cas(cas, prefix, objr.hash, (objr.start as u64, objr.end as u64))
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

/// Tries to parse a pointer file from the reader, but if parsing fails, returns
/// the data we have pulled out from the reader. (The AsyncIterator does not
/// provide a "put-back" function and for simplicity it probably shouldn't
/// have that).
///
/// if a pointer file is parsed successfully Returns `Ok(Some(PointerFile), data)`
/// if a pointer file is parsed unsuccessfully, Returns `Ok(None, data)`
/// if the read stream failed for reasons which are not EOF, returns `Err(e)`
pub async fn pointer_file_from_reader(
    path: &Path,
    reader: &mut impl AsyncIterator,
    force_no_smudge: bool,
) -> Result<(Option<PointerFile>, Vec<u8>)> {
    let mut data: Vec<u8> = Vec::new();

    while data.len() < POINTER_FILE_LIMIT {
        match reader.next().await? {
            Some(mut new_data) => {
                data.append(&mut new_data);
            }
            None => {
                break;
            }
        };
    }

    if force_no_smudge || data.len() > POINTER_FILE_LIMIT {
        // too large.
        return Ok((None, data));
    }

    let file_str: &str = match std::str::from_utf8(&data[..]) {
        Ok(v) => v,
        Err(_) => return Ok((None, data)), // can't utf-8. definitely a bad pointer file
    };

    let ptr_file = PointerFile::init_from_string(file_str, path.to_str().unwrap());
    if ptr_file.is_valid() {
        Ok((Some(ptr_file), data))
    } else {
        // pointer file did not parse correctly
        Ok((None, data))
    }
}

/// Manages the smudigng of a single set of blocks, that does not
/// require a full copy of the MerkleDB to hang around.
#[derive(Clone)]
pub struct MiniPointerFileSmudger {
    cas: Arc<dyn Staging + Send + Sync>,
    prefix: String,
    blocks: Vec<ObjectRange>,
}

impl MiniPointerFileSmudger {
    fn derive_ranged_blocks(&self, range: Option<(usize, usize)>) -> Result<Vec<ObjectRange>> {
        match range {
            Some((start, end)) => {
                // we expect callers to validate the range, but just in case, check it anyway.
                if end < start {
                    let msg = format!(
                        "End range value requested ({end}) is less than start range value ({start})"
                    );
                    error!(msg);
                    return Err(GitXetRepoError::Other(msg));
                }
                Ok(slice_object_range(&self.blocks, start, end - start))
            }
            None => Ok(self.blocks.clone()),
        }
    }

    pub async fn smudge_to_writer(
        &self,
        writer: &mut impl std::io::Write,
        range: Option<(usize, usize)>,
    ) -> Result<()> {
        let ranged_blocks = self.derive_ranged_blocks(range)?;
        data_from_chunks_to_writer(&self.cas, self.prefix.clone(), ranged_blocks, writer).await?;
        Ok(())
    }
}

pub enum PFTRouter {
    V1(PointerFileTranslatorV1),
    V2(PointerFileTranslatorV2),
}

pub struct PointerFileTranslator {
    pub pft: PFTRouter,
}

impl PointerFileTranslator {
    pub async fn from_config(config: &XetConfig) -> Result<Self> {
        let version = get_mdb_version(
            config
                .repo_path_if_present
                .as_ref()
                .unwrap_or(&PathBuf::default()),
        )?;

        match version {
            ShardVersion::V1 => Ok(Self {
                pft: PFTRouter::V1(PointerFileTranslatorV1::from_config(config).await?),
            }),
            ShardVersion::V2 | ShardVersion::Uninitialized => Ok(Self {
                pft: PFTRouter::V2(PointerFileTranslatorV2::from_config(config).await?),
            }),
        }
    }

    pub async fn from_config_and_repo_salt(config: &XetConfig, repo_salt: &[u8]) -> Result<Self> {
        let mut pftv2 = PointerFileTranslatorV2::from_config(config).await?;
        pftv2.set_repo_salt(repo_salt);

        Ok(Self {
            pft: PFTRouter::V2(pftv2),
        })
    }

    #[cfg(test)] // Only for testing.
    pub async fn new_temporary(temp_dir: &Path, version: ShardVersion) -> Result<Self> {
        match version {
            ShardVersion::V1 => Ok(Self {
                pft: PFTRouter::V1(PointerFileTranslatorV1::new_temporary(temp_dir)),
            }),
            ShardVersion::Uninitialized | ShardVersion::V2 => Ok(Self {
                pft: PFTRouter::V2(PointerFileTranslatorV2::new_temporary(temp_dir).await?),
            }),
        }
    }

    pub async fn refresh(&self) -> Result<()> {
        match &self.pft {
            PFTRouter::V1(ref p) => p.refresh().await,
            PFTRouter::V2(ref p) => p.refresh().await,
        }
    }

    pub fn mdb_version(&self) -> ShardVersion {
        match self.pft {
            PFTRouter::V1(_) => ShardVersion::V1,
            PFTRouter::V2(_) => ShardVersion::V2,
        }
    }

    pub fn get_cas(&self) -> Arc<dyn Staging + Send + Sync> {
        match &self.pft {
            PFTRouter::V1(ref p) => p.get_cas(),
            PFTRouter::V2(ref p) => p.get_cas(),
        }
    }

    pub fn get_prefix(&self) -> String {
        match &self.pft {
            PFTRouter::V1(ref p) => p.get_prefix(),
            PFTRouter::V2(ref p) => p.get_prefix(),
        }
    }

    pub fn get_summarydb(&self) -> Arc<Mutex<WholeRepoSummary>> {
        match &self.pft {
            PFTRouter::V1(ref p) => p.get_summarydb(),
            PFTRouter::V2(ref p) => p.get_summarydb(),
        }
    }

    pub async fn upload_cas_staged(&self, retain: bool) -> Result<()> {
        match &self.pft {
            PFTRouter::V1(ref p) => p.upload_cas_staged(retain).await,
            PFTRouter::V2(ref p) => p.upload_cas_staged(retain).await,
        }
    }

    pub async fn finalize_cleaning(&self) -> Result<()> {
        match &self.pft {
            PFTRouter::V1(ref p) => p.finalize_cleaning().await,
            PFTRouter::V2(ref p) => p.finalize_cleaning().await,
        }
    }

    pub fn print_stats(&self) {
        match &self.pft {
            PFTRouter::V1(ref p) => p.print_stats(),
            PFTRouter::V2(ref p) => p.print_stats(),
        }
    }
    pub async fn clean_file(
        &self,
        path: &Path,
        reader: impl AsyncIterator + Send + Sync,
    ) -> Result<Vec<u8>> {
        match &self.pft {
            PFTRouter::V1(ref p) => p.clean_file(path, reader).await,
            PFTRouter::V2(ref p) => p.clean_file(path, reader).await,
        }
    }

    pub async fn clean_file_and_report_progress(
        &self,
        path: &Path,
        reader: impl AsyncIterator + Send + Sync,
        progress_indicator: &Option<Arc<DataProgressReporter>>,
    ) -> Result<Vec<u8>> {
        match &self.pft {
            PFTRouter::V1(ref p) => {
                p.clean_file_and_report_progress(path, reader, progress_indicator)
                    .await
            }
            PFTRouter::V2(ref p) => {
                p.clean_file_and_report_progress(path, reader, progress_indicator)
                    .await
            }
        }
    }

    /// Queries merkle db for construction info for a pointer file.
    pub async fn derive_blocks(&self, hash: &MerkleHash) -> Result<Vec<ObjectRange>> {
        match &self.pft {
            PFTRouter::V1(ref p) => p.derive_blocks(hash).await,
            PFTRouter::V2(ref p) => p.derive_blocks(hash).await,
        }
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
        reader: impl AsyncIterator,
        writer: &mut impl std::io::Write,
        passthrough: bool,
        range: Option<(usize, usize)>,
    ) -> Result<()> {
        match &self.pft {
            PFTRouter::V1(ref p) => {
                p.smudge_file(path, reader, writer, passthrough, range)
                    .await
            }
            PFTRouter::V2(ref p) => {
                p.smudge_file(path, reader, writer, passthrough, range)
                    .await
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
        reader: impl AsyncIterator,
        writer: &Sender<Result<Vec<u8>>>,
        ready: &Option<watch::Sender<bool>>,
        progress_indicator: &Option<Arc<DataProgressReporter>>,
    ) -> usize {
        match &self.pft {
            PFTRouter::V1(ref p) => {
                p.smudge_file_to_mpsc(path, reader, writer, ready, progress_indicator)
                    .await
            }
            PFTRouter::V2(ref p) => {
                p.smudge_file_to_mpsc(path, reader, writer, ready, progress_indicator)
                    .await
            }
        }
    }

    pub async fn smudge_file_from_pointer(
        &self,
        path: &Path,
        pointer: &PointerFile,
        writer: &mut impl std::io::Write,
        range: Option<(usize, usize)>,
    ) -> Result<()> {
        match &self.pft {
            PFTRouter::V1(ref p) => {
                p.smudge_file_from_pointer(path, pointer, writer, range)
                    .await
            }
            PFTRouter::V2(ref p) => {
                p.smudge_file_from_pointer(path, pointer, writer, range)
                    .await
            }
        }
    }

    pub async fn smudge_file_from_hash(
        &self,
        path: Option<PathBuf>,
        file_id: &MerkleHash,
        writer: &mut impl std::io::Write,
        range: Option<(usize, usize)>,
    ) -> Result<()> {
        match &self.pft {
            PFTRouter::V1(ref p) => p.smudge_file_from_hash(path, file_id, writer, range).await,
            PFTRouter::V2(ref p) => p.smudge_file_from_hash(path, file_id, writer, range).await,
        }
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
        match &self.pft {
            PFTRouter::V1(ref p) => {
                p.smudge_file_from_pointer_to_mpsc(path, pointer, writer, ready, progress_indicator)
                    .await
            }
            PFTRouter::V2(ref p) => {
                p.smudge_file_from_pointer_to_mpsc(path, pointer, writer, ready, progress_indicator)
                    .await
            }
        }
    }

    /// To be called at the end of a batch of clean/smudge operations.
    /// Commits all MerkleDB changes to disk.
    pub async fn finalize(&self) -> Result<()> {
        match &self.pft {
            PFTRouter::V1(ref p) => p.finalize().await,
            PFTRouter::V2(ref p) => p.finalize().await,
        }
    }

    /// Performs a prefetch heuristic assuming that the user will be reading at
    /// the provided start position,
    ///
    /// The file is cut into chunks of PREFETCH_WINDOW_SIZE_MB.
    /// The prefetch will start a task to download the chunk which contains
    /// the byte X.
    ///
    /// Returns true if a prefetch was started, and false otherwise
    pub async fn prefetch(&self, pointer: &PointerFile, start: u64) -> Result<bool> {
        match &self.pft {
            PFTRouter::V1(ref p) => p.prefetch(pointer, start).await,
            PFTRouter::V2(ref p) => p.prefetch(pointer, start).await,
        }
    }

    /// Reload the MerkleDB from disk to memory.
    pub async fn reload_mdb(&self) {
        if let PFTRouter::V1(ref p) = &self.pft {
            p.reload_mdb().await
        }
    }

    /// Create a mini smudger that handles only the pointer file concerned
    /// without taking a full copy of the MerkleDB
    pub async fn make_mini_smudger(
        &self,
        path: &PathBuf,
        blocks: Vec<ObjectRange>,
        disable_cache: Option<bool>,
    ) -> Result<MiniPointerFileSmudger> {
        info!("Mini Smudging file {:?}", &path);

        let cas = if let Some(disable_cache) = disable_cache {
            let mut current_config = match &self.pft {
                PFTRouter::V1(ref p) => p.get_config(),
                PFTRouter::V2(ref p) => p.get_config(),
            };

            current_config.cache.enabled = !disable_cache;
            create_cas_client(&current_config).await?
        } else {
            match &self.pft {
                PFTRouter::V1(ref p) => p.get_cas(),
                PFTRouter::V2(ref p) => p.get_cas(),
            }
        };

        match &self.pft {
            PFTRouter::V1(ref p) => Ok(MiniPointerFileSmudger {
                cas,
                prefix: p.get_prefix(),
                blocks,
            }),
            PFTRouter::V2(ref p) => Ok(MiniPointerFileSmudger {
                cas,
                prefix: p.get_prefix(),
                blocks,
            }),
        }
    }
}
