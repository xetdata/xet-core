use crate::config::XetConfig;
use crate::data_processing_v1::PointerFileTranslatorV1;
pub use crate::data_processing_v1::{
    FILTER_BYTES_CLEANED, FILTER_BYTES_SMUDGED, FILTER_CAS_BYTES_PRODUCED, GIT_XET_VERION,
};
use crate::data_processing_v2::PointerFileTranslatorV2;
use crate::errors::Result;
use crate::git_integration::git_repo;
use cas_client::Staging;
use mdb_shard::shard_version::ShardVersion;
use merkledb::AsyncIterator;
use pointer_file::PointerFile;
use progress_reporting::DataProgressReporter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::sync::Mutex;

enum PFTRouter {
    V1(PointerFileTranslatorV1),
    V2(PointerFileTranslatorV2),
}
pub struct PointerFileTranslator {
    pft: PFTRouter,
}

impl PointerFileTranslator {
    pub async fn from_config(config: &XetConfig) -> Result<Self> {
        let version = git_repo::get_mdb_version(config.repo_path()?)?;
        match version {
            ShardVersion::V1 => Ok(Self {
                pft: PFTRouter::V1(PointerFileTranslatorV1::from_config(config).await?),
            }),
            ShardVersion::V2 => Ok(Self {
                pft: PFTRouter::V2(PointerFileTranslatorV2::from_config(config).await?),
            }),
        }
    }

    #[cfg(test)] // Only for testing.
    pub async fn new_temporary(temp_dir: &Path, version: ShardVersion) -> Result<Self> {
        match version {
            ShardVersion::V1 => Ok(Self {
                pft: PFTRouter::V1(PointerFileTranslatorV1::new_temporary(temp_dir)),
            }),
            ShardVersion::V2 => Ok(Self {
                pft: PFTRouter::V2(PointerFileTranslatorV2::new_temporary(temp_dir).await?),
            }),
        }
    }

    pub fn get_cas(&self) -> Arc<Box<dyn Staging + Send + Sync>> {
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

    pub async fn upload_cas_staged(&self, retain: bool) -> Result<()> {
        match &self.pft {
            PFTRouter::V1(ref p) => p.upload_cas_staged(retain).await,
            PFTRouter::V2(ref p) => p.upload_cas_staged(retain).await,
        }
    }

    pub async fn finalize_cleaning(&mut self) -> Result<()> {
        match &mut self.pft {
            PFTRouter::V1(ref mut p) => p.finalize_cleaning().await,
            PFTRouter::V2(ref mut p) => p.finalize_cleaning().await,
        }
    }

    pub fn print_stats(&self) {
        match &self.pft {
            PFTRouter::V1(ref p) => p.print_stats(),
            PFTRouter::V2(ref p) => p.print_stats(),
        }
    }

    pub async fn clean_file_and_report_progress(
        &self,
        path: &Path,
        reader: impl AsyncIterator + Send + Sync,
        progress_indicator: &Option<Arc<Mutex<(bool, DataProgressReporter)>>>,
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
        progress_indicator: &Option<Arc<Mutex<(bool, DataProgressReporter)>>>,
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
        path: &PathBuf,
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
    pub async fn finalize(&mut self) -> Result<()> {
        match &mut self.pft {
            PFTRouter::V1(ref mut p) => p.finalize().await,
            PFTRouter::V2(ref mut p) => p.finalize().await,
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
}
