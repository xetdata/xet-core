use super::cas_interface::create_cas_client;
use super::data_processing_v1::PointerFileTranslatorV1;
use super::data_processing_v2::PointerFileTranslatorV2;
use super::mdb::get_mdb_version;
use super::mini_smudger::MiniPointerFileSmudger;
use super::PointerFile;
use crate::config::XetConfig;
use crate::errors::Result;
use crate::git_integration::git_repo_salt::{read_repo_salt_by_dir, RepoSalt};
use crate::stream::data_iterators::AsyncDataIterator;
use crate::summaries::WholeRepoSummary;
use cas_client::Staging;
use mdb_shard::shard_version::ShardVersion;
use merkledb::ObjectRange;
use merklehash::MerkleHash;
use progress_reporting::DataProgressReporter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tracing::info;

use lazy_static::lazy_static;
use prometheus::{register_int_counter, IntCounter};

// Some of the common tracking things
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
}

pub enum PFTRouter {
    V1(PointerFileTranslatorV1),
    V2(PointerFileTranslatorV2),
}

pub struct PointerFileTranslator {
    pub pft: PFTRouter,
}

impl PointerFileTranslator {
    pub async fn v1_from_config(config: &XetConfig) -> Result<Self> {
        Ok(Self {
            pft: PFTRouter::V1(PointerFileTranslatorV1::from_config(config).await?),
        })
    }

    pub async fn v2_from_config(config: &XetConfig, repo_salt: RepoSalt) -> Result<Self> {
        Ok(Self {
            pft: PFTRouter::V2(PointerFileTranslatorV2::from_config(config, repo_salt).await?),
        })
    }

    pub async fn v2_from_config_smudge_only(config: &XetConfig) -> Result<Self> {
        Ok(Self {
            pft: PFTRouter::V2(PointerFileTranslatorV2::from_config_smudge_only(config).await?),
        })
    }

    pub async fn from_config_in_repo(config: &XetConfig) -> Result<Self> {
        let version = get_mdb_version(config.repo_path()?, config)?;

        match version {
            ShardVersion::V1 => Ok(Self {
                pft: PFTRouter::V1(PointerFileTranslatorV1::from_config(config).await?),
            }),
            ShardVersion::V2 => {
                let maybe_salt = read_repo_salt_by_dir(config.repo_path()?, config)?;

                if let Some(salt) = maybe_salt {
                    Ok(Self {
                        pft: PFTRouter::V2(
                            PointerFileTranslatorV2::from_config(config, salt).await?,
                        ),
                    })
                } else {
                    info!("Note: Repository salt unavailable; PointerFileTranslator created in smudge-only mode.");
                    Ok(Self {
                        pft: PFTRouter::V2(
                            PointerFileTranslatorV2::from_config_smudge_only(config).await?,
                        ),
                    })
                }
            }
            ShardVersion::Uninitialized => {
                info!("Note: Repository uninitialized; PointerFileTranslator created in smudge-only mode.");
                Ok(Self {
                    pft: PFTRouter::V2(
                        PointerFileTranslatorV2::from_config_smudge_only(config).await?,
                    ),
                })
            }
        }
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

    pub fn set_enable_global_dedup_queries(&mut self, enable: bool) {
        match &mut self.pft {
            PFTRouter::V2(ref mut p) => p.set_enable_global_dedup_queries(enable),
            _ => {}
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
        reader: impl AsyncDataIterator + 'static,
    ) -> Result<Vec<u8>> {
        match &self.pft {
            PFTRouter::V1(ref p) => p.clean_file(path, reader).await,
            PFTRouter::V2(ref p) => p.clean_file(path, reader).await,
        }
    }

    pub async fn clean_file_and_report_progress(
        &self,
        path: &Path,
        reader: impl AsyncDataIterator + 'static,
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
        reader: impl AsyncDataIterator,
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
        reader: impl AsyncDataIterator,
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

    /// Returns the repo salt, if set.
    pub fn repo_salt(&self) -> Result<RepoSalt> {
        match &self.pft {
            PFTRouter::V1(ref _p) => {
                Err(anyhow::anyhow!(
                    "Internal Error: Repo salt requested when repo uses Merkle DB V1."
                ))?;
                unreachable!();
            }
            PFTRouter::V2(ref p) => Ok(p.repo_salt()?),
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
