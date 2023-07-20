use std::path::Path;

use anyhow::anyhow;
use git2::{ErrorCode, Repository};

use libmagic::libmagic::{LibmagicSummary, summarize_libmagic};
use pointer_file::PointerFile;

use crate::config::XetConfig;
use crate::constants::{GIT_NOTES_SUMMARIES_REF_NAME, POINTER_FILE_LIMIT, SMALL_FILE_THRESHOLD};
use crate::diff::error::DiffError;
use crate::diff::error::DiffError::{FailedSummaryCalculation, NoSummaries, NotInRepoDir};
use crate::diff::util::RefOrT;
use crate::git_integration::git_repo::GitRepo;
use crate::log::ErrorPrinter;
use crate::summaries::analysis::FileSummary;
use crate::summaries::csv::summarize_csv_from_reader;
use crate::summaries::summary_type::SummaryType;
use crate::summaries_plumb::WholeRepoSummary;

/// Fetches FileSummaries for hashes or blob_ids.
///
/// TODO: db and repo should probably be lazily constructed.
pub struct SummaryFetcher {
    // For reading from hashes
    db: WholeRepoSummary,
    // For computing from blobs
    repo: Repository,
}

impl SummaryFetcher {
    pub async fn new(config: XetConfig) -> Result<Self, DiffError> {
        Ok(Self {
            db: Self::load_db(&config).await?,
            repo: Self::load_repo(config)?,
        })
    }

    async fn load_db(config: &XetConfig) -> Result<WholeRepoSummary, DiffError> {
        WholeRepoSummary::load_or_recreate_from_git(
            config,
            &config.summarydb,
            GIT_NOTES_SUMMARIES_REF_NAME,
        )
        .await
        .log_error("Error loading git notes")
        .map_err(|_| NoSummaries)
    }

    fn load_repo(config: XetConfig) -> Result<Repository, DiffError> {
        Ok(GitRepo::open(config)
            .log_error("Error opening git repo")
            .map_err(|_| NotInRepoDir)?
            .repo)
    }

    /// Gets the summary for the given hash or blob_id. This will prioritize getting the summary
    /// from the hash and if one cannot be found, then the summary will be computed from the
    /// contents of the indicated blob.
    ///
    /// This returns a [RefOrT] instead of a [std::Cow] since it is valid to have no summary be
    /// retrieved (in which case, [RefOrT::get()] will return None). Unfortunately, an
    /// Option<Cow> doesn't work without propagating the Cow throughout any
    /// downstream calls (instead of just using the reference).
    pub fn get_summary(
        &self,
        file_path: &str,
        hash: Option<&String>,
        blob_id: Option<&String>,
    ) -> Result<RefOrT<FileSummary>, DiffError> {
        if let Some(summary) = self.hash_to_summary(hash) {
            return Ok(summary);
        }
        self.blob_to_summary(file_path, blob_id)
    }

    fn hash_to_summary(&self, hash: Option<&String>) -> Option<RefOrT<FileSummary>> {
        hash.and_then(|hash| self.db.get(hash)).map(RefOrT::from)
    }

    fn blob_to_summary(&self, file_path: &str, blob_id: Option<&String>) -> Result<RefOrT<FileSummary>, DiffError> {
        blob_id
            .map(|blob_id| {
                self.summary_from_blob(file_path, blob_id)
                    .log_error(format!("Error getting summary for blob_id: {blob_id}"))
            })
            .unwrap_or(Ok(RefOrT::None))
    }

    fn summary_from_blob(&self, file_path: &str, blob_id: &str) -> Result<RefOrT<FileSummary>, DiffError> {
        let result = self.get_blob(blob_id);
        let blob = match result {
            Ok(b) => b,
            Err(e) => {
                return match e {
                    NoSummaries => Ok(RefOrT::None),
                    _ => Err(e),
                }
            }
        };
        check_can_summarize(&blob)?;


        // file is either a pass-through or a pointer file.
        let content = blob.content();
        let summary = if let Some(pointer_file) = is_valid_pointer_file(content) {
            self.hash_to_summary(Some(pointer_file.hash()))
                .unwrap_or_default()
        } else {
            // file is a pass-through, calculate the summary:
            // use libmagic to get the filetype:
            let libmagic_summary = summarize_libmagic(Path::new(file_path))
                    .map_err(|e| FailedSummaryCalculation(anyhow!(e)))?;
            let summary_type = get_type_from_libmagic(&libmagic_summary);
            let mut summary = FileSummary::default();
            summary.libmagic = Some(libmagic_summary);
            // then use the summary_type to build the summary
            if summary_type == SummaryType::Csv {
                summary.csv = summarize_csv_from_reader(&mut &content[..])
                    .map_err(|e| FailedSummaryCalculation(anyhow!(e)))?
            }
            summary.into()
        };
        Ok(summary)
    }

    fn get_blob(&self, blob_id: &str) -> Result<git2::Blob<'_>, DiffError> {
        let oid = git2::Oid::from_str(blob_id).map_err(|e| FailedSummaryCalculation(anyhow!(e)))?;
        self.repo
            .find_blob(oid)
            .or_else(|e| match e.code() {
                ErrorCode::NotFound => {
                    let full_oid = self.repo.odb()?.exists_prefix(oid, blob_id.len())?;
                    self.repo.find_blob(full_oid)
                }
                _ => Err(e),
            })
            .map_err(|e| match e.code() {
                ErrorCode::NotFound => NoSummaries,
                _ => FailedSummaryCalculation(anyhow!(e)),
            })
    }
}

/// We reject summarizing any blobs larger than the SMALL_FILE_THRESHOLD as those should have
/// been cleaned into a pointer file and had their summaries computed and stored in the summary db.
fn check_can_summarize(blob: &git2::Blob) -> Result<(), DiffError> {
    let blob_size = blob.size();
    if blob_size > SMALL_FILE_THRESHOLD {
        Err(FailedSummaryCalculation(anyhow!(
            "file too large to summarize on-demand: {} bytes",
            blob_size
        )))
    } else {
        Ok(())
    }
}

fn is_valid_pointer_file(content: &[u8]) -> Option<PointerFile> {
    if content.len() <= POINTER_FILE_LIMIT {
        if let Ok(content_str) = std::str::from_utf8(content) {
            let pointer_file = PointerFile::init_from_string(content_str, "");
            if pointer_file.is_valid() {
                return Some(pointer_file);
            }
        }
    }
    None
}

fn get_type_from_libmagic(summary: &LibmagicSummary) -> SummaryType {
    let mime = &summary.file_type_mime;
    let mime_parts: Vec<&str> = mime.split(';').collect();
    match mime_parts[0] {
        "text/csv" => SummaryType::Csv,
        _ => SummaryType::Libmagic,
    }
}
