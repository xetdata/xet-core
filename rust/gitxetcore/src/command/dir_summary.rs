use crate::{
    data_processing::PointerFileTranslator, git_integration::git_file_tools::GitTreeListing,
};
use clap::Args;
use git2::{Blob, Oid};
use pointer_file::PointerFile;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
};
use tracing::{error, warn};

use crate::config::XetConfig;
use crate::{
    constants::{GIT_NOTES_SUMMARIES_REF_NAME, POINTER_FILE_LIMIT},
    errors::{self, GitXetRepoError},
    git_integration::git_repo::GitRepo,
    summaries::{
        analysis::FileSummary, libmagic::LibmagicSummary, libmagic::LIBMAGIC_ANALYZER_MAX_BYTES,
    },
    summaries_plumb::WholeRepoSummary,
};

use crate::summaries::libmagic::summarize_libmagic_from_reader;

#[derive(Args, Debug)]
pub struct DirSummaryArgs {
    /// A git commit reference to build directory summary statistics
    #[clap(default_value = "HEAD")]
    reference: String,

    /// If set, only libmagic file summary data will be used to fetch file info
    #[clap(long)]
    read_notes_only: bool,

    /// If set, do not read nor write the summary statistics in git notes
    #[clap(long)]
    no_cache: bool,

    /// If true, aggregate results so that each directory contains the results of all
    /// subdirectories as well.  Otherwise, the summary for a directory ignores
    /// subdirectories.  
    #[clap(long)]
    recursive: bool,
}

pub async fn dir_summary_command(config: XetConfig, args: &DirSummaryArgs) -> errors::Result<()> {
    let summarydb_path = config.summarydb.clone();
    let pointer_file_translator = PointerFileTranslator::from_config(&config).await?;
    let repo = GitRepo::open(config.clone())?;
    let gitrepo = &repo.repo;

    let notes_ref = if args.recursive {
        "refs/notes/xet/dir-summary-recursive"
    } else {
        "refs/notes/xet/dir-summary"
    };

    let oid = gitrepo
        .revparse_single(&args.reference)
        .map_err(|_| anyhow::anyhow!("Unable to resolve reference {}", args.reference))?
        .id();

    // if cached in git notes for the current commit, return that
    if let (false, Ok(note)) = (args.no_cache, gitrepo.find_note(Some(notes_ref), oid)) {
        tracing::info!("Fetching from note");
        let message = note.message().ok_or_else(|| {
            GitXetRepoError::Other("Failed to get message from git note".to_string())
        })?;
        println!("{message}");
    } else {
        tracing::info!("Recomputing");
        // recompute the dir summary
        let summaries = compute_dir_summaries(
            &config,
            &repo,
            &summarydb_path,
            &args.reference,
            args.read_notes_only,
            args.recursive,
            &pointer_file_translator,
        )
        .await?;

        let content_str = serde_json::to_string_pretty(&summaries).map_err(|_| {
            GitXetRepoError::Other("Failed to serialize dir summaries to JSON".to_string())
        })?;

        if !args.no_cache {
            let sig = gitrepo.signature()?;
            gitrepo.note(&sig, &sig, Some(notes_ref), oid, &content_str, false)?;
        }
        println!("{content_str}");
    }

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub enum SummaryType {
    // libmagic summaries
    FileTypeSimple,
}

// hash map from summary type to summary
// where a summary is {value: count, value2: count}
type SummaryInfo = HashMap<SummaryType, HashMap<String, i64>>;

// hash map from dir (as String) to summaries for that dir (non-recursive)
type DirSummaries = HashMap<String, SummaryInfo>;

async fn get_or_compute_file_summary(
    path: &str,
    blob: &Blob<'_>,
    repo_summary: &WholeRepoSummary,
    read_notes_only: bool,
    pointer_file_translator: &PointerFileTranslator,
) -> errors::Result<FileSummary> {
    let blob_size = blob.size();
    let mut ret = FileSummary::default();
    if blob_size <= POINTER_FILE_LIMIT {
        if let Ok(utf8_content) = std::str::from_utf8(blob.content()) {
            let pointer = PointerFile::init_from_string(utf8_content, "");
            if pointer.is_valid() {
                // if pointer file, use merklehash and stored summary (if any)
                if let Some(file_summary) = repo_summary.get(pointer.hash()) {
                    ret = file_summary.clone();
                }

                if ret.libmagic.is_none() {
                    // may or may not have successfully read a file summary, but we don't have libmagic populated.
                    if read_notes_only {
                        warn!("Notes not found for entry");
                        let empty_string = "".to_string();
                        let unknown_string = "Unknown".to_string();
                        ret.libmagic = Some(LibmagicSummary {
                            file_type: empty_string.clone(),
                            file_type_simple: unknown_string.clone(),
                            file_type_simple_category: unknown_string,
                            file_type_mime: empty_string,
                            buffer: None,
                        });
                    } else {
                        // Missing stored summary, but read_notes_only is not set so we can compute it on the fly.
                        let mut buf = Vec::<u8>::new();
                        let path = PathBuf::from(path);
                        pointer_file_translator
                            .smudge_file_from_pointer(
                                &path,
                                &pointer,
                                &mut buf,
                                Some((0, LIBMAGIC_ANALYZER_MAX_BYTES)),
                            )
                            .await?;
                        let mut slice = buf.as_slice();
                        let result = summarize_libmagic_from_reader(&mut slice)?;
                        ret.libmagic = Some(result);
                    }
                }
            } else {
                // non-pointer file (but valid utf-8), analyze it now
                let content = blob.content();
                ret.libmagic = Some(summarize_libmagic_from_reader(&mut &content[..])?);
            }
        } else {
            // non-pointer file (not utf-8), analyze it now
            let content = blob.content();
            ret.libmagic = Some(summarize_libmagic_from_reader(&mut &content[..])?);
        }
    } else {
        // non-pointer file (too large), analyze it now
        if let Some(mut content) = blob.content().chunks(LIBMAGIC_ANALYZER_MAX_BYTES).next() {
            ret.libmagic = Some(summarize_libmagic_from_reader(&mut content)?);
        }
    }
    Ok(ret)
}

pub async fn compute_dir_summaries(
    config: &XetConfig,
    repo: &GitRepo,
    summarydb_path: &Path,
    reference: &str,
    read_notes_only: bool,
    recursive: bool,
    translator: &PointerFileTranslator,
) -> errors::Result<DirSummaries> {
    let repo_summary = WholeRepoSummary::load_or_recreate_from_git(
        config,
        summarydb_path,
        GIT_NOTES_SUMMARIES_REF_NAME,
    )
    .await?;

    let tree_listing = GitTreeListing::build(&repo.repo_dir, Some(reference), true, true, true)?;

    let repo_ref = &repo.repo;
    let repo_summary_ref = &repo_summary;

    let mut dir_summary = DirSummaries::new();

    for blob_data in tree_listing.files {
        // For each file, get the blob information and decide what to do with it.

        let file_summary = match repo_ref.find_blob(Oid::from_str(&blob_data.object_id[..])?) {
            Err(e) => {
                error!(
                    "Error loading blob {} from repository, skipping {:?}",
                    blob_data.object_id, &e
                );
                FileSummary::default()
            }
            Ok(blob) => {
                get_or_compute_file_summary(
                    &blob_data.path,
                    &blob,
                    repo_summary_ref,
                    read_notes_only,
                    translator,
                )
                .await?
            }
        };

        // Now, go through and increase the counts for these file types in this directory.
        let entry_path = PathBuf::from_str(&blob_data.path).unwrap();
        let entry_dir = entry_path.parent().unwrap_or_else(|| Path::new(""));

        let summaries = dir_summary
            .entry(entry_dir.to_string_lossy().to_string())
            .or_default();

        if let Some(ref libmagic_summary) = file_summary.libmagic {
            let file_type_simple_summary =
                summaries.entry(SummaryType::FileTypeSimple).or_default();

            let current_file_type_simple_count = file_type_simple_summary
                .entry(libmagic_summary.file_type_simple.clone())
                .or_default();

            *current_file_type_simple_count += 1;
        }
    }

    if recursive {
        // Now, go through and create a new dir summary that has aggregated all the entries back up
        // to their parent directories.
        let mut aggregated_ds = DirSummaries::new();

        for (path, st_hashmap) in dir_summary.into_iter() {
            for (_st, file_count_hashmap) in st_hashmap.into_iter() {
                for (file_type, count) in file_count_hashmap.into_iter() {
                    let mut entry_dir = PathBuf::from_str(&path).unwrap();

                    loop {
                        let summaries = aggregated_ds
                            .entry(entry_dir.to_string_lossy().to_string())
                            .or_default();

                        let file_type_simple_summary =
                            summaries.entry(SummaryType::FileTypeSimple).or_default();

                        let current_file_type_simple_count = file_type_simple_summary
                            .entry(file_type.clone())
                            .or_default();

                        *current_file_type_simple_count += count;

                        if entry_dir == PathBuf::from_str("").unwrap() {
                            break;
                        } else {
                            entry_dir = entry_dir
                                .parent()
                                .unwrap_or_else(|| Path::new(""))
                                .to_path_buf();
                        }
                    }
                }
            }
        }
        Ok(aggregated_ds)
    } else {
        Ok(dir_summary)
    }
}
