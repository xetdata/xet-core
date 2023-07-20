use crate::git_integration::git_file_tools::GitTreeListing;
use clap::Args;

use libmagic::libmagic::summarize_libmagic;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
};

use crate::config::XetConfig;
use crate::{
    errors::{self, GitXetRepoError},
    git_integration::git_repo::GitRepo,
    summaries::analysis::FileSummary,
};

#[derive(Args, Debug)]
pub struct DirSummaryArgs {
    /// A git commit reference to build directory summary statistics
    #[clap(default_value = "HEAD")]
    reference: String,

    /// Deprecated. Does nothing.
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
            &repo,
            &args.reference,
            args.recursive,
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
    // file type summaries
    FileTypeSimple,
}

// hash map from summary type to summary
// where a summary is {value: count, value2: count}
type SummaryInfo = HashMap<SummaryType, HashMap<String, i64>>;

// hash map from dir (as String) to summaries for that dir (non-recursive)
type DirSummaries = HashMap<String, SummaryInfo>;

fn compute_file_summary(
    path: &str,
) -> errors::Result<FileSummary> {
    let mut ret = FileSummary::default();
    ret.libmagic = Some(summarize_libmagic(Path::new(path))?);
    Ok(ret)
}

pub async fn compute_dir_summaries(
    repo: &GitRepo,
    reference: &str,
    recursive: bool,
) -> errors::Result<DirSummaries> {
    let tree_listing = GitTreeListing::build(&repo.repo_dir, Some(reference), true, true, true)?;

    let mut dir_summary = DirSummaries::new();

    for blob_data in tree_listing.files {
        // For each file, compute file summary from file path
        let file_summary = compute_file_summary(
            &blob_data.path,
        )?;

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
