use crate::config::XetConfig;
use crate::errors::{self, GitXetRepoError};
use crate::git_integration::{GitTreeListing, GitXetRepo};
use crate::summaries::analysis::FileSummary;
use clap::Args;
use libmagic::libmagic::summarize_libmagic;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
};

const DIR_SUMMARY_VERSION: i64 = 1;

#[derive(Args, Debug)]
pub struct DirSummaryArgs {
    /// A git commit reference to build directory summary statistics
    #[clap(default_value = "HEAD")]
    reference: String,

    /// If true, aggregate results so that each directory contains the results of all
    /// subdirectories as well.  Otherwise, the summary for a directory ignores
    /// subdirectories.  
    #[clap(long)]
    recursive: bool,
}

pub async fn dir_summary_command(config: XetConfig, args: &DirSummaryArgs) -> errors::Result<()> {
    let repo = GitXetRepo::open(config.clone())?;

    // recompute the dir summary
    let summaries = compute_dir_summaries(&repo, &args.reference, args.recursive).await?;

    let content_str = serde_json::to_string_pretty(&summaries).map_err(|_| {
        GitXetRepoError::Other("Failed to serialize dir summaries to JSON".to_string())
    })?;

    println!("{content_str}");
    Ok(())
}

type FileExtension = String;
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct PerFileInfo {
    count: i64,
    display_name: String,
}
type SummaryInfo = HashMap<FileExtension, PerFileInfo>;

type FolderPath = String;
// hash map from dir (as String) to summaries for that dir (non-recursive)
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct DirSummaries {
    version: i64,
    summaries: HashMap<FolderPath, SummaryInfo>,
}

impl Default for DirSummaries {
    fn default() -> Self {
        Self {
            version: DIR_SUMMARY_VERSION,
            summaries: Default::default(),
        }
    }
}

fn compute_file_summary(path: &str) -> errors::Result<FileSummary> {
    Ok(FileSummary {
        libmagic: Some(summarize_libmagic(Path::new(path))?),
        ..Default::default()
    })
}

pub async fn compute_dir_summaries(
    repo: &GitXetRepo,
    reference: &str,
    recursive: bool,
) -> errors::Result<DirSummaries> {
    let tree_listing = GitTreeListing::build(&repo.repo_dir, Some(reference), true, true, true)?;

    let mut dir_summary = DirSummaries::default();

    for blob_data in tree_listing.files {
        // For each file, compute file summary from file path
        let file_summary = compute_file_summary(&blob_data.path)?;

        // Now, go through and increase the counts for these file types in this directory.
        let entry_path = PathBuf::from_str(&blob_data.path).unwrap();
        let entry_dir = entry_path.parent().unwrap_or_else(|| Path::new(""));

        let summaries = dir_summary
            .summaries
            .entry(entry_dir.to_string_lossy().to_string())
            .or_default();

        if let Some(ref libmagic_summary) = file_summary.libmagic {
            let extension = libmagic_summary.file_type.clone();
            // exclude empty file extension from dir summaries
            if !extension.is_empty() {
                let file_type_simple_summary = summaries.entry(extension).or_insert(PerFileInfo {
                    count: 0,
                    display_name: libmagic_summary.file_type_simple.clone(),
                });

                file_type_simple_summary.count += 1;
            }
        }
    }

    if recursive {
        // Now, go through and create a new dir summary that has aggregated all the entries back up
        // to their parent directories.
        let mut aggregated_ds = DirSummaries::default();

        for (path, st_hashmap) in dir_summary.summaries.into_iter() {
            for (file_type, info) in st_hashmap.into_iter() {
                let count = info.count;
                let mut entry_dir = PathBuf::from_str(&path).unwrap();

                loop {
                    let summaries = aggregated_ds
                        .summaries
                        .entry(entry_dir.to_string_lossy().to_string())
                        .or_default();

                    let file_type_simple_summary =
                        summaries.entry(file_type.clone()).or_insert(PerFileInfo {
                            count: 0,
                            display_name: info.display_name.clone(),
                        });

                    file_type_simple_summary.count += count;

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
        Ok(aggregated_ds)
    } else {
        Ok(dir_summary)
    }
}
