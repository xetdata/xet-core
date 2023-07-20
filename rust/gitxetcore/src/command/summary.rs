use anyhow::anyhow;
use clap::{Args, Subcommand};
use pointer_file::PointerFile;
use serde::Serialize;
use std::{
    fs,
    path::{Path, PathBuf},
};
use tracing::warn;

use crate::{config::XetConfig, utils, errors::GitXetRepoError};
use crate::{
    constants::{GIT_NOTES_SUMMARIES_REF_NAME, POINTER_FILE_LIMIT},
    errors,
    git_integration::git_repo::GitRepo,
    summaries::csv::print_csv_summary,
    summaries::csv::print_csv_summary_from_reader,
    summaries::libmagic::print_libmagic_summary,
    summaries::summary_type::SummaryType,
    summaries_plumb::{summaries_dump, summaries_list_git, summaries_query, WholeRepoSummary},
};

#[derive(Args, Debug)]
pub struct SummaryArgs {
    #[clap(subcommand)]
    pub command: SummarySubCommand,
}

#[derive(Subcommand, Debug)]
pub enum SummarySubCommand {
    /// Computes the summary for a file and prints JSON to stdout.
    /// (Note: if the summary is stored in notes, this will not compute it again.)
    Compute {
        #[clap(long = "type", short = 't')]
        summary_type: SummaryType,

        file: PathBuf,
    },

    /// Computes the summary for a git blob and prints JSON to stdout.
    /// (Note: if the summary is stored in notes, this will not compute it again.)
    ComputeFromBlobId {
        #[clap(long = "type", short = 't')]
        summary_type: SummaryType,

        blobid: String,
    },
    /// Lists the summary contents of git notes, writing to stdout
    ListGit,

    /// Merges the summary contents of git notes from head repo to base repo,
    /// for merge across forks.
    MergeGit { base: PathBuf, head: PathBuf },

    /// Queries for the stats summary in git notes for a file with the given merklehash
    Query {
        #[clap(long)]
        merklehash: String,
    },

    /// Writes out all summary types for all files (from git notes) as JSON.
    Dump,
}

fn print_stored_summary_impl<T: Serialize>(t: &Option<T>) -> errors::Result<()>
where
    T: Default,
{
    match t {
        Some(item) => {
            let str = serde_json::to_string_pretty(item)?;
            println!("{str}");
            Ok(())
        }
        None => {
            warn!("stored file-level summary was missing requested summary type");
            // return empty but valid JSON in case caller expects it
            println!("{{}}");
            Ok(())
        }
    }
}

async fn print_summary_from_db(
    config: &XetConfig,
    pointer_file: PointerFile,
    summary_type: &SummaryType,
) -> errors::Result<()> {
    let summarydb = WholeRepoSummary::load_or_recreate_from_git(
        config,
        &config.summarydb,
        GIT_NOTES_SUMMARIES_REF_NAME,
    )
    .await?;
    let summary = summarydb.get(pointer_file.hash()).ok_or_else(|| {
        anyhow!(
            "could not find summary for pointer file with hash {}",
            pointer_file.hash()
        )
    })?;
    match summary_type {
        SummaryType::Libmagic => print_stored_summary_impl(&summary.libmagic),
        SummaryType::Csv => print_stored_summary_impl(&summary.csv),
    }?;
    Ok(())
}

async fn print_summary(
    config: &XetConfig,
    summary_type: &SummaryType,
    file_path: &Path,
) -> errors::Result<()> {
    // first, see if the input file is a pointer file
    let size = fs::metadata(file_path)?.len();
    if size <= POINTER_FILE_LIMIT as u64 {
        // see if it's a pointer
        let file_path_str = file_path.to_string_lossy().to_string();
        let pointer_file = PointerFile::init_from_path(&file_path_str);
        if pointer_file.is_valid() {
            return print_summary_from_db(config, pointer_file, summary_type).await;
        }
    }
    // fall through. Non-pointer.
    match summary_type {
        SummaryType::Libmagic => print_libmagic_summary(file_path)
            .map_err(|e| errors::GitXetRepoError::Other(e.to_string())),
        SummaryType::Csv => print_csv_summary(file_path),
    }
}

async fn print_summary_from_blobid(
    config: &XetConfig,
    summary_type: &SummaryType,
    blobid: &str,
) -> errors::Result<()> {
    let repo = GitRepo::open(config.clone())?.repo;
    let blob = repo.find_blob(git2::Oid::from_str(blobid)?)?;

    let blob_size = blob.size();
    let content = blob.content();
    if blob_size <= POINTER_FILE_LIMIT {
        if let Ok(content_str) = std::str::from_utf8(content) {
            let pointer_file = PointerFile::init_from_string(content_str, "");
            if pointer_file.is_valid() {
                return print_summary_from_db(config, pointer_file, summary_type).await;
            }
        }
    }
    // fall through. Non-pointer.
    match summary_type {
        SummaryType::Libmagic => Err(GitXetRepoError::InvalidOperation("file type summarization from contents not supported".to_string())),
        SummaryType::Csv => print_csv_summary_from_reader(&mut &content[..]),
    }
}

pub async fn summary_command(config: XetConfig, args: &SummaryArgs) -> errors::Result<()> {
    match &args.command {
        SummarySubCommand::Compute { summary_type, file } => {
            print_summary(&config, summary_type, file).await
        }
        SummarySubCommand::ComputeFromBlobId {
            summary_type,
            blobid,
        } => print_summary_from_blobid(&config, summary_type, blobid).await,
        SummarySubCommand::ListGit => summaries_list_git(config).await,
        SummarySubCommand::MergeGit { base, head } => {
            utils::merge_git_notes(base, head, GIT_NOTES_SUMMARIES_REF_NAME).await
        }
        SummarySubCommand::Query { merklehash } => summaries_query(config, merklehash).await,
        SummarySubCommand::Dump => summaries_dump(config).await,
    }
}
