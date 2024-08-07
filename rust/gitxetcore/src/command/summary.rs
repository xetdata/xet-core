use clap::{Args, Subcommand};
use serde::Serialize;
use std::io::stdin;
use std::path::{Path, PathBuf};
use tracing::warn;

use chunkpipe::pipe;
use error_printer::ErrorPrinter;
use merklehash::MerkleHash;
use tableau_summary::tds::printer::{print_tds_summary, print_tds_summary_from_reader};
use tableau_summary::tds::TdsSummary;
use tableau_summary::twb::printer::{print_twb_summary, print_twb_summary_from_reader};
use tableau_summary::twb::TwbSummary;

use crate::data::{PointerFile, PointerFileTranslator};
use crate::summaries::csv::CsvDelimiter;
use crate::{config::XetConfig, errors::GitXetRepoError, utils};
use crate::{
    constants::{GIT_NOTES_SUMMARIES_REF_NAME, POINTER_FILE_LIMIT},
    errors,
    git_integration::GitXetRepo,
    summaries::csv::print_csv_summary,
    summaries::csv::print_csv_summary_from_reader,
    summaries::libmagic::print_libmagic_summary,
    summaries::summary_type::SummaryType,
    summaries::{summaries_dump, summaries_list_git, summaries_query, WholeRepoSummary},
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

        #[clap(long = "delimiter", short = 'd')]
        csv_delimiter: Option<CsvDelimiter>,

        #[clap(long = "force", short = 'f')]
        force_compute: bool,
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

    /// Computes the summary for a file smudged from a hash and prints JSON to stdout.
    ComputeFromHash {
        #[clap(long = "type", short = 't')]
        summary_type: SummaryType,

        hash: String,

        #[clap(long = "delimiter", short = 'd')]
        csv_delimiter: Option<CsvDelimiter>,
    },

    ComputeFromStdin {
        #[clap(long = "type", short = 't')]
        summary_type: SummaryType,

        #[clap(long = "delimiter", short = 'd')]
        csv_delimiter: Option<CsvDelimiter>,
    },
}

fn print_stored_summary_impl<T: Serialize + Default>(t: &Option<T>) -> errors::Result<()> {
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
    pointer_file: &PointerFile,
    summary_type: &SummaryType,
) -> errors::Result<()> {
    let summarydb = WholeRepoSummary::load_or_recreate_from_git(
        config,
        &config.summarydb,
        GIT_NOTES_SUMMARIES_REF_NAME,
    )
    .await?;
    let summary = summarydb.get(pointer_file.hash_string()).ok_or_else(|| {
        GitXetRepoError::SummaryDBNotFoundError(format!(
            "could not find summary for pointer file with hash {}",
            pointer_file.hash_string()
        ))
    })?;
    match summary_type {
        SummaryType::Libmagic => print_stored_summary_impl(&summary.libmagic),
        SummaryType::Csv => print_stored_summary_impl(&summary.csv),
        SummaryType::Twb => {
            if let Some(sum) = summary.additional_summaries.as_ref() {
                print_stored_summary_impl(&sum.twb)
            } else {
                print_stored_summary_impl::<TwbSummary>(&None)
            }
        }
        SummaryType::Tds => {
            if let Some(sum) = summary.additional_summaries.as_ref() {
                print_stored_summary_impl(&sum.tds)
            } else {
                print_stored_summary_impl::<TdsSummary>(&None)
            }
        }
    }?;
    Ok(())
}

async fn print_summary(
    config: &XetConfig,
    summary_type: &SummaryType,
    file_path: &Path,
) -> errors::Result<()> {
    // see if it's a pointer
    let pointer_file = PointerFile::init_from_path(file_path);
    if pointer_file.is_valid() {
        return print_summary_from_db(config, &pointer_file, summary_type).await;
    }
    // fall through. Non-pointer.
    match summary_type {
        SummaryType::Libmagic => {
            print_libmagic_summary(file_path).map_err(|e| GitXetRepoError::Other(e.to_string()))
        }
        SummaryType::Csv => print_csv_summary(file_path),
        SummaryType::Twb => print_twb_summary(file_path).map_err(GitXetRepoError::from),
        SummaryType::Tds => print_tds_summary(file_path).map_err(GitXetRepoError::from),
    }
}

async fn print_summary_from_blobid(
    config: &XetConfig,
    summary_type: &SummaryType,
    blobid: &str,
    csv_delimiter: Option<CsvDelimiter>,
    force_compute: bool,
) -> errors::Result<()> {
    let repo = GitXetRepo::open(config.clone())?.repo;
    let blob = repo.find_blob(git2::Oid::from_str(blobid)?)?;

    let blob_size = blob.size();
    let content = blob.content();
    if blob_size > POINTER_FILE_LIMIT {
        return print_summary_from_reader(&mut &content[..], summary_type, csv_delimiter);
    }
    if let Ok(content_str) = std::str::from_utf8(content) {
        let pointer_file = PointerFile::init_from_string(content_str, "");
        if pointer_file.is_valid() {
            if force_compute {
                return print_summary_from_hash(
                    config,
                    summary_type,
                    pointer_file.hash_string(),
                    csv_delimiter,
                )
                .await;
            }
            let res = print_summary_from_db(config, &pointer_file, summary_type).await;
            if let Err(GitXetRepoError::SummaryDBNotFoundError(_)) = res {
                // not in summary db, expected that we have to recompute
                return print_summary_from_hash(
                    config,
                    summary_type,
                    pointer_file.hash_string(),
                    csv_delimiter,
                )
                .await;
            }
            return res;
        }
    }
    print_summary_from_reader(&mut &content[..], summary_type, csv_delimiter)
}

async fn print_summary_from_hash(
    config: &XetConfig,
    summary_type: &SummaryType,
    hash: &str,
    csv_delimiter: Option<CsvDelimiter>,
) -> errors::Result<()> {
    if let SummaryType::Libmagic = summary_type {
        return Err(GitXetRepoError::InvalidOperation(
            "file type summarization from contents not supported".to_string(),
        ));
    }
    let pft = PointerFileTranslator::v2_from_config_smudge_only(config).await?;
    let hash = MerkleHash::from_hex(hash)?;

    let (mut w, mut r) = pipe();

    let smudge_handle =
        tokio::spawn(async move { pft.smudge_file_from_hash(None, &hash, &mut w, None).await });

    let res = print_summary_from_reader(&mut r, summary_type, csv_delimiter)
        .log_error("error summarizing: ");
    let (smudge_res,) = tokio::join!(smudge_handle);
    smudge_res?.log_error("error from smudging?: ")?;
    res
}

async fn print_summary_from_stdin(
    _xet_config: &XetConfig,
    summary_type: &SummaryType,
    csv_delimiter: Option<CsvDelimiter>,
) -> errors::Result<()> {
    let mut r = stdin();
    print_summary_from_reader(&mut r, summary_type, csv_delimiter)
}

fn print_summary_from_reader<T: std::io::Read>(
    reader: &mut T,
    summary_type: &SummaryType,
    csv_delimiter: Option<CsvDelimiter>,
) -> errors::Result<()> {
    match summary_type {
        SummaryType::Libmagic => Err(GitXetRepoError::InvalidOperation(
            "file type summarization from contents not supported".to_string(),
        )),
        SummaryType::Twb => print_twb_summary_from_reader(reader).map_err(GitXetRepoError::from),
        SummaryType::Tds => print_tds_summary_from_reader(reader).map_err(GitXetRepoError::from),
        SummaryType::Csv => {
            print_csv_summary_from_reader(reader, csv_delimiter.unwrap_or_default().into())
        }
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
            csv_delimiter,
            force_compute,
        } => {
            print_summary_from_blobid(
                &config,
                summary_type,
                blobid,
                *csv_delimiter,
                *force_compute,
            )
            .await
        }
        SummarySubCommand::ListGit => summaries_list_git(config).await,
        SummarySubCommand::MergeGit { base, head } => {
            utils::merge_git_notes(base, head, GIT_NOTES_SUMMARIES_REF_NAME, &config).await
        }
        SummarySubCommand::Query { merklehash } => summaries_query(config, merklehash).await,
        SummarySubCommand::Dump => summaries_dump(config).await,
        SummarySubCommand::ComputeFromHash {
            summary_type,
            hash,
            csv_delimiter,
        } => print_summary_from_hash(&config, summary_type, hash, *csv_delimiter).await,
        SummarySubCommand::ComputeFromStdin {
            summary_type,
            csv_delimiter,
        } => print_summary_from_stdin(&config, summary_type, *csv_delimiter).await,
    }
}
