use crate::config::XetConfig;
use crate::constants::{GIT_NOTES_MERKLEDB_V1_REF_NAME, GIT_NOTES_MERKLEDB_V2_REF_NAME};
use crate::errors::{self, GitXetRepoError};
use crate::git_integration::git_repo::get_mdb_version;
use crate::merkledb_plumb as mdbv1;
use crate::merkledb_shard_plumb as mdbv2;
use crate::utils;

use clap::{Args, Subcommand};
use merklehash::MerkleHash;
use std::path::PathBuf;

use mdb_shard::shard_version::ShardVersion;
use tracing::info;

/*
Clap CLI Argument definitions
*/

/// Plumbing commands for MerkleDB manipulation.
#[derive(Subcommand, Debug)]
#[non_exhaustive]
enum MerkleDBCommand {
    FindGitDB(MerkleDBFindGitDBArgs),
    Merge(MerkleDBMergeArgs),
    Diff(MerkleDBDiffArgs),
    Print(MerkleDBPrintArgs),
    Query(MerkleDBQueryArgs),
    MergeGit(MerkleDBGitMergeArgs),
    ExtractGit(MerkleDBGitExtractArgs),
    UpdateGit(MerkleDBGitUpdateArgs),
    ListGit(MerkleDBGitListArgs),
    Stat(MerkleDBGitStatArgs),
    /// Outputs statistics about the CAS entries tracked by the MerkleDB
    CASStat,
    /// Prints out the merkledb version of the current repository
    Version,
    FileEntryRewrite(MerkleDBFileDataSequenceEntryRewrite),
}

impl MerkleDBSubCommandShim {
    pub fn subcommand_name(&self) -> String {
        match self.subcommand {
            MerkleDBCommand::FindGitDB(_) => "find_git_db".to_string(),
            MerkleDBCommand::Merge(_) => "merge".to_string(),
            MerkleDBCommand::Diff(_) => "diff".to_string(),
            MerkleDBCommand::Print(_) => "print".to_string(),
            MerkleDBCommand::Query(_) => "query".to_string(),
            MerkleDBCommand::MergeGit(_) => "merge_git".to_string(),
            MerkleDBCommand::ExtractGit(_) => "extract_git".to_string(),
            MerkleDBCommand::UpdateGit(_) => "update_git".to_string(),
            MerkleDBCommand::ListGit(_) => "list_git".to_string(),
            MerkleDBCommand::Stat(_) => "stat".to_string(),
            MerkleDBCommand::CASStat => "casstat".to_string(),
            MerkleDBCommand::Version => "version".to_string(),
            MerkleDBCommand::FileEntryRewrite(_) => "file_entry_rewrite".to_string(),
        }
    }
}

// THIS "SHIM" STRUCT IS MANDATORY
#[derive(Args, Debug)]
pub struct MerkleDBSubCommandShim {
    #[clap(subcommand)]
    subcommand: MerkleDBCommand,
}
/// Prints the location for the active MerkleDB given a repository path.
/// This is usually .git/xet/merkledb.db in the repository root.
///
/// Inspects the current working directory a path is not provided
#[derive(Args, Debug)]
struct MerkleDBFindGitDBArgs {
    path: Option<PathBuf>,
}

/// Merges a collection of MerkleDB files into one file.
///
/// Computes the equivalent of result = Sum(inputs)
///
/// The result file is not overwritten, but is added to. That is, if the result
/// file already exists, the merge result will be appended to it.
#[derive(Args, Debug)]
struct MerkleDBMergeArgs {
    result: PathBuf,
    inputs: Vec<PathBuf>,
}

/// Writes out the diff between a newer MerkleDB file (first parameter) and an
/// older MerkleDB file (2nd parameter). The result will be written to a
/// result file.
///
/// Computes the equivalent of result = newer - older
///
/// The result file is not overwritten, but is added to. That is, if the result
/// file already exists, the delta will be appended to it instead. This allows
/// diffs to be easily combined. Hence if a symmetric diff is needed, you can
/// call the diff method twice flipping the input arguments.
///
///
/// ```ignore
/// git xet merkledb diff new.db old.db result.db
/// git xet merkledb diff old.db new.db result.db
/// ```
#[derive(Args, Debug)]
struct MerkleDBDiffArgs {
    newer: PathBuf,
    older: PathBuf,
    result: PathBuf,
}

/// Prints the contents of a MerkleDB file to stdout
#[derive(Args, Debug)]
struct MerkleDBPrintArgs {
    input: PathBuf,
}

/// Queries information about a Hash
#[derive(Args, Debug)]
struct MerkleDBQueryArgs {
    input: PathBuf,
    hash: String,
}

/// Merges the MerkleDB contents of git notes from head repo to base repo,
/// for merge across forks.
#[derive(Args, Debug)]
struct MerkleDBGitMergeArgs {
    base: PathBuf,
    head: PathBuf,
}

/// Extracts the contents of git notes appending to a MerkleDB file.
///
/// The result file is not overwritten, but is added to. That is, if the result
/// file already exists, the merge result will be appended to it.
///
/// If no output argument is provided, this defaults to the active MerkleDB
/// in the current repository (.git/xet/merkledb.db) which is equivalent
/// to calling
/// ```ignore
/// git xet merkle-db extract-git `git xet merkle-db find-git-db`
/// ```
///
/// Appends Sum(notes) into output.
#[derive(Args, Debug)]
struct MerkleDBGitExtractArgs {
    output: Option<PathBuf>,
    #[clap(short, long, default_value = "refs/notes/xet/merkledb")]
    notesref: String,
}

/// Writes out a MerkleDB to git notes.
/// The input db will be diffed against the contents already stored in git notes
/// and the remaining delta will be stored. The outcome is that git notes
/// will contain a superset of nodes as the input db.
///
/// Inserts (input - Sum(notes)) into notes.
#[derive(Args, Debug)]
struct MerkleDBGitUpdateArgs {
    input: PathBuf,
    #[clap(short, long, default_value = "refs/notes/xet/merkledb")]
    notesref: String,
}

/// Lists the MerkleDB contents of git notes, writing to stdout.
#[derive(Args, Debug)]
struct MerkleDBGitListArgs {
    #[clap(short, long, default_value = "refs/notes/xet/merkledb")]
    notesref: String,
}

/// Prints out deduplication statistics about a particular commit.
#[derive(Args, Debug)]
struct MerkleDBGitStatArgs {
    /// A git commit reference to read statistics about.
    /// Merge commits and initial commits are not supported.
    #[clap(default_value = "HEAD")]
    reference: String,

    /// Prints per-file change statistics in CSV form. Summary stats will not be printed.
    #[clap(short, long)]
    change_stats: bool,

    /// Prints working tree per-file smilarity in CSV form. Summary stats will not be printed.
    #[clap(short, long)]
    similarity: bool,
}

/// Rewrites a FileDataSequenceEntry in a Shard file.
#[derive(Args, Debug)]
struct MerkleDBFileDataSequenceEntryRewrite {
    #[clap(short, long)]
    shard_path: String,

    #[clap(long)]
    file_hash: String,

    #[clap(long)]
    entry_index: u32,

    #[clap(long)]
    cas_hash: Option<String>,

    #[clap(long)]
    cas_flags: Option<u32>,

    #[clap(long)]
    unpacked_segment_bytes: Option<u32>,

    #[clap(long)]
    chunk_byte_range_start: Option<u32>,

    #[clap(long)]
    chunk_byte_range_end: Option<u32>,
}

pub async fn handle_merkledb_plumb_command(
    cfg: XetConfig,
    command: &MerkleDBSubCommandShim,
) -> errors::Result<()> {
    let version = get_mdb_version(cfg.repo_path()?)?;
    info!("MDB version: {version:?}");
    match &command.subcommand {
        MerkleDBCommand::FindGitDB(args) => {
            println!("{:?}", mdbv1::find_git_db(args.path.clone())?);
            Ok(())
        }
        MerkleDBCommand::Merge(args) => {
            mdbv1::merge_merkledb(&args.result, &args.inputs).map_err(GitXetRepoError::from)
        }
        MerkleDBCommand::Diff(args) => mdbv1::diff_merkledb(&args.older, &args.newer, &args.result)
            .map_err(GitXetRepoError::from),
        MerkleDBCommand::Print(args) => match version {
            ShardVersion::V1 => mdbv1::print_merkledb(&args.input).map_err(GitXetRepoError::from),
            ShardVersion::V2 => {
                mdbv2::print_merkledb(&cfg.merkledb_v2_cache).map_err(GitXetRepoError::from)
            }
        },
        MerkleDBCommand::Query(args) => match version {
            ShardVersion::V1 => {
                mdbv1::query_merkledb(&args.input, &args.hash).map_err(GitXetRepoError::from)
            }
            ShardVersion::V2 => mdbv2::query_merkledb(&cfg, &args.hash).await,
        },
        MerkleDBCommand::MergeGit(args) => match version {
            ShardVersion::V1 => {
                utils::merge_git_notes(&args.base, &args.head, GIT_NOTES_MERKLEDB_V1_REF_NAME).await
            }
            ShardVersion::V2 => {
                utils::merge_git_notes(&args.base, &args.head, GIT_NOTES_MERKLEDB_V2_REF_NAME).await
            }
        },
        MerkleDBCommand::ExtractGit(args) => match version {
            ShardVersion::V1 => {
                if let Some(output) = &args.output {
                    mdbv1::merge_merkledb_from_git(&cfg, output, GIT_NOTES_MERKLEDB_V1_REF_NAME)
                        .await
                        .map_err(GitXetRepoError::from)
                } else {
                    mdbv1::merge_merkledb_from_git(
                        &cfg,
                        &mdbv1::find_git_db(None)?,
                        GIT_NOTES_MERKLEDB_V1_REF_NAME,
                    )
                    .await
                    .map_err(GitXetRepoError::from)
                }
            }
            ShardVersion::V2 => {
                if let Some(output) = &args.output {
                    mdbv2::sync_mdb_shards_from_git(
                        &cfg,
                        output,
                        GIT_NOTES_MERKLEDB_V2_REF_NAME,
                        true, // with Shard client we can disable this in the future
                    )
                    .await
                } else {
                    mdbv2::sync_mdb_shards_from_git(
                        &cfg,
                        &cfg.merkledb_v2_cache,
                        GIT_NOTES_MERKLEDB_V2_REF_NAME,
                        true, // with Shard client we can disable this in the future
                    )
                    .await
                }
            }
        },
        MerkleDBCommand::UpdateGit(args) => {
            mdbv1::update_merkledb_to_git(&cfg, &args.input, &args.notesref)
                .await
                .map_err(GitXetRepoError::from)
        }
        MerkleDBCommand::ListGit(args) => {
            mdbv1::list_git(&cfg, &args.notesref).map_err(GitXetRepoError::from)
        }
        MerkleDBCommand::Stat(args) => mdbv1::stat_git(
            &mdbv1::find_git_db(None)?,
            &args.reference,
            args.change_stats,
            args.similarity,
        )
        .map_err(GitXetRepoError::from),
        MerkleDBCommand::CASStat => match version {
            ShardVersion::V1 => {
                mdbv1::cas_stat_git(&mdbv1::find_git_db(None)?).map_err(GitXetRepoError::from)
            }
            ShardVersion::V2 => mdbv2::cas_stat_git(&cfg).await,
        },
        MerkleDBCommand::Version => {
            println!("{:?}", version.get_value());
            Ok(())
        }
        MerkleDBCommand::FileEntryRewrite(args) => match version {
            ShardVersion::V1 => unreachable!("Invalid command for V1 repo"),
            ShardVersion::V2 => {
                let file_hash = MerkleHash::from_hex(&args.file_hash).map_err(|_| {
                    GitXetRepoError::Other(
                        format!("Invalid MerkleHash {:?}", &args.file_hash).to_owned(),
                    )
                })?;
                let cas_hash = match &args.cas_hash {
                    Some(h) => Some(MerkleHash::from_hex(h).map_err(|_| {
                        GitXetRepoError::Other(format!("Invalid MerkleHash {:?}", h).to_owned())
                    })?),
                    None => None,
                };

                mdbv2::file_entry_rewrite(
                    &args.shard_path,
                    file_hash,
                    args.entry_index,
                    cas_hash,
                    args.cas_flags,
                    args.unpacked_segment_bytes,
                    args.chunk_byte_range_start,
                    args.chunk_byte_range_end,
                )
                .await?;

                Ok(())
            }
        },
    }
}
