use crate::config::XetConfig;
use crate::constants::{GIT_NOTES_MERKLEDB_V1_REF_NAME, GIT_NOTES_MERKLEDB_V2_REF_NAME};
use crate::data::mdb::{self as mdbv2, force_sync_shard, get_mdb_version};
use crate::data::mdbv1;
use crate::errors::{self, GitXetRepoError};
use crate::git_integration::git_repo_salt::read_repo_salt_by_dir;
use crate::utils;

use clap::{Args, Subcommand};
use merklehash::MerkleHash;
use std::path::PathBuf;

use mdb_shard::shard_version::ShardVersion;
use tracing::{error, info};

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
    ForceSync(MerkleDBForceShardServerSync),
    /// Outputs statistics about the CAS entries tracked by the MerkleDB
    CASStat,
    Version(MerkleDBVersionArgs),
    Upgrade(MerkleDBUpgradeArgs),
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
            MerkleDBCommand::ForceSync(_) => "force_sync".to_string(),
            MerkleDBCommand::Version(_) => "version".to_string(),
            MerkleDBCommand::Upgrade(_) => "upgrade".to_string(),
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

/// Forces a specific shard stored in CAS to be synced to the shard server.
///
#[derive(Args, Debug)]
struct MerkleDBForceShardServerSync {
    hash: String,
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

/// Prints out the merkledb version of the current repository
#[derive(Args, Debug)]
struct MerkleDBVersionArgs {
    /// Also prints repo salt
    #[clap(long)]
    with_salt: bool,
}

/// Upgrades repo MerkleDB and fix potential issues.
#[derive(Args, Debug)]
struct MerkleDBUpgradeArgs {
    #[clap(long, hide = true)]
    fix: bool,
}

pub async fn handle_merkledb_plumb_command(
    cfg: XetConfig,
    command: &MerkleDBSubCommandShim,
) -> errors::Result<()> {
    let version = get_mdb_version(cfg.repo_path()?, &cfg)?;
    info!("MDB version: {version:?}");
    match &command.subcommand {
        MerkleDBCommand::FindGitDB(args) => match version {
            ShardVersion::Uninitialized => {
                error!("Repo is not initialized for Xet.");
                Err(GitXetRepoError::RepoUninitialized(format!(
                    "FindGitDB: Shard version config not detected in repo={:?}.",
                    cfg.repo_path()
                )))
            }
            ShardVersion::V1 => {
                println!("{:?}", mdbv1::find_git_db(args.path.clone())?);
                Ok(())
            }
            ShardVersion::V2 => todo!(),
        },
        MerkleDBCommand::Merge(args) => match version {
            ShardVersion::Uninitialized => {
                error!("Repo is not initialized for Xet.");
                Err(GitXetRepoError::RepoUninitialized(format!(
                    "Merge: Shard version config not detected in repo={:?}.",
                    cfg.repo_path()
                )))
            }
            ShardVersion::V1 => {
                mdbv1::merge_merkledb(&args.result, &args.inputs).map_err(GitXetRepoError::from)
            }
            ShardVersion::V2 => todo!(),
        },
        MerkleDBCommand::Diff(args) => match version {
            ShardVersion::Uninitialized => {
                error!("Repo is not initialized for Xet.");
                Err(GitXetRepoError::RepoUninitialized(format!(
                    "Diff: Shard version config not detected in repo={:?}.",
                    cfg.repo_path()
                )))
            }
            ShardVersion::V1 => mdbv1::diff_merkledb(&args.older, &args.newer, &args.result)
                .map_err(GitXetRepoError::from),
            ShardVersion::V2 => todo!(),
        },
        MerkleDBCommand::Print(args) => match version {
            ShardVersion::Uninitialized => {
                error!("Repo is not initialized for Xet.");
                Err(GitXetRepoError::RepoUninitialized(format!(
                    "Print: Shard version config not detected in repo={:?}.",
                    cfg.repo_path()
                )))
            }
            ShardVersion::V1 => mdbv1::print_merkledb(&args.input).map_err(GitXetRepoError::from),
            ShardVersion::V2 => todo!(),
        },
        MerkleDBCommand::Query(args) => match version {
            ShardVersion::Uninitialized => {
                error!("Repo is not initialized for Xet.");
                Err(GitXetRepoError::RepoUninitialized(format!(
                    "Query: Shard version config not detected in repo={:?}.",
                    cfg.repo_path()
                )))
            }
            ShardVersion::V1 => {
                mdbv1::query_merkledb(&args.input, &args.hash).map_err(GitXetRepoError::from)
            }
            ShardVersion::V2 => mdbv2::query_merkledb(&cfg, &args.hash).await,
        },
        MerkleDBCommand::MergeGit(args) => match version {
            ShardVersion::V1 => {
                utils::merge_git_notes(&args.base, &args.head, GIT_NOTES_MERKLEDB_V1_REF_NAME, &cfg)
                    .await
            }
            ShardVersion::V2 => {
                utils::merge_git_notes(&args.base, &args.head, GIT_NOTES_MERKLEDB_V2_REF_NAME, &cfg)
                    .await
            }
            ShardVersion::Uninitialized => {
                error!("Repo is not initialized for Xet.");
                Err(GitXetRepoError::RepoUninitialized(format!(
                    "MergeGit: Shard version config not detected in repo={:?}.",
                    cfg.repo_path()
                )))
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
            ShardVersion::Uninitialized => {
                error!("Repo is not initialized for Xet.");
                Err(GitXetRepoError::RepoUninitialized(format!(
                    "ExtractGit: Shard version config not detected in repo={:?}.",
                    cfg.repo_path()
                )))
            }
        },
        MerkleDBCommand::UpdateGit(args) => match version {
            ShardVersion::Uninitialized => {
                error!("Repo is not initialized for Xet.");
                Err(GitXetRepoError::RepoUninitialized(format!(
                    "UpdateGit: Shard version config not detected in repo={:?}.",
                    cfg.repo_path()
                )))
            }
            ShardVersion::V1 => mdbv1::update_merkledb_to_git(&cfg, &args.input, &args.notesref)
                .await
                .map_err(GitXetRepoError::from),
            ShardVersion::V2 => todo!(),
        },
        MerkleDBCommand::ListGit(args) => match version {
            ShardVersion::Uninitialized => {
                error!("Repo is not initialized for Xet.");
                Err(GitXetRepoError::RepoUninitialized(format!(
                    "ListGit: Shard version config not detected in repo={:?}.",
                    cfg.repo_path()
                )))
            }
            ShardVersion::V1 => {
                mdbv1::list_git(&cfg, &args.notesref).map_err(GitXetRepoError::from)
            }
            ShardVersion::V2 => todo!(),
        },
        MerkleDBCommand::Stat(args) => match version {
            ShardVersion::Uninitialized => {
                error!("Repo is not initialized for Xet.");
                Err(GitXetRepoError::RepoUninitialized(format!(
                    "Stat: Shard version config not detected in repo={:?}.",
                    cfg.repo_path()
                )))
            }
            ShardVersion::V1 => mdbv1::stat_git(
                &mdbv1::find_git_db(None)?,
                &args.reference,
                args.change_stats,
                args.similarity,
            )
            .map_err(GitXetRepoError::from),
            ShardVersion::V2 => todo!(),
        },
        MerkleDBCommand::CASStat => match version {
            ShardVersion::Uninitialized => {
                error!("Repo is not initialized for Xet.");
                Err(GitXetRepoError::RepoUninitialized(format!(
                    "CASStat: Shard version config not detected in repo={:?}.",
                    cfg.repo_path()
                )))
            }
            ShardVersion::V1 => {
                mdbv1::cas_stat_git(&mdbv1::find_git_db(None)?).map_err(GitXetRepoError::from)
            }
            ShardVersion::V2 => mdbv2::cas_stat_git(&cfg).await,
        },
        MerkleDBCommand::Version(args) => {
            println!("{{");
            if args.with_salt && version.need_salt() {
                let reposalt = read_repo_salt_by_dir(cfg.repo_path()?, &cfg)?;
                if let Some(reposalt) = reposalt {
                    println!("\"repo_salt\" : \"{}\",", base64::encode(reposalt));
                } else {
                    return Err(GitXetRepoError::RepoSaltUnavailable(format!(
                        "Failed to read a repo salt from a MDB {version:?} repo"
                    )));
                }
            }
            println!("\"mdb_version\" : \"{}\"", version.get_value());
            println!("}}");
            Ok(())
        }
        MerkleDBCommand::ForceSync(args) => match version {
            ShardVersion::Uninitialized => {
                error!("Repo is not initialized for Xet.");
                Err(GitXetRepoError::RepoUninitialized(format!(
                    "ForceSync: Shard version config not detected in repo={:?}.",
                    cfg.repo_path()
                )))
            }
            ShardVersion::V1 => {
                error!("ForceSync not valid for MDB V1 repo.");
                Err(GitXetRepoError::InvalidOperation(format!(
                    "ForceSync not valid for repo={:?}.",
                    cfg.repo_path()
                )))
            }
            ShardVersion::V2 => {
                let hash = MerkleHash::from_hex(&args.hash)
                    .map_err(|e| GitXetRepoError::Other(format!("{e:?}")))?;

                force_sync_shard(&cfg, &hash).await
            }
        },
        MerkleDBCommand::Upgrade(args) => match version {
            ShardVersion::Uninitialized => {
                error!("Repo is not initialized for Xet.");
                Err(GitXetRepoError::RepoUninitialized(format!(
                    "Upgrade: Shard version config not detected in repo={:?}.",
                    cfg.repo_path()
                )))
            }
            // When both V1 and V2 refs notes exist, version check logic in new
            // clients will see it as a V2 repo.
            ShardVersion::V1 => mdbv2::merkledb_upgrade(&cfg).await,
            ShardVersion::V2 => {
                if args.fix {
                    mdbv2::merkledb_upgrade(&cfg).await?;
                } else {
                    println!("Repo already uses MDB V2, not available for upgrade");
                }

                Ok(())
            }
        },
    }
}
