use clap::{Args, Subcommand};

use crate::config::XetConfig;
use crate::errors;
use crate::git_integration::git_repo::GitRepo;

#[derive(Args, Debug)]
pub struct RemoteArg {
    /// The name of the remote
    #[clap(long, short)]
    remote: String,
}

#[derive(Args, Debug)]
pub struct RefTransactionArg {
    /// The name of the action being executed.
    #[clap(long, short)]
    action: String,
}

#[derive(Args, Debug)]
pub struct PrePushArg {
    /// The name of the remote.
    #[clap(long)]
    remote: String,

    /// The location address of the remote, as provided by git.
    #[clap(long)]
    remote_loc: String,
}

#[derive(Args, Debug)]
pub struct PostCheckoutLFSArg {
    #[clap(long)]
    previous: String,

    #[clap(long)]
    new: String,

    #[clap(long)]
    flag: String,
}

#[derive(Args, Debug)]
pub struct PostMergeLFSArg {
    /// Specifies whether or not the merge is a squash merge
    #[clap(long)]
    flag: String,
}

#[derive(Args, Debug)]
pub struct FilterConfigArg {
    /// Only write the filter config locally
    #[clap(long, short)]
    local: bool,
}

/// Plumbing commands for the git intergation hooks.
#[derive(Subcommand, Debug)]
#[non_exhaustive]
#[clap(about = "Git Integration Plumbing Commands", long_about = None)]
enum HookCommand {
    // The hooks we install
    /// Run the reference transaction hook; this pulls refspecs from stdin,
    /// triggering an update when any remote is updated to also update the accompaning
    /// merkledb notes.
    ReferenceTransactionHook(RefTransactionArg),

    /// Run the pre-push hook that pushes all the staged content to the remote CAS, as well as
    /// updating the notes containing the merkledb information.
    PrePushHook(PrePushArg),

    /// Handle LFS style locking
    PostCommitLFSHook,
    PostMergeLFSHook(PostMergeLFSArg),
    PostCheckoutLFSHook(PostCheckoutLFSArg),

    // Plumbing commands for syncing remote things to the local
    /// Fetch the remote notes containing the merkledb information to the local notes.
    SyncRemoteToNotes(RemoteArg),

    /// Update the local merkledb file from the notes stored in the local repository.
    SyncNotesToMerkleDB,

    /// Shorthand for SyncRemoteToNotes + SyncNotesToMerkleDB.
    SyncRemoteToMerkleDB(RemoteArg),

    // The plumbing commands for syncing things from local changes to the remote.
    /// Update the local notes with any new information contained in the merkledb file.
    SyncMerkleDBToNotes,

    /// Push the local notes to the remote repository.  
    /// Involves fetching the remote to merge any information.
    SyncNotesToRemote(RemoteArg),

    /// Shorthand for SyncMerkleDBToNotes + SyncNotestoRemote.
    SyncMerkleDBToRemote(RemoteArg),

    // The plumbing commands for writing out parts of the config.
    /// Install the pre-push hook.
    WritePrepushHook,

    /// Install the reference transaction hook.
    WriteReferenceTransactionHook,

    /// Install the configuration information to set the repository
    /// to automatically fetch notes from all registered remotes
    WriteRepoFetchConfig,

    /// Write the filter config
    WriteFilterConfig(FilterConfigArg),

    /// Write the .gitattributes file.
    WriteGitAttributes,
}

#[derive(Args, Debug)]
pub struct HookCommandShim {
    #[clap(subcommand)]
    subcommand: HookCommand,
}

pub async fn handle_hook_plumb_command(
    config: XetConfig,
    command: &HookCommandShim,
) -> errors::Result<()> {
    let repo = || GitRepo::open(config.clone());

    match &command.subcommand {
        // Hooks
        HookCommand::ReferenceTransactionHook(action) => {
            repo()?.reference_transaction_hook(&action.action)?
        }
        HookCommand::PrePushHook(remote_info) => repo()?.pre_push_hook(&remote_info.remote).await?,

        // Locking hooks
        HookCommand::PostMergeLFSHook(args) => repo()?.post_merge_lfs_hook(&args.flag).await?,
        HookCommand::PostCheckoutLFSHook(args) => {
            repo()?
                .post_checkout_lfs_hook(&args.previous, &args.new, &args.flag)
                .await?
        }
        HookCommand::PostCommitLFSHook => repo()?.post_commit_lfs_hook().await?,

        // Download direction
        HookCommand::SyncRemoteToNotes(remote) => repo()?.sync_remote_to_notes(&remote.remote)?,
        HookCommand::SyncNotesToMerkleDB => repo()?.sync_notes_to_dbs().await?,
        HookCommand::SyncRemoteToMerkleDB(remote) => {
            repo()?.sync_remote_to_notes(&remote.remote)?;
            repo()?.sync_notes_to_dbs().await?;
        }

        // Upload direction
        HookCommand::SyncMerkleDBToNotes => repo()?.sync_dbs_to_notes().await?,
        HookCommand::SyncNotesToRemote(remote) => repo()?.sync_notes_to_remote(&remote.remote)?,
        HookCommand::SyncMerkleDBToRemote(remote) => {
            repo()?.sync_dbs_to_notes().await?;
            repo()?.sync_notes_to_remote(&remote.remote)?;
        }

        // Write config stuff
        HookCommand::WritePrepushHook => {
            let _ = repo()?.write_prepush_hook()?;
        }
        HookCommand::WriteReferenceTransactionHook => {
            {
                let _ = repo()?.write_reference_transaction_hook()?;
            };
        }
        HookCommand::WriteRepoFetchConfig => {
            let _ = repo()?.write_repo_fetch_config()?;
        }
        HookCommand::WriteFilterConfig(filter_config) => {
            if filter_config.local {
                if config.associated_with_repo() {
                    repo()?.write_local_filter_config()?;
                } else {
                    return Err(errors::GitXetRepoError::InvalidOperation(
                        "Command must be run inside a git repository.".to_string(),
                    ));
                }
            } else {
                GitRepo::write_global_xet_config()?;
            }
        }
        HookCommand::WriteGitAttributes => {
            let _ = repo()?.write_gitattributes(true)?;
        }
    };

    Ok(())
}

impl HookCommandShim {
    pub fn subcommand_name(&self) -> String {
        match &self.subcommand {
            HookCommand::ReferenceTransactionHook(args) => {
                format!("reference_transaction.{}", args.action)
            }
            HookCommand::PrePushHook(_) => "pre_push".to_string(),
            HookCommand::SyncRemoteToNotes(_) => "sync_remote_to_notes".to_string(),
            HookCommand::SyncNotesToMerkleDB => "sync_notes_to_merkledb".to_string(),
            HookCommand::SyncRemoteToMerkleDB(_) => "sync_remote_to_merkledb".to_string(),
            HookCommand::SyncMerkleDBToNotes => "sync_merkledb_to_notes".to_string(),
            HookCommand::SyncNotesToRemote(_) => "sync_notes_to_remote".to_string(),
            HookCommand::SyncMerkleDBToRemote(_) => "sync_merkledb_to_remote".to_string(),
            HookCommand::WritePrepushHook => "write_pre_push".to_string(),
            HookCommand::WriteReferenceTransactionHook => "write_reference_transaction".to_string(),
            HookCommand::WriteRepoFetchConfig => "write_repo_fetch_config".to_string(),
            HookCommand::WriteFilterConfig(_) => "write_filter_config".to_string(),
            HookCommand::WriteGitAttributes => "write_git_attributes".to_string(),
            HookCommand::PostCheckoutLFSHook(_) => "post_checkout".to_string(),
            HookCommand::PostCommitLFSHook => "post_commit".to_string(),
            HookCommand::PostMergeLFSHook(_) => "post_merge".to_string(),
        }
    }
}
