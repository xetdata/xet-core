use clap::Args;

use crate::config::XetConfig;
use crate::errors::Result;
use crate::git_integration::git_repo::GitRepo;

#[derive(Args, Debug, Clone)]
pub struct UninitArgs {
    /// Perform a full uninit, including all local and global settings.  
    /// If local_only or global_only is specified, then all local or global settings are given.
    #[clap(short, long)]
    pub full: bool,

    /// Purge git xet information from the .gitattributes file.  
    #[clap(long)]
    pub purge_gitattributes: bool,

    /// Purge git xet data directories from the local repository.
    #[clap(long)]
    pub purge_xet_data_dir: bool,

    /// Purge git xet config information from the local repository.
    #[clap(long)]
    pub purge_xet_config: bool,

    /// Purge repository filter config information.  This defaults to true if no flags are given.
    #[clap(long)]
    pub purge_filter_config: bool,

    /// Purge repository fetch config information.  
    #[clap(long)]
    pub purge_fetch_config: bool,

    /// Remove all the hooks from the local repository.  This defaults to true if no flags are given.
    #[clap(long)]
    pub remove_hooks: bool,

    /// Override any hooks or files locked by a "# XET LOCK" comment, removing them as well.
    #[clap(long)]
    pub ignore_locks: bool,
}

pub async fn uninit_command(config: XetConfig, args: &UninitArgs) -> Result<()> {
    let repo = GitRepo::open(config)?;

    // If the user has specified one of these flags, just do that.  Otherwise, do the default.
    let mut args: UninitArgs = args.clone();

    if !(args.remove_hooks
        || args.purge_gitattributes
        || args.purge_xet_data_dir
        || args.purge_xet_config
        || args.purge_filter_config
        || args.purge_fetch_config)
    {
        args.remove_hooks = true;
        args.purge_fetch_config = true;
    }

    repo.uninstall_xet_from_local_repo(&args)?;

    Ok(())
}
