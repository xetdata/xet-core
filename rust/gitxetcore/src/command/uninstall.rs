use clap::Args;

use crate::config::XetConfig;
use crate::errors::{GitXetRepoError, Result};
use crate::git_integration::GitXetRepo;

#[derive(Args, Debug)]
pub struct UninstallArgs {
    /// Remove git xet filter config information from the local config settings
    #[clap(short, long)]
    pub local: bool,
}

pub async fn uninstall_command(config: XetConfig, args: &UninstallArgs) -> Result<()> {
    // If global_only or local_only flags are set, restrict to those.  Otherwise, do both.
    if args.local {
        if config.associated_with_repo() {
            let repo = GitXetRepo::open(config)?;
            repo.purge_local_filter_config()?;
        } else {
            return Err(GitXetRepoError::InvalidOperation(
                "--local can only be used inside an existing git repo.".to_string(),
            ));
        }
    } else {
        GitXetRepo::purge_global_filter_config()?;
    }

    Ok(())
}
