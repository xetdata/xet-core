use crate::git_integration::{clone_xet_repo, git_user_config::verify_user_config};
use clap::Args;

use crate::config::XetConfig;
use crate::errors::Result;

/// Clone an existing repo.  This command ensures that XET is properly configured, then calls git clone
/// to clone the repo.
#[derive(Args, Debug)]
pub struct CloneArgs {
    /// If given, the repo is cloned without downloading the reference data blocks.   Data files will only show up as pointer files, and can later be downloaded using git xet checkout.
    #[clap(long)]
    no_smudge: bool,

    #[clap(long)]
    lazy: bool,

    /// All remaining arguments are passed to git clone.
    /// any arguments after '--' are unprocessed and passed through as is.
    /// This is useful for instance in:
    ///
    /// > git xet clone https://xethub.com/user/repo -- --branch abranch
    arguments: Vec<String>,
}

pub async fn clone_command(config: XetConfig, args: &CloneArgs) -> Result<()> {
    let arg_v: Vec<&str> = args.arguments.iter().map(|s| s.as_ref()).collect();

    // First check local config.
    verify_user_config(None)?;
    eprintln!("Preparing to clone Xet repository.");

    clone_xet_repo(
        Some(&config),
        &arg_v[..],
        args.no_smudge || args.lazy,
        None,
        true,
        true,
        false,
    )?;

    Ok(())
}
