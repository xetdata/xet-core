use crate::constants::GIT_LAZY_CHECKOUT_CONFIG;
use crate::git_integration::git_repo::{verify_user_config, GitRepo};
use clap::Args;
use lazy::lazy_rule_config::XET_LAZY_CLONE_ENV;
use tracing::info;

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

    if args.lazy {
        // We cannot reliably detect the directory cloned into,
        // because of how git picks the "humanish" part of the source
        // repository (https://git-scm.com/docs/git-clone#Documentation/git-clone.txt-ltdirectorygt),
        // and scenarios when a directory is specified in command line arguments.
        //
        // So we set a env var and delegate setting the lazy configuration
        // to the filter command.

        eprintln!(
            "Setting up lazyconfig under your repo at REPO_ROOT/{GIT_LAZY_CHECKOUT_CONFIG}. 
If cloning an empty branch, please create this file manually at the aforementioned path."
        );
        info!(
            "Setting up lazyconfig under the cloned repo at REPO_ROOT/{GIT_LAZY_CHECKOUT_CONFIG}"
        );

        std::env::set_var(XET_LAZY_CLONE_ENV, "1");
    }

    GitRepo::clone(
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
