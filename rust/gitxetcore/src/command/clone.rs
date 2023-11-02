use std::env::current_dir;

use crate::constants::GIT_LAZY_CHECKOUT_CONFIG;
use crate::git_integration::git_repo::{verify_user_config, GitRepo};
use clap::Args;
use lazy::lazy_config::write_default_lazy_config;

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

    let (repo, _) = GitRepo::clone(
        Some(&config),
        &arg_v[..],
        args.no_smudge || args.lazy,
        None,
        true,
        true,
        false,
    )?;

    if args.lazy {
        let mut path = current_dir()?;
        path.push(repo);

        let config = config
            .switch_repo_path(crate::config::ConfigGitPathOption::PathDiscover(path), None)?;

        path = config.repo_path()?.to_owned();
        path.push(GIT_LAZY_CHECKOUT_CONFIG);
        write_default_lazy_config(&path)?;
    }

    Ok(())
}
