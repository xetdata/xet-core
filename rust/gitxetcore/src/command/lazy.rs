use clap::{Args, Subcommand};
use lazy::lazy_config::LazyConfig;
use std::path::PathBuf;

use crate::config::XetConfig;
use crate::errors::{GitXetRepoError, Result};
use crate::git_integration::git_repo::GitRepo;
use crate::git_integration::git_wrap;

#[non_exhaustive]
#[derive(Subcommand, Debug)]
enum LazyCommand {
    /// Checks if a config file has correct syntax and
    /// rules are non-conflicting.
    Check,
    Match(LazyMatchArgs),
    /// After editting a config file, apply changes to the
    /// working directory.
    Apply,
}

/// Given a path relative to the repository root, find
/// the rule that will be applied.
#[derive(Args, Debug)]
struct LazyMatchArgs {
    path: PathBuf,
}

// THIS "SHIM" STRUCT IS MANDATORY
#[derive(Args, Debug)]
pub struct LazyCommandShim {
    #[clap(subcommand)]
    subcommond: LazyCommand,
}

impl LazyCommandShim {
    pub fn subcommand_name(&self) -> String {
        match self.subcommond {
            LazyCommand::Check => "check".to_string(),
            LazyCommand::Match(_) => "match".to_string(),
            LazyCommand::Apply => "apply".to_string(),
        }
    }
}

pub async fn lazy_command(cfg: XetConfig, command: &LazyCommandShim) -> Result<()> {
    match &command.subcommond {
        LazyCommand::Check => lazy_check_command(&cfg).await,
        LazyCommand::Match(args) => lazy_match_command(&cfg, args).await,
        LazyCommand::Apply => lazy_apply_command(&cfg).await,
    }
}

async fn lazy_check_command(cfg: &XetConfig) -> Result<()> {
    if let Some(lazyconfig) = &cfg.lazy_config {
        let lazyconfig = LazyConfig::load_from_file(lazyconfig).await?;

        lazyconfig.check()?;

        Ok(())
    } else {
        Err(GitXetRepoError::InvalidOperation(
            "lazy config file doesn't exist".to_owned(),
        ))
    }
}

async fn lazy_match_command(cfg: &XetConfig, args: &LazyMatchArgs) -> Result<()> {
    if let Some(lazyconfig) = &cfg.lazy_config {
        let lazyconfig = LazyConfig::load_from_file(lazyconfig).await?;

        let matched = lazyconfig.match_rule(&args.path)?;

        println!("Matched {matched:?}");

        Ok(())
    } else {
        Err(GitXetRepoError::InvalidOperation(
            "lazy config file doesn't exist".to_owned(),
        ))
    }
}

async fn lazy_apply_command(cfg: &XetConfig) -> Result<()> {
    // rm .git/index
    // git checkout -f

    let repo = GitRepo::open(cfg.clone())?;

    if !repo.repo_is_clean()? {
        return Err(GitXetRepoError::InvalidOperation(
            "repo is dirty, abort...".to_owned(),
        ));
    }

    std::fs::remove_file(cfg.repo_path()?.join("index"))?;

    git_wrap::run_git_captured(None, "checkout", &["--force"], true, None)?;

    Ok(())
}
