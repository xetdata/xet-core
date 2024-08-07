use clap::{Args, Subcommand};
use lazy::lazy_pathlist_config::{print_lazy_config, LazyPathListConfigFile};
use lazy::lazy_rule_config::LazyStrategy;
use std::path::PathBuf;

use crate::config::XetConfig;
use crate::errors::{GitXetRepoError, Result};
use crate::git_integration::run_git_captured;
use crate::git_integration::GitXetRepo;

#[non_exhaustive]
#[derive(Subcommand, Debug)]
enum LazyCommand {
    /// Checks if a config file has correct syntax and
    /// rules are non-conflicting.
    Check,
    Match(LazyMatchArgs),
    /// After editing a config file, apply changes to the
    /// working directory.
    Apply,
    /// List files that are materialized in a lazy cloned repository.
    Show,
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
            LazyCommand::Show => "show".to_string(),
        }
    }
}

pub async fn lazy_command(cfg: XetConfig, command: &LazyCommandShim) -> Result<()> {
    match &command.subcommond {
        LazyCommand::Check => lazy_check_command(&cfg).await,
        LazyCommand::Match(args) => lazy_match_command(&cfg, args).await,
        LazyCommand::Apply => lazy_apply_command(&cfg).await,
        LazyCommand::Show => lazy_show_command(&cfg),
    }
}

async fn lazy_check_command(cfg: &XetConfig) -> Result<()> {
    if let Some(lazyconfig) = &cfg.lazy_config {
        let _lazyconfig =
            LazyPathListConfigFile::load_smudge_list_from_file(lazyconfig, true).await?;

        Ok(())
    } else {
        Err(GitXetRepoError::InvalidOperation(
            "lazy config file doesn't exist".to_owned(),
        ))
    }
}

async fn lazy_match_command(cfg: &XetConfig, args: &LazyMatchArgs) -> Result<()> {
    if let Some(lazyconfig) = &cfg.lazy_config {
        let lazyconfig =
            LazyPathListConfigFile::load_smudge_list_from_file(lazyconfig, false).await?;

        let matched = lazyconfig.match_rule(&args.path);

        match matched {
            LazyStrategy::SMUDGE => eprintln!("Path matched, will materialize {:?}", &args.path),
            LazyStrategy::POINTER => {
                eprintln!("No path matched, will keep {:?} dematerialized", &args.path)
            }
        }

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

    let repo = GitXetRepo::open(cfg.clone())?;

    if !repo.repo_is_clean()? {
        return Err(GitXetRepoError::InvalidOperation(
            "Repo is dirty; commit your changes and try this operation again.".to_owned(),
        ));
    }

    std::fs::remove_file(cfg.repo_path()?.join("index"))?;

    run_git_captured(None, "checkout", &["--force"], true, None)?;

    Ok(())
}

fn lazy_show_command(cfg: &XetConfig) -> Result<()> {
    if let Some(lazyconfig) = &cfg.lazy_config {
        print_lazy_config(lazyconfig)?;

        Ok(())
    } else {
        Err(GitXetRepoError::InvalidOperation(
            "lazy config file doesn't exist".to_owned(),
        ))
    }
}
