use std::path::PathBuf;

use crate::config::XetConfig;
use crate::errors;
use crate::git_integration::git_repo::GitRepo;
use clap::Args;

#[derive(Args, Debug)]
pub struct InitArgs {
    /// If set, will write the config information to the global settings instead of local
    #[clap(long, short)]
    pub global_config: bool,

    /// If set, will skip writing out the config
    #[clap(long, short)]
    pub skip_filter_config: bool,

    /// Always write out the local config.  By default, this is done only if the global config is not set.
    #[clap(long)]
    pub force_local_config: bool,

    /// If .gitattributes is present, then preserve the file as is.  
    #[clap(long)]
    pub preserve_gitattributes: bool,

    /// Enables locking through the git LFS mechanic; assumes remote is configured to use git-lfs locking
    #[clap(long)]
    pub enable_locking: bool,

    /// If init is called on an empty bare repo, then create a branch with this name associated
    /// with the initial git commit.
    #[clap(long, default_value("main"))]
    pub branch_name_on_empty_repo: String,

    /// If local is given, initialize the repository even if one of the remotes is not a registered domain.
    #[clap(long, short)]
    pub force: bool,

    /// The merkledb version to use, 2 is MDB Shard.
    #[clap(long, short, default_value_t = 2)]
    pub mdb_version: u64,

    /// Only install the absolute minimal components needed to configure the repository for Xet use.  All others will
    /// be implicitly created on the first run of the filter.
    #[clap(long)]
    pub minimal: bool,

    /// Install a toml config file as part of the initialization.
    #[clap(long)]
    pub xet_config_file: Option<PathBuf>,

    /// If given, only configures the notes.
    #[clap(long, alias("bare"))]
    pub notes_only: bool,
}

pub async fn init_command(config: XetConfig, args: &InitArgs) -> errors::Result<()> {
    let mut repo = GitRepo::open(config)?;
    repo.perform_explicit_setup(args).await?;
    Ok(())
}
