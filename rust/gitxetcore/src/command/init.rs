use std::path::PathBuf;

use crate::config::XetConfig;
use crate::errors;
use crate::git_integration::git_repo::GitRepo;
use clap::Args;

#[derive(Args, Debug, Clone)]
pub struct InitArgs {
    /// By default, all parts of the setup process are performed.  If this flag is given,
    /// then only the setup steps that are specified explicitly are performed.  
    #[clap(short, long)]
    pub explicit: bool,

    /// Install a given toml config file to .xet/config.toml as part of the initialization.
    #[clap(long)]
    pub xet_config_file: Option<PathBuf>,

    /// Write the .gitattributes file to the repo.  
    #[clap(long)]
    pub write_gitattributes: bool,

    /// If .gitattributes is present, then preserve the file as is if attempting to write to it.
    #[clap(long)]
    pub preserve_gitattributes: bool,

    /// Create and write out the repo salt to the notes.
    #[clap(long)]
    pub write_repo_salt: bool,

    /// Create and write out the MerkleDB information to the notes.
    #[clap(long)]
    pub write_mdb_notes: bool,

    /// Write out the needed hooks.
    #[clap(long, short)]
    pub write_hooks: bool,

    /// Init the cache directories inside the .git/xet folder.
    #[clap(long)]
    pub init_cache_directories: bool,

    /// Write fetch configuration for remotes to the local git config so that xet notes are automatically fetched
    /// from the remotes.
    #[clap(long)]
    pub write_remote_fetch_config: bool,

    /// When writing out the hooks, enable locking using git-lfs.
    #[clap(long)]
    pub enable_locking: bool,

    /// Set the filter config
    #[clap(long)]
    pub write_filter_config: bool,

    /// Write the filter config information to the global settings instead of the local repo.
    #[clap(long, short)]
    pub global_config: bool,

    /// Write the local filter config information, even if the global filter config is set.
    #[clap(long, short)]
    pub force_local_config: bool,

    /// Alias for --explicit --mdb-notes --repo-salt
    #[clap(short, long)]
    pub bare: bool,

    /// If init is called on an empty bare repo, then create a branch with this name associated
    /// with the initial git commit.
    #[clap(long, default_value("main"))]
    pub branch_name_on_empty_repo: String,

    /// If local is given, initialize the repository even if one of the remotes is not a registered domain.
    #[clap(long, short)]
    pub force: bool,

    /// The merkledb version to use, default value is the newest version.
    #[clap(long, short, default_value_t = 2)]
    pub mdb_version: u64,
}

pub async fn init_command(config: XetConfig, args: &InitArgs) -> errors::Result<()> {
    let mut repo = GitRepo::open(config)?;

    let mut args = args.clone();

    if args.bare {
        args.explicit = true;
        args.write_mdb_notes = true;
        args.write_repo_salt = true;
    }

    // If --explicit is not given, then add in everything.
    if !args.explicit {
        args.write_hooks = true;
        args.write_gitattributes = true;
        args.write_mdb_notes = true;
        args.write_repo_salt = true;
        args.write_filter_config = true;
    }

    repo.perform_explicit_setup(args).await?;
    Ok(())
}
