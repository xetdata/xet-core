use crate::config::XetConfig;
use crate::errors;
use crate::git_integration::git_repo::GitRepo;
use clap::Args;

#[derive(Args, Debug)]
pub struct InitArgs {
    /// If set, will write the config information to the global settings instead of local
    #[clap(long, short)]
    pub global_config: bool,

    /// Always write out the local config.  By default, this is done only if the global config is not set.
    #[clap(long)]
    pub force_local_config: bool,

    /// If .gitattributes is present, then preserve the file as is.  
    #[clap(long)]
    pub preserve_gitattributes: bool,

    #[clap(long)]
    pub enable_locking: bool,

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

    /// If set, initial for bare repo, skipping setting global or local configs.
    /// Ignores all options except "mdb_version".
    #[clap(long, short)]
    pub bare: bool,
}

pub async fn init_command(config: XetConfig, args: &InitArgs) -> errors::Result<()> {
    let mut repo = GitRepo::open(config)?;
    if args.bare {
        repo.install_gitxet_for_bare_repo(args.mdb_version).await?;
    } else {
        repo.install_gitxet(&args).await?;
    }
    Ok(())
}
