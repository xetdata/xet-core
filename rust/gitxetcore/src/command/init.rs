use crate::config::XetConfig;
use crate::errors;
use crate::git_integration::git_repo::GitRepo;
use clap::Args;

#[derive(Args, Debug)]
pub struct InitArgs {
    /// If set, will write the config information to the global settings instead of local
    #[clap(long, short)]
    global_config: bool,

    /// Always write out the local config.  By default, this is done only if the global config is not set.
    #[clap(long)]
    force_local_config: bool,

    /// If .gitattributes is present, then preserve the file as is.  
    #[clap(long)]
    preserve_gitattributes: bool,

    #[clap(long)]
    enable_locking: bool,

    /// If local is given, initialize the repository even if one of the remotes is not a registered domain.
    #[clap(long, short)]
    force: bool,

    /// The merkledb version to use, 2 is MDB Shard.
    #[clap(long, short, default_value_t = 2)]
    mdb_version: u64,

    /// If set, initial for bare repo, skipping setting global or local configs.
    /// Ignores all options except "mdb_version".
    #[clap(long, short)]
    bare: bool,
}

pub async fn init_command(config: XetConfig, args: &InitArgs) -> errors::Result<()> {
    let mut repo = GitRepo::open(config)?;
    if args.bare {
        repo.install_gitxet_for_bare_repo(args.mdb_version).await?;
    } else {
        repo.install_gitxet(
            args.global_config,
            args.force_local_config,
            args.preserve_gitattributes,
            args.force,
            args.enable_locking,
            args.mdb_version,
        )
        .await?;
    }
    Ok(())
}
