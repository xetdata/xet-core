use crate::config::XetConfig;
use crate::errors;
use crate::git_integration::git_repo::GitRepo;
use clap::Args;

#[derive(Args, Debug)]
pub struct InstallArgs {
    /// If set, will only modify the local repo's git configuration
    #[clap(long, short)]
    local: bool,
}

pub async fn install_command(config: XetConfig, args: &InstallArgs) -> errors::Result<()> {
    if args.local {
        let repo = GitRepo::open(config)?;
        repo.write_local_filter_config()?;
    } else {
        GitRepo::write_global_xet_config()?;
    }

    Ok(())
}
