use clap::Args;
use lazy::lazy_pathlist_config::{check_or_create_lazy_config, LazyPathListConfig};
use std::path::PathBuf;

use crate::errors::{GitXetRepoError, Result};
use crate::git_integration::git_repo::GitRepo;
use crate::{config::XetConfig, constants::GIT_LAZY_CHECKOUT_CONFIG};

#[derive(Args, Debug)]
pub struct MaterializeArgs {
    #[clap(short, long)]
    recursive: bool,

    path: PathBuf,
}

pub async fn materialize_command(cfg: XetConfig, args: &MaterializeArgs) -> Result<()> {
    // Make sure repo working directory is clean
    let repo = GitRepo::open(cfg.clone())?;

    if !repo.repo_is_clean()? {
        return Err(GitXetRepoError::InvalidOperation(
            "Repo is dirty; commit your changes and try this operation again.".to_owned(),
        ));
    }

    let lazy_config_path = cfg.lazy_config.unwrap_or_else(|| {
        let path = cfg.repo_path()?.join(GIT_LAZY_CHECKOUT_CONFIG);
        check_or_create_lazy_config(&path)?;
        path
    });

    // At this point lazy config path is valid.
    let lazyconfig =
        LazyPathListConfig::load_smudge_list_from_file(lazy_config_path.unwrap(), false).await?;

    todo!()
}
