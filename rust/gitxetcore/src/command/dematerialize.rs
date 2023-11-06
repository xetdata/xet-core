use clap::Args;
use lazy::lazy_pathlist_config::{check_or_create_lazy_config, LazyPathListConfigFile};

use crate::errors::{GitXetRepoError, Result};
use crate::git_integration::git_repo::GitRepo;
use crate::git_integration::git_wrap::{self, list_files_from_repo};
use crate::{config::XetConfig, constants::GIT_LAZY_CHECKOUT_CONFIG};

#[derive(Args, Debug)]
pub struct DematerializeArgs {
    #[clap(short, long)]
    recursive: bool,

    path: String,
}

pub async fn dematerialize_command(cfg: XetConfig, args: &DematerializeArgs) -> Result<()> {
    // Make sure repo working directory is clean
    let repo = GitRepo::open(cfg.clone())?;

    eprintln!("Checking repo state...");

    if !repo.repo_is_clean()? {
        return Err(GitXetRepoError::InvalidOperation(
            "Repo is dirty; commit your changes and try this operation again.".to_owned(),
        ));
    }

    let lazy_config_path = if let Some(path) = &cfg.lazy_config {
        path.to_owned()
    } else {
        let path = cfg.repo_path()?.join(GIT_LAZY_CHECKOUT_CONFIG);
        check_or_create_lazy_config(&path).await?;
        path
    };

    eprintln!("Dematerializing...");

    // At this point lazy config path is valid.
    let mut lazyconfig =
        LazyPathListConfigFile::load_smudge_list_from_file(&lazy_config_path, false).await?;

    let path_list = list_files_from_repo(&repo.repo, &args.path, None, args.recursive)?;

    let path_list_ref: Vec<_> = path_list.iter().map(|s| s.as_str()).collect();

    lazyconfig.drop_rules(&path_list_ref);

    // saves changes to the file
    drop(lazyconfig);

    // rerun smudge filter
    std::fs::remove_file(cfg.repo_path()?.join("index"))?;

    git_wrap::run_git_captured(None, "checkout", &["--force"], true, None)?;

    eprintln!("Done");

    Ok(())
}
