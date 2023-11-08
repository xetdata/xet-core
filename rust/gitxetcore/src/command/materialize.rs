use clap::Args;
use lazy::lazy_pathlist_config::{check_or_create_lazy_config, LazyPathListConfigFile};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::error;

use crate::errors::{GitXetRepoError, Result};
use crate::git_integration::{list_files_from_repo, run_git_captured, GitXetRepo};
use crate::{config::XetConfig, constants::GIT_LAZY_CHECKOUT_CONFIG};

#[derive(Args, Debug)]
pub struct MaterializeArgs {
    #[clap(short, long)]
    recursive: bool,

    path: String,
}

pub async fn materialize_command(cfg: XetConfig, args: &MaterializeArgs) -> Result<()> {
    let repo = GitXetRepo::open(cfg.clone())?;

    let lazy_config_path = if let Some(path) = &cfg.lazy_config {
        path.to_owned()
    } else {
        let path = cfg.repo_path()?.join(GIT_LAZY_CHECKOUT_CONFIG);
        check_or_create_lazy_config(&path).await?;
        path
    };

    // At this point lazy config path is valid.
    let mut lazyconfig =
        LazyPathListConfigFile::load_smudge_list_from_file(&lazy_config_path, false).await?;

    // now find where we are in the working dir.
    let pwd = std::env::current_dir()?;
    let workdir_root = repo.repo_dir;
    let relative_to_workroot = pwd.strip_prefix(&workdir_root).map_err(|e| {
        error!("Failed to get relative path to the working directory root.");
        GitXetRepoError::Other(e.to_string())
    })?;

    // if now in a subdir and user typed ".", the root_path should not include the "."
    let root_path = if relative_to_workroot != Path::new("") && args.path == "." {
        relative_to_workroot.to_owned()
    } else {
        relative_to_workroot.join(&args.path)
    };

    let path_list = list_files_from_repo(
        &repo.repo,
        root_path.to_str().unwrap_or_default(),
        None,
        args.recursive,
    )?;

    if path_list.is_empty() {
        eprintln!(
            "Didn't find any files under {}, skip materializing.",
            args.path
        );
        return Ok(());
    }

    eprintln!("Materializing {} files...", path_list.len());

    let path_list_ref: Vec<_> = path_list.iter().map(|s| s.as_str()).collect();

    lazyconfig.add_rules(&path_list_ref);

    // saves changes to the file
    drop(lazyconfig);

    // rerun smudge filter

    // touch all files we want to materialize
    for p in path_list_ref.iter() {
        let relative_path_from_curdir =
            Path::new(p)
                .strip_prefix(relative_to_workroot)
                .map_err(|e| {
                    error!(
                        "Failed to get relative path for {p:?} to the current working directory."
                    );
                    GitXetRepoError::Other(e.to_string())
                })?;
        touch_file(relative_path_from_curdir)?;
    }

    // files not touched under args.path will not rerun through the filter
    let git_args = ["--", &args.path];

    run_git_captured(None, "checkout", &git_args, true, None)?;

    eprintln!("Done");

    Ok(())
}

fn touch_file(file_path: &Path) -> anyhow::Result<()> {
    use utime::*;

    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

    set_file_times(file_path, now, now)?;

    Ok(())
}
