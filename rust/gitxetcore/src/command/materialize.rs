use crate::data::smudge_pointerfile_to_itself;
use clap::Args;
use itertools::Itertools;
use lazy::lazy_pathlist_config::{check_or_create_lazy_config, LazyPathListConfigFile};
use parutils::tokio_par_for_each;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::error;

use crate::constants::MAX_CONCURRENT_DOWNLOADS;
use crate::data::PointerFileTranslator;
use crate::errors::Result;
use crate::git_integration::{filter_files_from_index, walk_working_dir, GitXetRepo};
use crate::{config::XetConfig, constants::GIT_LAZY_CHECKOUT_CONFIG};

#[derive(Args, Debug)]
pub struct MaterializeArgs {
    #[clap(short, long)]
    recursive: bool,

    paths: Vec<PathBuf>,
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

    let workdir_root = repo.repo_dir.clone();

    // now they are relative path to the working directory root
    let path_list = args
        .paths
        .iter()
        .map(|path| {
            let ret = walk_working_dir(&workdir_root, path, args.recursive);
            if let Err(e) = &ret {
                error!("{e:?}");
            }
            ret
        })
        .filter_map(|ret| ret.ok())
        .flatten()
        .collect_vec();

    let path_list = filter_files_from_index(&path_list, repo.repo.clone())?;

    if path_list.is_empty() {
        eprintln!(
            "Didn't find any checked in files under {:?}, skip materializing.",
            &args.paths
        );
        return Ok(());
    }

    eprintln!("Materializing {} file(s)...", path_list.len());

    let path_list_ref: Vec<_> = path_list
        .iter()
        .map(|s| s.to_str().unwrap_or_default())
        .collect();

    lazyconfig.add_rules(&path_list_ref);

    // saves changes to the file
    drop(lazyconfig);

    // smudge all files
    let absolute_path_list: Vec<_> = path_list.iter().map(|p| workdir_root.join(p)).collect();

    let translator = Arc::new(PointerFileTranslator::from_config_in_repo(&cfg).await?);
    let translator_ref = &translator;

    tokio_par_for_each(
        absolute_path_list,
        *MAX_CONCURRENT_DOWNLOADS,
        |path, _| async move {
            let translator = translator_ref.clone();
            smudge_pointerfile_to_itself(&translator, &path).await
        },
    )
    .await
    .map_err(|e| match e {
        parutils::ParallelError::JoinError => {
            anyhow::anyhow!("Join error on smudge file")
        }
        parutils::ParallelError::TaskError(e) => e,
    })?;

    // update index so materialized files don't show as "Changes not staged for commit"
    repo.run_git_checked_in_repo("add", &["-u"])?;

    eprintln!("Done");

    Ok(())
}
