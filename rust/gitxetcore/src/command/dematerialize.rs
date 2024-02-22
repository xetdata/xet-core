use clap::Args;
use itertools::Itertools;
use lazy::lazy_pathlist_config::{check_or_create_lazy_config, LazyPathListConfigFile};
use parutils::tokio_par_for_each;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::error;

use crate::constants::{GIT_MAX_PACKET_SIZE, MAX_CONCURRENT_UPLOADS};
use crate::data::PointerFileTranslator;
use crate::errors::Result;
use crate::git_integration::{filter_files_from_index, walk_working_dir, GitXetRepo};
use crate::stream::data_iterators::AsyncFileIterator;
use crate::{config::XetConfig, constants::GIT_LAZY_CHECKOUT_CONFIG};

#[derive(Args, Debug)]
pub struct DematerializeArgs {
    #[clap(short, long)]
    recursive: bool,

    paths: Vec<PathBuf>,
}

pub async fn dematerialize_command(cfg: XetConfig, args: &DematerializeArgs) -> Result<()> {
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
            "Didn't find any checked in files under {:?}, skip dematerializing.",
            args.paths
        );
        return Ok(());
    }

    eprintln!("Dematerializing {} file(s)...", path_list.len());

    let path_list_ref: Vec<_> = path_list
        .iter()
        .map(|s| s.to_str().unwrap_or_default())
        .collect();

    lazyconfig.drop_rules(&path_list_ref);

    // saves changes to the file
    drop(lazyconfig);

    // clean all files
    let absolute_path_list: Vec<_> = path_list.iter().map(|p| workdir_root.join(p)).collect();

    let translator = Arc::new(PointerFileTranslator::from_config_in_repo(&cfg).await?);
    let translator_ref = &translator;

    tokio_par_for_each(
        absolute_path_list,
        MAX_CONCURRENT_UPLOADS,
        |path, _| async move {
            let translator = translator_ref.clone();
            clean_file_to_itself(&translator, &path).await
        },
    )
    .await
    .map_err(|e| match e {
        parutils::ParallelError::JoinError => {
            anyhow::anyhow!("Join error on clean file")
        }
        parutils::ParallelError::TaskError(e) => e,
    })?;

    // update index so dematerialized files don't show as "Changes not staged for commit"
    repo.run_git_checked_in_repo("add", &["-u"])?;

    eprintln!("Done");

    Ok(())
}

/// Clean a file and overwrite itself
async fn clean_file_to_itself(
    translator: &PointerFileTranslator,
    path: &Path,
) -> anyhow::Result<()> {
    let reader = BufReader::new(File::open(path)?);

    let async_reader = AsyncFileIterator::new(reader, GIT_MAX_PACKET_SIZE);

    let pointer_file = translator.clean_file(path, async_reader).await?;

    std::fs::write(path, pointer_file)?;

    Ok(())
}
