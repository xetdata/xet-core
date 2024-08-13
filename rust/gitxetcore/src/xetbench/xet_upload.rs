use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;

use gitxetcore::config::{ConfigGitPathOption, XetConfig};
use gitxetcore::data::PointerFileTranslatorV2;
use gitxetcore::data::remote_shard_interface::GlobalDedupPolicy;
use gitxetcore::environment::log::initialize_tracing_subscriber;
use gitxetcore::git_integration::{clone_xet_repo, GitXetRepo};
use tracing::debug;

use crate::xet_bench_config::XetBenchConfig;

async fn validate_remote_repo(bench_config: &XetBenchConfig, xet_config: &XetConfig) -> Result<()> {
    fs::remove_dir_all(&bench_config.xet_clone_repo_directory)?;
    xet_config.permission.create_dir_all(&bench_config.xet_clone_repo_directory)?;

    if let Err(e) = clone_xet_repo(
        Some(&xet_config),
        &["--bare", &bench_config.xet_clone_repo_url, &bench_config.xet_clone_repo_directory],
        true,
        None,
        true,
        true,
        true,
    ) {
        eprintln!(
            "Error accessing destination repository at {:?}: {e:?}",
            &bench_config.xet_clone_repo_directory
        );
        eprintln!("\nPlease ensure the repository url is correct and you have run git xet login.");
        Err(e)?;
    }

    Ok(())
}

pub async fn upload_files(bench_config: &XetBenchConfig, dataset_files: &Vec<PathBuf>) -> Result<()> {
    let  xet_config = XetConfig::new(None, None, ConfigGitPathOption::NoPath)?;
    validate_remote_repo(bench_config, &xet_config).await?;
    let mut xet_config = xet_config.switch_repo_path(
        ConfigGitPathOption::PathDiscover(bench_config.xet_clone_repo_directory.clone().parse()?),
        None,
    )?;
    // set the cas_cache not to be staging but do direct to remote
    //
    xet_config.global_dedup_query_policy = GlobalDedupPolicy::Always;
    initialize_tracing_subscriber(&xet_config)?;

    // This will open and configure everything
    let xet_repo = GitXetRepo::open_and_verify_setup(xet_config.clone()).await?;
    let dest = xet_repo.repo.clone();
    let pft = Arc::new(
        PointerFileTranslatorV2::from_config(&xet_repo.xet_config, xet_repo.repo_salt().await?).await?,
    );
    // todo parallelize like migration.rs:491
    // todo check this is the only steps needed
    for dataset_file in dataset_files {
        debug!("[xetbench upload] cleaning starting on {:?}", dataset_file);
        let src_data = fs::read(dataset_file)?;
        let _ret_data = pft.clean_file_and_report_progress(
            &dataset_file,
            src_data,
            &None,
        ).await?;
        debug!("[xetbench upload] cleaning done on {:?}", dataset_file);
        pft.upload_cas_staged(false).await?;
    }
    pft.finalize().await?;
    xet_repo.pre_push_hook("origin").await?; // push the shards

    Ok(())
}
