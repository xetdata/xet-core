use std::fs;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;

use gitxetcore::command::CliOverrides;
use gitxetcore::config::{ConfigGitPathOption, XetConfig};
use gitxetcore::data::remote_shard_interface::GlobalDedupPolicy;
use gitxetcore::data::PointerFileTranslatorV2;
use gitxetcore::git_integration::{clone_xet_repo, GitXetRepo};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::info;
use gitxetcore::environment::log::initialize_tracing_subscriber;
use gitxetcore::errors::GitXetRepoError;
use crate::xet_bench_config::XetBenchConfig;
const BENCH_PROCESSING_PERMITS:usize = 256;

async fn validate_remote_repo(bench_config: &XetBenchConfig, xet_config: &XetConfig) -> Result<()> {
    fs::remove_dir_all(&bench_config.xet_clone_repo_directory)?;
    xet_config
        .permission
        .create_dir_all(&bench_config.xet_clone_repo_directory)?;

    if let Err(e) = clone_xet_repo(
        Some(xet_config),
        &[
            "--bare",
            &bench_config.xet_clone_repo_url,
            &bench_config.xet_clone_repo_directory,
        ],
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

pub async fn upload_files(
    bench_config: &XetBenchConfig,
    dataset_files: &Vec<PathBuf>,
) -> Result<()> {
    // NOT working, check with Di
    if !bench_config.benchmark_cas_endpoint.is_empty() {
        unsafe {
            env::set_var("XET_CAS_SERVER", bench_config.benchmark_cas_endpoint.clone());
            env::set_var("XET_CAS_PROTOCOL_VERSION", "0.3.0");
        }
    }
    let xet_config = XetConfig::new(None, None, ConfigGitPathOption::NoPath)?;
    validate_remote_repo(bench_config, &xet_config).await?;
    let mut xet_config = xet_config.switch_repo_path(
        ConfigGitPathOption::PathDiscover(bench_config.xet_clone_repo_directory.clone().parse()?),
        Some(CliOverrides {
            // Disable global dedup, this together with shards deletion below make sure
            // that this benchmark simulates "first time upload".
            global_dedup_query_policy: Some(GlobalDedupPolicy::Never),
            ..Default::default()
        }),
    )?;

    // Disable staging so that xorbs are directly uploaded to remote as soon as generated.
    xet_config.staging_path = None;

    initialize_tracing_subscriber(&xet_config)?;

    // This will open the repo and pull in {repo_salt, mdb, summary} git notes,
    // and download all shards.
    let xet_repo = GitXetRepo::open_and_verify_setup(xet_config.clone()).await?;

    // Delete downloaded shards from the above step as part of the "first time upload" benchmark setup.
    for entry in fs::read_dir(&xet_repo.merkledb_v2_cache_dir)? {
        fs::remove_file(entry?.path())?;
    }

    // dir for shard session so that the generated shards from cleaning this file
    // will not be used by cleaning other files, part of the "first time upload" benchmark setup.
    for entry in fs::read_dir(&bench_config.mdb_session_directory)? {
        fs::remove_dir_all(entry?.path())?;
    }

    let bench_processing_permits = Arc::new(Semaphore::new(BENCH_PROCESSING_PERMITS));
    let mut bench_processing_pool = JoinSet::<gitxetcore::errors::Result<(PathBuf, Vec<u8>)>>::new();

    let mut dataset_file_count = 0;
    for dataset_file in dataset_files {
        info!("[xetbench] cleaning starting on {:?} with id {}", dataset_file, dataset_file_count);
        let current_mdb_session_directory = format!("{}/{}", bench_config.mdb_session_directory, dataset_file_count);
        xet_config.permission.create_dir_all(current_mdb_session_directory.clone())?;
        dataset_file_count += 1;
        let bench_processing_permits = bench_processing_permits.clone();

        let mut local_xet_config = xet_config.clone();

        local_xet_config.merkledb_v2_session = current_mdb_session_directory.parse()?;

        let pft = Arc::new(
            PointerFileTranslatorV2::from_config(&local_xet_config, xet_repo.repo_salt().await?)
                .await?,
        );


        let src_data = fs::read(dataset_file)?;
        let dataset_file_clone = dataset_file.clone();
        let bench_config_mdb_session_directory_clone = current_mdb_session_directory.clone();
        let merkledb_v2_session_clone = xet_config.merkledb_v2_session.clone();
        bench_processing_pool.spawn(async move {
            let _permit = bench_processing_permits.acquire_owned().await?;
            let ret_data = pft
                .clean_file_and_report_progress(&dataset_file_clone, src_data, &None)
                .await?;

            pft.finalize().await?;

            // Retain the shards for push later.
            for entry in fs::read_dir(bench_config_mdb_session_directory_clone)? {
                let shard_path = entry?.path();
                let new_path = merkledb_v2_session_clone
                    .join(shard_path.file_name().unwrap_or_default());
                fs::copy(shard_path, new_path)?;
            };
            Ok::<(PathBuf, Vec<u8>), GitXetRepoError>((dataset_file_clone, ret_data))
        });
        while let Some(res) = bench_processing_pool.try_join_next() {
            let (dataset_file, _new_data) = res??;
            info!("[xetbench] cleaning done on {:?}", dataset_file);
        }
    }
    while let Some(res) = bench_processing_pool.join_next().await {
        let (dataset_file, _new_data) = res??;
        info!("[xetbench] cleaning done on {:?}", dataset_file);
    }

    xet_repo.pre_push_hook("origin").await?; // push the shards

    Ok(())
}
