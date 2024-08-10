use anyhow::Result;
use glob::glob;
use std::process;

mod hf_dataset;
mod utils;
mod xet_bench_config;
mod xet_upload;
use hf_dataset::download_and_list_dataset_files_for_upload;
use xet_bench_config::XetBenchConfig;
use crate::xet_upload::upload_files;


#[tokio::main]
async fn main() -> Result<()> {
    let pattern = "**/xetbench.toml";
    if let Some(config_path)  = glob(pattern)?.filter_map(|entry| entry.ok()).find(|path| path.is_file()){
        let xet_bench_config = XetBenchConfig::from_file(config_path.to_str().unwrap());
        let dataset_files = download_and_list_dataset_files_for_upload(xet_bench_config.clone()).await?;
        upload_files(&xet_bench_config, &dataset_files).await?;

    } else {
        eprintln!("Unable to find xetbench.toml in the directory");
        process::exit(1);

    }

    Ok(())
}
