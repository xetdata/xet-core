use anyhow::Result;

use crate::config::Config;
use crate::hf_dataset::download_and_list_dataset_files_for_upload;

mod config;
mod hf_dataset;
mod dataset;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_file("config.toml");
    for dataset_file in download_and_list_dataset_files_for_upload(config.clone()).await? {
        println!("Name: {:?}", dataset_file);
    }

    Ok(())
}
