mod config;
mod hf_api_client;
mod dataset;

use anyhow::{anyhow, Result};
use reqwest::Error;
use crate::config::Config;
use crate::hf_api_client::{identify_large_datasets, validate_git_hf_access};




#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_file("config.toml");

    let large_datasets = identify_large_datasets(&config).await?;
    if !validate_git_hf_access()? {
        return Err(anyhow!("Please validate that you have ssh git access to hf"))
    }
    for dataset in large_datasets {
        println!("Name: {}", dataset.id);
        println!("Size: {:.3} GB", dataset.download_size as f64 / (1024.0 * 1024.0 * 1024.0));
        println!("{}", "-".repeat(40));
    }

    Ok(())
}
