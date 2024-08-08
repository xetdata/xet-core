use std::process::Command;
use anyhow::{Context, Result};
use serde_json::Value;

use crate::config::Config;
use crate::dataset::Dataset;

pub async fn identify_large_datasets(config: &Config) -> Result<Vec<Dataset>> {
    let client = reqwest::Client::new();
    let json_response = client.get(&config.api_url).bearer_auth(&config.hf_token).query(&[("sort", "downloads"), ("limit", &config.limit.to_string()), ("full", "true")]).send().await?.text().await?;

    let json_response_array: Vec<Value> = serde_json::from_str(&json_response)?;
    let mut large_datasets: Vec<Dataset> = vec![];

    for dataset in json_response_array.iter().take(config.limit) {
        if let Some(id) = dataset.get("id").and_then(Value::as_str) {
            if let Some(card_data) = dataset.get("cardData") {
                if let Some(dataset_info) = card_data.get("dataset_info") {
                    if let Some(download_size) = dataset_info.get("download_size") {
                        if let Some(download_size) = download_size.as_u64() {
                            if download_size > config.dataset_large_cutoff {
                                large_datasets.push(Dataset {
                                    id: id.to_string(),
                                    download_size,
                                });
                            }
                        }
                    }
                }
            }
        }
    }


    Ok(large_datasets)
}

/// Validates access to the Git repositories on Hugging Face using SSH.
///
/// Tests that these commands pass
/// `ssh -T git@hf.co`
/// `git-lfs --version`
pub fn validate_git_hf_access() -> Result<bool> {
    Ok(
        Command::new("ssh").arg("-T").arg("git@hf.co").output()?.status.success() &&
        Command::new("git-lfs").arg("--version").output()?.status.success()
    )
}