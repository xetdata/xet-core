use std::{env, fs};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;
use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use crate::xet_bench_config::XetBenchConfig;


struct Dataset {
    pub id: String,
    pub download_size: u64,
}

async fn identify_large_datasets(config: &XetBenchConfig) -> Result<Vec<Dataset>> {
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
fn validate_git_hf_access() -> Result<bool> {
    Ok(
        Command::new("ssh").arg("-T").arg("git@hf.co").output()?.status.success() &&
        Command::new("git-lfs").arg("--version").output()?.status.success()
    )
}

fn download_large_datasets(large_dataset_names: &Vec<String>, checkout_directory: &str) -> Result<()> {
        for dataset_name in large_dataset_names {
        let subdirectory_path = Path::new(&checkout_directory).join(dataset_name);
        if subdirectory_path.exists() {
            println!("Directory {:?} already exists, skipping...", subdirectory_path);
            continue;
        }

        fs::create_dir_all(&subdirectory_path).with_context(|| {
            format!(
                "Failed to create subdirectory for dataset: {}",
                dataset_name
            )
        })?;

        env::set_current_dir(&subdirectory_path).with_context(|| {
            format!(
                "Failed to change directory to: {:?}",
                subdirectory_path
            )
        })?;
        println!("Starting clone of repository for dataset: {}", dataset_name);
        let git_url = format!("git@hf.co:datasets/{}", dataset_name);
        let clone_output = Command::new("git")
            .arg("clone")
            .arg(git_url)
            .output()
            .with_context(|| format!("Failed to clone repository for dataset: {}", dataset_name))?;

        if clone_output.status.success() {
            println!("Successfully cloned repository for dataset: {}", dataset_name);
        } else {
            eprintln!(
                "Failed to clone repository for dataset: {}\nstderr: {}",
                dataset_name,
                String::from_utf8_lossy(&clone_output.stderr)
            );
        }
    }

    Ok(())
}

fn list_dataset_files(checkout_directory: &str) -> Result<Vec<PathBuf>> {
    list_files(Path::new(checkout_directory))
}

fn invalid_file_filter(file_name: &OsStr) -> bool {
    let name = file_name.to_string_lossy();
    !name.contains("parquet")
    // name.starts_with(".git") || name.contains(".DS_Store")
}
fn list_files(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut dataset_files = Vec::new();

    // Recursively scan the directory
    for entry in fs::read_dir(directory).with_context(|| format!("Failed to read directory: {:?}", directory))? {
        let entry = entry?;
        let path = entry.path();

        // Check if the path is a directory
        if path.is_dir() {
            // Skip the .git directory
            if path.ends_with(".git") {
                continue;
            }
            // Recursively scan subdirectories
            let mut sub_files = list_files(&path)?;
            dataset_files.append(&mut sub_files);
        } else {
            // Add files that are not .git-related
            if !path.file_name().map_or(false, |name| invalid_file_filter(name)) {
                dataset_files.push(path);
            }
        }
    }

    Ok(dataset_files)
}

pub async fn download_and_list_dataset_files_for_upload(config: XetBenchConfig) -> Result<Vec<PathBuf>> {
    let mut large_dataset_names: Vec<String> = vec![];
    if config.large_dataset_names.is_empty() {
        let large_datasets = identify_large_datasets(&config).await?;
        for dataset in large_datasets {
            let dataset_name = dataset.id.clone();
            large_dataset_names.push(dataset_name);
            println!("Name: {}", dataset.id);
            println!("Size: {:.3} GB", dataset.download_size as f64 / (1024.0 * 1024.0 * 1024.0));
            println!("{}", "-".repeat(40));
        }
    } else {
        large_dataset_names = config.large_dataset_names;
    }
    if !validate_git_hf_access()? {
        return Err(anyhow!("Please validate that you have ssh git access to hf"));
    }
    download_large_datasets(&large_dataset_names, &config.checkout_directory)?;
    list_dataset_files(&config.checkout_directory)
}