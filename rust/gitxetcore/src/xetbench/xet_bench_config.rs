use serde::Deserialize;
use std::{env, fs};
use crate::utils::expand_tilde;

#[derive(Clone, Debug, Deserialize)]
pub struct XetBenchConfig {
    pub api_url: String,
    pub hf_token: String,
    pub limit: usize,
    pub dataset_large_cutoff: u64,
    pub large_dataset_names: Vec<String>,
    pub checkout_directory: String,
    pub xet_clone_repo_directory: String,
    pub xet_clone_repo_url: String
}

impl XetBenchConfig {
    pub fn from_file(path: &str) -> Self {
        let config_content = fs::read_to_string(path)
            .expect("Failed to read configuration file");

        let mut config: XetBenchConfig = toml::from_str(&config_content)
            .expect("Failed to parse configuration file");
        config.checkout_directory = expand_tilde(&config.checkout_directory).to_str().unwrap().parse().unwrap();
        config.xet_clone_repo_directory = expand_tilde(&config.xet_clone_repo_directory).to_str().unwrap().parse().unwrap();
        config.hf_token = env::var("HF_TOKEN").expect("HF_TOKEN must be set in the environment");
        config.xet_clone_repo_url = env::var("XET_CLONE_REPO_URL").expect("XET_CLONE_REPO_URL must be set in the environment");
        config
    }
}