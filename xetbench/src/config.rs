use serde::Deserialize;
use std::{env, fs};

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub api_url: String,
    pub hf_token: String,
    pub limit: usize,
    pub dataset_large_cutoff: u64,
    pub large_dataset_names: Vec<String>,
    pub checkout_directory: String
}

impl Config {
    pub fn from_file(path: &str) -> Self {
        let config_content = fs::read_to_string(path)
            .expect("Failed to read configuration file");

        let mut config: Config = toml::from_str(&config_content)
            .expect("Failed to parse configuration file");
        config.hf_token = env::var("HF_TOKEN").expect("HF_TOKEN must be set in the environment");
        config
    }
}