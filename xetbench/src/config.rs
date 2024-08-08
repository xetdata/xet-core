use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub api_url: String,
    pub hf_token: String,
    pub limit: usize,
    pub dataset_large_cutoff: u64,
}

impl Config {
    pub fn from_file(path: &str) -> Self {
        let config_content = fs::read_to_string(path)
            .expect("Failed to read configuration file");

        toml::from_str(&config_content)
            .expect("Failed to parse configuration file")
    }
}