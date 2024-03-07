mod atomic_commit_queries;
mod bbq_queries;
mod dir_entry;
mod file_open_flags;
mod file_operations;
mod retry_policy;
mod rfile_object;
mod wfile_object;
mod xet_repo;
mod xet_repo_manager;

use anyhow::anyhow;
use bbq_queries::BbqClient;
use serde::{Deserialize, Serialize};
use tracing::debug;
use url::Url;

pub use dir_entry::DirEntry;
pub use file_open_flags::*;
pub use file_operations::*;
pub use rfile_object::XetRFileObject;
pub use wfile_object::XetWFileObject;
pub use xet_repo::XetRepo;
pub use xet_repo::XetRepoWriteTransaction;
pub use xet_repo_manager::XetRepoManager;

#[derive(Serialize, Deserialize, Debug)]
pub struct AuxRepoInfo {
    pub html_url: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct XetRepoInfo {
    pub mdb_version: String,
    pub repo_salt: Option<String>,
}

/// this is the JSON structure returned by the xetea repo info function,
/// explicitly ignoring part of the "repo" section because unneeded.
#[derive(Serialize, Deserialize, Debug)]
pub struct RepoInfo {
    pub repo: AuxRepoInfo,
    pub xet: XetRepoInfo,
}

pub async fn get_repo_info(
    url: &Url,
    bbq_client: &BbqClient,
) -> anyhow::Result<(RepoInfo, Vec<u8>)> {
    let response = bbq_client.perform_api_query(url, "", "get", "").await?;
    let res_str = String::from_utf8(response.clone())?;
    debug!("{res_str:?}");
    Ok((serde_json::de::from_slice(&response)?, response))
}
