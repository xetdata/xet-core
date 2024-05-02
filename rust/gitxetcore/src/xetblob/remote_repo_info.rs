use super::bbq_queries::git_remote_to_base_url;
use super::BbqClient;
use crate::config::XetConfig;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use tracing::debug;
use url::Url;

#[derive(Serialize, Deserialize, Debug)]
pub struct AuxRepoInfo {
    pub html_url: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct XetRepoInfo {
    pub cas: String,
    pub mdb_version: String,
    pub repo_salt: Option<String>,
}

/// This is the JSON structure returned by the xetea repo info function,
/// explicitly ignoring part of the "repo" section because unneeded.
#[derive(Serialize, Deserialize, Debug)]
pub struct RepoInfo {
    pub repo: AuxRepoInfo,
    pub xet: XetRepoInfo,
}

/// Retrieve repository information from Xetea at endpoint
/// http[s]://<domain>/api/xet/repos/<user>/<repo>.
/// Return the deserialized struct and the raw response.
pub async fn get_repo_info(
    url: &Url,
    bbq_client: &BbqClient,
) -> anyhow::Result<(RepoInfo, Vec<u8>)> {
    let response = bbq_client.perform_api_query(url, "", "get", "").await?;
    let res_str = String::from_utf8(response.clone())?;
    debug!("{res_str:?}");
    Ok((serde_json::de::from_slice(&response)?, response))
}

/// Retrieve CAS endpoint with respect to a repository url.
pub async fn get_cas_endpoint_from_git_remote(
    remote: &str,
    config: &XetConfig,
) -> anyhow::Result<String> {
    let remote = config.build_authenticated_remote_url(&remote);
    let url = git_remote_to_base_url(&remote)?;
    let bbq_client = BbqClient::new().map_err(|_| anyhow!("Unable to create network client."))?;
    get_repo_info(&url, &bbq_client)
        .await
        .map(|(info, _)| info.xet.cas)
}
