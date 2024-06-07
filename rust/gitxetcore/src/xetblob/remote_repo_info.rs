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

/// This is the JSON structure returned by the xetea cas query.
#[derive(Serialize, Deserialize, Debug)]
pub struct CasQueryResponse {
    pub cas: String,
}

pub async fn get_cas_endpoint(url: &Url, bbq_client: &BbqClient) -> anyhow::Result<String> {
    let response = bbq_client.perform_cas_query(url).await?;
    let cas_response: CasQueryResponse = serde_json::de::from_slice(&response)?;
    Ok(cas_response.cas)
}

/// Retrieve CAS endpoint with respect to a repository url.
#[allow(unreachable_code)]
#[allow(unused_variables)] // only to avoid warnings in test build
pub async fn get_cas_endpoint_from_git_remote(
    remote: &str,
    config: &XetConfig,
) -> anyhow::Result<String> {
    #[cfg(test)]
    {
        return Ok(xet_config::PROD_CAS_ENDPOINT.to_owned());
    }

    let bbq_client = BbqClient::new().map_err(|_| anyhow!("Unable to create network client."))?;

    // first try the cas endpoint query route that doesn't need auth
    let url = git_remote_to_base_url(&remote)?;
    if let Ok(cas) = get_cas_endpoint(&url, &bbq_client).await {
        return Ok(cas);
    }

    // on failure try the repo info query route with auth
    let remote = config.build_authenticated_remote_url(remote);
    let url = git_remote_to_base_url(&remote)?;
    get_repo_info(&url, &bbq_client)
        .await
        .map(|(info, _)| info.xet.cas)
}
