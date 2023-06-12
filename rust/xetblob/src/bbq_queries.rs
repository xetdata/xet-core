use crate::retry_policy::is_status_retriable_and_print;
use retry_strategy::RetryStrategy;
use url::Url;

const NUM_RETRIES: usize = 5;
const BASE_RETRY_DELAY_MS: u64 = 500;
/// Internal method that performs a BBQ query against remote
/// https://[domain]/api/xet/repos/[user]/[repo]/bbq/branch/{branch}/{path}
/// remote_base_url is https://[domain]/[user]/[repo]
/// So we take the path and prepend /api/xet/repos
pub async fn perform_bbq_query_internal(
    remote_base_url: Url,
    branch: &str,
    filename: &str,
    query_type: &str,
) -> anyhow::Result<reqwest::Response> {
    // derive the bbq url
    let base_path = remote_base_url.path();
    let bbq_path = if filename.is_empty() {
        format!("/api/xet/repos{base_path}/bbq/{query_type}/{branch}")
    } else {
        format!("/api/xet/repos{base_path}/bbq/{query_type}/{branch}/{filename}")
    };
    let mut bbq_url = remote_base_url;
    bbq_url.set_path(&bbq_path);

    // build the query and ask for the contents
    let retry_strategy = RetryStrategy::new(NUM_RETRIES, BASE_RETRY_DELAY_MS);
    let res = retry_strategy
        .retry(
            || async {
                let url = bbq_url.clone();
                reqwest::get(url).await
            },
            is_status_retriable_and_print,
        )
        .await?;
    Ok(res)
}

/// Internal method that performs a BBQ query against remote
/// remote_base_url is https://domain/user/repo
pub async fn perform_bbq_query(
    remote_base_url: Url,
    branch: &str,
    filename: &str,
) -> anyhow::Result<Vec<u8>> {
    let response = perform_bbq_query_internal(remote_base_url, branch, filename, "branch").await?;
    let response = response.error_for_status()?;
    let body = response.bytes().await?;
    Ok(body.to_vec())
}

/// Normalize git remote urls for the bbq query, stripping the .git if provided
pub fn git_remote_to_base_url(remote: &str) -> anyhow::Result<Url> {
    // trim the ".git" if its there
    if let Some(remote_stripped) = remote.strip_suffix(".git") {
        Ok(Url::parse(remote_stripped)?)
    } else {
        Ok(Url::parse(remote)?)
    }
}
