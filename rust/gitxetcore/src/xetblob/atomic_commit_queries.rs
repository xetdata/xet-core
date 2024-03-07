use super::retry_policy::is_status_retriable_and_print;
use retry_strategy::RetryStrategy;
use url::Url;

const NUM_RETRIES: usize = 5;
const BASE_RETRY_DELAY_MS: u64 = 500;
/// Internal method that performs a atomic query against remote
/// https://[domain]/api/xet/repos/[user]/[repo]/commits
/// remote_base_url is https://[domain]/[user]/[repo]
/// So we take the path and prepend /api/xet/repos
pub async fn perform_atomic_commit_query_internal(
    remote_base_url: Url,
    body: &str,
) -> anyhow::Result<reqwest::Response> {
    // derive the bbq url
    let base_path = remote_base_url.path();
    // note that base path begins with a /
    let commit_path = format!("/api/xet/repos{base_path}/commits");
    let mut commit_url = remote_base_url;
    commit_url.set_path(&commit_path);

    // build the query and ask for the contents
    let retry_strategy = RetryStrategy::new(NUM_RETRIES, BASE_RETRY_DELAY_MS);
    let body = body.to_string();
    let res = retry_strategy
        .retry(
            || async {
                let url = commit_url.clone();
                let client = reqwest::Client::new();
                let my_body /* is made of meat */ = body.clone();
                client
                    .post(url)
                    .header("Content-Type", "application/json")
                    .body(my_body)
                    .send()
                    .await
            },
            is_status_retriable_and_print,
        )
        .await?;
    Ok(res)
}

/// Internal method that performs an atomic query against remote
/// remote_base_url is https://domain/user/repo
pub async fn perform_atomic_commit_query(
    remote_base_url: Url,
    body: &str,
) -> anyhow::Result<Vec<u8>> {
    let response = perform_atomic_commit_query_internal(remote_base_url, body).await?;
    let response = response.error_for_status()?;
    let body = response.bytes().await?;
    Ok(body.to_vec())
}
