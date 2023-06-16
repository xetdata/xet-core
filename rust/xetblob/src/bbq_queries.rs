use crate::retry_policy::is_status_retriable_and_print;
use retry_strategy::RetryStrategy;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

const NUM_RETRIES: usize = 5;
const BASE_RETRY_DELAY_MS: u64 = 500;
const CACHE_TIME_S: u64 = 30; // 30 seconds

#[derive(Clone)]
pub struct BbqClient {
    client: reqwest::Client,
    cache: Arc<Mutex<HashMap<String, (std::time::Instant, Vec<u8>)>>>,
}

impl BbqClient {
    async fn put_cache(&self, request: &str, response_body: Vec<u8>) {
        self.cache.lock().await.insert(
            request.to_string(),
            (std::time::Instant::now(), response_body),
        );
    }

    async fn get_cache(&self, request: &str) -> Option<Vec<u8>> {
        let mut cache = self.cache.lock().await;
        if let Some((insert_time, val)) = cache.get(request) {
            if std::time::Instant::now()
                .duration_since(insert_time.clone())
                .as_secs()
                < CACHE_TIME_S
            {
                Some(val.clone())
            } else {
                drop(insert_time);
                drop(val);
                cache.remove(request);
                None
            }
        } else {
            None
        }
    }
    pub fn new() -> BbqClient {
        BbqClient {
            client: reqwest::Client::new(),
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    /// Internal method that performs a BBQ query against remote
    /// https://[domain]/api/xet/repos/[user]/[repo]/bbq/branch/{branch}/{path}
    /// remote_base_url is https://[domain]/[user]/[repo]
    /// So we take the path and prepend /api/xet/repos
    pub async fn perform_bbq_query_internal(
        &self,
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
                    self.client.get(url).send().await
                },
                is_status_retriable_and_print,
            )
            .await?;
        Ok(res)
    }

    /// Internal method that performs a BBQ query against remote
    /// remote_base_url is https://domain/user/repo
    pub async fn perform_bbq_query(
        &self,
        remote_base_url: Url,
        branch: &str,
        filename: &str,
    ) -> anyhow::Result<Vec<u8>> {
        let cache_key = format!("{}/{}/{}", remote_base_url, branch, filename);
        if let Some(res) = self.get_cache(&cache_key).await {
            return Ok(res);
        }
        let response = self
            .perform_bbq_query_internal(remote_base_url, branch, filename, "branch")
            .await?;
        let response = response.error_for_status()?;
        let body = response.bytes().await?;
        let body = body.to_vec();
        self.put_cache(&cache_key, body.clone()).await;
        Ok(body)
    }
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
