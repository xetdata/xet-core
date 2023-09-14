use crate::retry_policy::is_status_retriable_and_print;
use anyhow::anyhow;
use retry_strategy::RetryStrategy;
use std::collections::HashMap;
use std::sync::Arc;
use std::env;
use std::path::Path;
use tokio::sync::Mutex;
use tracing::{debug, info};
use url::Url;

const NUM_RETRIES: usize = 5;
const BASE_RETRY_DELAY_MS: u64 = 500;
const CACHE_TIME_S: u64 = 30; // 30 seconds

#[derive(Clone)]
pub struct BbqClient {
    client: reqwest::Client,
    #[allow(clippy::type_complexity)]
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
                .duration_since(*insert_time)
                .as_secs()
                < CACHE_TIME_S
            {
                Some(val.clone())
            } else {
                cache.remove(request);
                None
            }
        } else {
            None
        }
    }

    async fn remove_cache(&self, request: &str) {
        self.cache.lock().await.remove(request);
    }

    #[allow(clippy::new_without_default)]
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
        info!("Querying {}", bbq_path);
        bbq_url.set_path(&bbq_path);

        // build the query and ask for the contents
        let retry_strategy = RetryStrategy::new(NUM_RETRIES, BASE_RETRY_DELAY_MS);
        let res = retry_strategy
            .retry(
                || async {
                    let url = bbq_url.clone();
                    self.client.get(url).header("User-Agent", detect_downstream_client()).send().await
                },
                is_status_retriable_and_print,
            )
            .await?;
        Ok(res)
    }

    /// Internal method that performs an arbitrary api query against remote
    /// https://[domain]/api/xet/repos/[user]/[repo]/[op]
    /// remote_base_url is https://[domain]/[user]/[repo]
    /// So we take the path and prepend /api/xet/repos
    ///
    /// query_type is a HTTP method as is one of 'get','post','patch','delete','put'
    pub async fn perform_api_query_internal(
        &self,
        remote_base_url: Url,
        op: &str,
        http_command: &str,
        body: &str,
    ) -> anyhow::Result<reqwest::Response> {
        let base_path = remote_base_url.path();
        let api_path = format!("/api/xet/repos{base_path}/{op}");
        let api_path = api_path.trim_end_matches('/');
        let mut api_url = remote_base_url;
        info!("Querying {}", api_path);
        api_url.set_path(api_path);
        if http_command != "get"
            && http_command != "post"
            && http_command != "patch"
            && http_command != "put"
            && http_command != "delete"
        {
            return Err(anyhow!("Invalid http method"));
        }

        // build the query and ask for the contents
        let retry_strategy = RetryStrategy::new(NUM_RETRIES, BASE_RETRY_DELAY_MS);
        let res = retry_strategy
            .retry(
                || async {
                    let url = api_url.clone();
                    let client = match http_command {
                        "get" => self.client.get(url),
                        "post" => self.client.post(url),
                        "patch" => self.client.patch(url),
                        "put" => self.client.put(url),
                        "delete" => self.client.delete(url),
                        _ => self.client.get(url),
                    };
                    let client = if !body.is_empty() {
                        client
                            .header("Content-Type", "application/json")
                            .header("User-Agent", detect_downstream_client())
                            .body(body.to_string())
                    } else {
                        client
                    };

                    client.send().await
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
        let cache_key = format!("branch: {}/{}/{}", remote_base_url, branch, filename);
        debug!("bbq cache_key: {cache_key}");
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

    pub async fn invalidate_cache(&self, remote_base_url: Url, branch: &str, filename: &str) {
        // invalidate self and ancestors
        let mut path = filename;
        loop {
            let cache_key = format!("branch: {}/{}/{}", remote_base_url, branch, path);
            self.remove_cache(&cache_key).await;

            if path.is_empty() {
                break;
            }

            let parent_path = match std::path::Path::new(path).parent() {
                Some(p) => p.to_str().unwrap_or_default(),
                None => "",
            };

            path = parent_path;
        }
    }

    /// Internal method that performs a Stat query against remote
    /// remote_base_url is https://domain/user/repo
    /// Returns Ok(None) on "file not found"
    /// Returns Ok(body) on if file exists
    /// Returns Errors on any other error
    ///
    /// Only positive responses are cached
    pub async fn perform_stat_query(
        &self,
        remote_base_url: Url,
        branch: &str,
        filename: &str,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let cache_key = format!("stat: {}/{}/{}", remote_base_url, branch, filename);
        if let Some(res) = self.get_cache(&cache_key).await {
            return Ok(Some(res));
        }
        let response = self
            .perform_bbq_query_internal(remote_base_url, branch, filename, "stat")
            .await?;
        if matches!(response.status(), reqwest::StatusCode::NOT_FOUND) {
            return Ok(None);
        }
        let response = response.error_for_status()?;
        let body = response.bytes().await?;
        let body = body.to_vec();
        self.put_cache(&cache_key, body.clone()).await;
        Ok(Some(body))
    }

    /// Internal method that performs an arbitrary API query against remote
    /// remote_base_url is https://domain/
    pub async fn perform_api_query(
        &self,
        remote_base_url: Url,
        op: &str,
        http_command: &str,
        body: &str,
    ) -> anyhow::Result<Vec<u8>> {
        let response = self
            .perform_api_query_internal(remote_base_url, op, http_command, body)
            .await?;
        let response = response.error_for_status()?;
        let body = response.bytes().await?;
        let body = body.to_vec();
        Ok(body)
    }
}

/// Detect the downstream client from the program arguments. If any program arguments contains the
/// xet binary (detected via "xet" binary path in the 2nd arg) then it is detected as xet-cli, otherwise
/// defaults to pyxet.
fn detect_downstream_client() -> String {
    let is_xet_cli = env::args_os().nth(1).and_then(|x| {
        let path = Path::new(&x);
        Some(path.ends_with("xet") & path.is_file())
    }).unwrap_or(false);
    return if is_xet_cli {
        "xet-cli".to_string()
    } else {
        "pyxet".to_string()
    };
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
