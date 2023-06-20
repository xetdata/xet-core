use cas::constants::*;
use std::time::Duration;

use crate::{
    cas_connection_pool::CasConnectionConfig,
    grpc::{get_request_id, trace_forwarding},
    remote_client::CAS_PROTOCOL_VERSION,
};
use anyhow::{anyhow, Result};
use http::{Method, Request, Version};
use hyper::{
    client::HttpConnector,
    header::RANGE,
    header::{HeaderMap, HeaderName, HeaderValue},
    Body, Client,
};
use opentelemetry::propagation::{Injector, TextMapPropagator};
use retry_strategy::RetryStrategy;
use thiserror::Error;
use tracing::{debug, error, info_span, warn, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use merklehash::MerkleHash;

const HTTP2_POOL_IDLE_TIMEOUT_SECS: u64 = 30;
const HTTP2_KEEPALIVE_MILLIS: u64 = 500;
const HTTP2_WINDOW_SIZE: u32 = 2147418112;
const NUM_RETRIES: usize = 5;
const BASE_RETRY_DELAY_MS: u64 = 3000;

pub struct DataTransport {
    http2_client: Client<HttpConnector>,
    retry_strategy: RetryStrategy,
    cas_connection_config: CasConnectionConfig,
}

impl std::fmt::Debug for DataTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataTransport")
            .field("authority", &self.authority())
            .finish()
    }
}

/// This struct is used to wrap the error types which we may
/// retry on. Request Errors (which are triggered if there was a
/// header error building the request) are not retryable.
/// Right now this retries every h2 error. Reading these:
///  - https://docs.rs/h2/latest/h2/struct.Error.html,
///  - https://docs.rs/h2/latest/h2/struct.Reason.html
/// unclear if there is any reason not to retry.
#[derive(Error, Debug)]
enum RetryError {
    #[error("{0}")]
    Hyper(#[from] hyper::Error),

    #[error("Request Error: {0}")]
    Request(#[from] anyhow::Error),

    /// Should only be used for non-success errors
    #[error("Status Error: {0}")]
    Status(http::StatusCode),
}
fn is_status_retriable(err: &RetryError) -> bool {
    match err {
        RetryError::Hyper(_) => true,
        RetryError::Request(_) => false,
        RetryError::Status(n) => retry_http_status_code(n),
    }
}
fn retry_http_status_code(stat: &http::StatusCode) -> bool {
    stat.is_server_error() || *stat == http::StatusCode::TOO_MANY_REQUESTS
}

fn is_status_retriable_and_print(err: &RetryError) -> bool {
    let ret = is_status_retriable(err);
    if ret {
        warn!("{}. Retrying...", err);
    }
    ret
}
fn print_final_retry_error(err: RetryError) -> RetryError {
    if is_status_retriable(&err) {
        error!("Too many failures {}", err);
    }
    err
}

impl DataTransport {
    pub fn new(
        http2_client: Client<HttpConnector>,
        retry_strategy: RetryStrategy,
        cas_connection_config: CasConnectionConfig,
    ) -> Self {
        Self {
            http2_client,
            retry_strategy,
            cas_connection_config,
        }
    }

    /// creates the DataTransport instance for the H2 connection using
    /// CasConnectionConfig info, and additional port
    pub async fn from_config(cas_connection_config: CasConnectionConfig) -> Result<Self> {
        debug!(
            "Attempting to make HTTP connection with {}",
            cas_connection_config.endpoint
        );
        let h2_client = Client::builder()
            .pool_idle_timeout(Duration::from_secs(HTTP2_POOL_IDLE_TIMEOUT_SECS))
            .http2_keep_alive_interval(Duration::from_millis(HTTP2_KEEPALIVE_MILLIS))
            .http2_initial_connection_window_size(HTTP2_WINDOW_SIZE)
            .http2_initial_stream_window_size(HTTP2_WINDOW_SIZE)
            .http2_only(true)
            .build_http();

        let retry_strategy = RetryStrategy::new(NUM_RETRIES, BASE_RETRY_DELAY_MS);
        Ok(Self::new(h2_client, retry_strategy, cas_connection_config))
    }

    fn authority(&self) -> &str {
        self.cas_connection_config.endpoint.as_str()
    }

    fn get_uri(&self, prefix: &str, hash: &MerkleHash) -> String {
        let cas_key_string = cas::key::Key {
            prefix: prefix.to_string(),
            hash: *hash,
        }
        .to_string();
        if cas_key_string.starts_with('/') {
            format!("{}{}", self.authority(), cas_key_string)
        } else {
            format!("{}/{}", self.authority(), cas_key_string)
        }
    }

    fn setup_request(
        &self,
        method: Method,
        prefix: &str,
        hash: &MerkleHash,
        body: Option<Vec<u8>>,
    ) -> Result<Request<Body>> {
        let dest = self.get_uri(prefix, hash);
        debug!("Calling {} with address: {}", method, dest);
        let user_id_header = HeaderName::from_static(USER_ID_HEADER);
        let user_id = self.cas_connection_config.user_id.clone();
        let request_id_header = HeaderName::from_static(REQUEST_ID_HEADER);
        let request_id = get_request_id();
        let repo_path_header = HeaderName::from_static(REPO_PATHS_HEADER);
        let repo_paths = self.cas_connection_config.repo_paths.clone();
        let git_xet_version_header = HeaderName::from_static(GIT_XET_VERSION_HEADER);
        let git_xet_version = self.cas_connection_config.git_xet_version.clone();
        let cas_protocol_version_header = HeaderName::from_static(CAS_PROTOCOL_VERSION_HEADER);
        let cas_protocol_version = CAS_PROTOCOL_VERSION.clone();

        let mut req = Request::builder()
            .method(method)
            .header(user_id_header, user_id)
            .header(request_id_header, request_id)
            .header(repo_path_header, repo_paths)
            .header(git_xet_version_header, git_xet_version)
            .header(cas_protocol_version_header, cas_protocol_version)
            .uri(&dest)
            .version(Version::HTTP_2);
        if trace_forwarding() {
            if let Some(headers) = req.headers_mut() {
                let mut injector = HeaderInjector(headers);
                let propagator = opentelemetry_jaeger::Propagator::new();
                let cur_span = Span::current();
                let ctx = cur_span.context();
                propagator.inject_context(&ctx, &mut injector);
            }
        }
        let req = if let Some(data) = body {
            req.body(Body::from(data))?
        } else {
            req.body(Body::empty())?
        };
        Ok(req)
    }

    // Single get to the H2 server
    pub async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>> {
        let resp = self
            .retry_strategy
            .retry(
                || async {
                    let req = self
                        .setup_request(Method::GET, prefix, hash, None)
                        .map_err(RetryError::from)?;

                    let resp = self
                        .http2_client
                        .request(req)
                        .instrument(info_span!("transport.h2_get"))
                        .await
                        .map_err(RetryError::from)?;

                    if retry_http_status_code(&resp.status()) {
                        return Err(RetryError::Status(resp.status()));
                    }
                    Ok(resp)
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(print_final_retry_error)?;
        let status = resp.status();
        if status != http::StatusCode::OK {
            return Err(anyhow!(
                "data get status {} received for URL {}",
                status,
                self.get_uri(prefix, hash)
            ));
        }
        debug!("Received Response from HTTP2 GET: {}", status);
        // Get the body
        let bytes = hyper::body::to_bytes(resp)
            .instrument(info_span!("transport.read_body"))
            .await?;
        Ok(bytes.to_vec())
    }

    // Single get range to the H2 server
    pub async fn get_range(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        range: (u64, u64),
    ) -> Result<Vec<u8>> {
        let res = self
            .retry_strategy
            .retry(
                || async {
                    let mut req = self
                        .setup_request(Method::GET, prefix, hash, None)
                        .map_err(RetryError::from)?;
                    let header_value =
                        HeaderValue::from_str(&format!("bytes={}-{}", range.0, range.1 - 1))
                            .map_err(anyhow::Error::from)
                            .map_err(RetryError::from)?;
                    req.headers_mut().insert(RANGE, header_value);

                    let resp = self
                        .http2_client
                        .request(req)
                        .instrument(info_span!("transport.h2_get_range"))
                        .await
                        .map_err(RetryError::from)?;

                    if retry_http_status_code(&resp.status()) {
                        return Err(RetryError::Status(resp.status()));
                    }

                    let status = resp.status();
                    if status != http::StatusCode::OK {
                        return Err(RetryError::Request(anyhow!(
                            "data get_range status {} received for URL {} with range {:?}",
                            status,
                            self.get_uri(prefix, hash),
                            range
                        )));
                    }
                    debug!("Received Response from HTTP2 GET range: {}", status);
                    // Get the body
                    let bytes = hyper::body::to_bytes(resp)
                        .instrument(info_span!("transport.read_body"))
                        .await?;
                    Ok(bytes.to_vec())
                },
                is_status_retriable_and_print,
            )
            .await;

        res.map_err(print_final_retry_error)
            .map_err(anyhow::Error::from)
    }

    // Single put to the H2 server
    pub async fn put(&self, prefix: &str, hash: &MerkleHash, data: &[u8]) -> Result<()> {
        let resp = self
            .retry_strategy
            .retry(
                || async {
                    let req = self
                        .setup_request(Method::POST, prefix, hash, Some(data.to_owned()))
                        .map_err(RetryError::from)?;

                    let resp = self
                        .http2_client
                        .request(req)
                        .instrument(info_span!("transport.h2_put"))
                        .await
                        .map_err(RetryError::from)?;

                    if retry_http_status_code(&resp.status()) {
                        return Err(RetryError::Status(resp.status()));
                    }
                    Ok(resp)
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(print_final_retry_error)?;
        let status = resp.status();
        if status != http::StatusCode::OK {
            return Err(anyhow!(
                "data put status {} received for URL {}",
                status,
                self.get_uri(prefix, hash),
            ));
        }
        debug!("Received Response from HTTP2 POST: {}", status);

        Ok(())
    }
}
pub struct HeaderInjector<'a>(pub &'a mut HeaderMap<HeaderValue>);

impl<'a> Injector for HeaderInjector<'a> {
    /// Set a key and value in the HeaderMap.  Does nothing if the key or value are not valid inputs.
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key_header) = HeaderName::try_from(key) {
            if let Ok(header_value) = HeaderValue::from_str(&value) {
                self.0.insert(key_header, header_value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    #[tokio::test]
    async fn test_from_config() {
        let endpoint = "http://localhost:443";
        let config = CasConnectionConfig {
            endpoint: endpoint.to_string(),
            user_id: "user".to_string(),
            repo_paths: "repo".to_string(),
            git_xet_version: "0.1.0".to_string(),
        };
        let dt = DataTransport::from_config(config).await.unwrap();
        assert_eq!(dt.authority(), endpoint);
    }

    #[tokio::test]
    async fn repo_path_header_test() {
        let data: Vec<Vec<String>> = vec![
            vec!["user1/repo-😀".to_string(), "user1/répô_123".to_string()],
            vec![
                "user2/👾_repo".to_string(),
                "user2/üникод".to_string(),
                "user2/foobar!@#".to_string(),
            ],
            vec!["user3/sømè_repo".to_string(), "user3/你好-世界".to_string()],
            vec!["user4/✨🌈repo".to_string()],
            vec!["user5/Ω≈ç√repo".to_string()],
            vec!["user6/42°_repo".to_string()],
            vec![
                "user7/äëïöü_repo".to_string(),
                "user7/ĀāĒēĪīŌōŪū".to_string(),
            ],
        ];
        for inner_vec in data {
            let config = CasConnectionConfig::new(
                "".to_string(),
                "".to_string(),
                inner_vec.clone(),
                "".to_string(),
            );
            let client = DataTransport::from_config(config).await.unwrap();
            let hello = "hello".as_bytes().to_vec();
            let hash = merklehash::compute_data_hash(&hello[..]);
            let req = client.setup_request(Method::GET, "", &hash, None).unwrap();
            let repo_paths = req.headers().get(REPO_PATHS_HEADER).unwrap();
            let repo_path_str = String::from_utf8(repo_paths.as_bytes().to_vec()).unwrap();
            let vec_of_strings: Vec<String> =
                serde_json::from_str(repo_path_str.as_str()).expect("Failed to deserialize JSON");
            assert_eq!(vec_of_strings, inner_vec);
        }
    }

    #[tokio::test]
    async fn string_headers_test() {
        let user_id = "XET USER";
        let git_xet_version = "0.1.0";

        let cas_connection_config = CasConnectionConfig::new(
            "".to_string(),
            user_id.to_string(),
            vec![],
            git_xet_version.to_string(),
        );
        let client = DataTransport::from_config(cas_connection_config)
            .await
            .unwrap();
        let hash = merklehash::compute_data_hash("test".as_bytes());
        let req = client
            .setup_request(Method::POST, "default", &hash, None)
            .unwrap();
        let headers = req.headers();
        // gets header value assuming all well, panic if not
        let get_header_value = |header: &str| headers.get(header).unwrap().to_str().unwrap();

        // check against values in config
        assert_eq!(get_header_value(GIT_XET_VERSION_HEADER), git_xet_version);
        assert_eq!(get_header_value(USER_ID_HEADER), user_id);

        // check against global static
        assert_eq!(
            get_header_value(CAS_PROTOCOL_VERSION_HEADER),
            CAS_PROTOCOL_VERSION.as_str()
        );
    }
}
