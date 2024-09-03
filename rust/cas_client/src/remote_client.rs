use anyhow::anyhow;
use bytes::Buf;
use cas::key::Key;
use cas::singleflight;
use cas_types::compression_scheme::CompressionScheme;
use cas_types::data_ops::PostResponse;
use cas_types::shard_ops::{
    QueryChunkParams, QueryChunkResponse, QueryFileParams, QueryFileResponse, SyncShardParams,
    SyncShardResponse, SyncShardResponseType,
};
use dsfs::log;
use error_printer::ErrorPrinter;
use futures::lock::Mutex;
use lazy_static::lazy_static;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::Serialize;

use merklehash::MerkleHash;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use url::Url;

use crate::error::{CasClientError, Result};
use crate::Client;
use retry_strategy::RetryStrategy;

pub const CAS_ENDPOINT: &str = "localhost:4884";
// pub const CAS_ENDPOINT: &str = "localhost:443";
pub const SCHEME: &str = "http";

/// cas protocol version as seen from the client
/// cas protocol determines the parameters and protocols used for
/// cas operations
///
/// 0.1.0:
///     grpc initiate; port 443; scheme https
///     h2 get + h2 put; port 443; scheme http; host from initiate rpc response
///     grpc put_complete; port 5000; scheme http; host from initiate rpc response
/// 0.2.0:
///     grpc initiate; port 443; scheme https
///     h2 get + h2 put; port, host, and scheme from initiate rpc response
///     grpc put_complete; port, host, and scheme from initiate rpc response
///     defaults to 0.1.0 if initiate does not respond with required info
/// 0.3.0:
///     grpc initiate; port 443; scheme https; widely trusted certificate
///     h2 get + h2 put; port, host and scheme from initiate rpc response; includes custom root certificate authority
///     grpc put_complete; port, host and scheme from initiate rpc response; includes custom root certificate authority
///     defaults to 0.2.0 if initiate does not respond with correct info
const _CAS_PROTOCOL_VERSION: &str = "0.3.0";

lazy_static! {
    pub static ref CAS_PROTOCOL_VERSION: String =
        std::env::var("XET_CAS_PROTOCOL_VERSION").unwrap_or(_CAS_PROTOCOL_VERSION.to_string());
}

// Completely arbitrary CAS size for using a single-hit put call.
// This should be tuned after performance testing.
const _MINIMUM_DATA_TRANSPORT_UPLOAD_SIZE: usize = 500;

const PUT_MAX_RETRIES: usize = 3;
const PUT_RETRY_DELAY_MS: u64 = 1000;

// We have different pool sizes since the GRPC connections are shorter-lived and
// thus, not as many of them are needed. This helps reduce the impact that connection
// creation can have (which, on MacOS, can be significant (hundreds of ms)).
const H2_TRANSPORT_POOL_SIZE: usize = 16;

/// CAS Remote client. This negotiates between the control plane (gRPC)
/// and data plane (HTTP) to optimize the uploads and fetches according to
/// the network, file size, and other dynamic qualities.
#[derive(Debug)]
pub struct RemoteClient {
    lb_endpoint: String,
    user_id: String,
    auth: String,
    repo_paths: Vec<String>,
    length_singleflight: singleflight::Group<u64, CasClientError>,
    length_cache: Arc<Mutex<HashMap<String, u64>>>,
    git_xet_version: String,
    client: DSCASAPIClient,
}

impl Default for RemoteClient {
    fn default() -> Self {
        Self {
            lb_endpoint: Default::default(),
            user_id: Default::default(),
            auth: Default::default(),
            repo_paths: Default::default(),
            length_singleflight: singleflight::Group::new(),
            length_cache: Default::default(),
            git_xet_version: Default::default(),
            client: Default::default(),
        }
    }
}

// DTO's for organization moving around endpoint info
#[derive(Clone)]
struct InitiateResponseEndpointInfo {
    endpoint: String,
    root_ca: String,
}

#[derive(Clone)]
struct InitiateResponseEndpoints {
    h2: InitiateResponseEndpointInfo,
    put_complete: InitiateResponseEndpointInfo,
    accepted_encodings: Vec<CompressionScheme>,
}

impl RemoteClient {
    pub fn new(
        lb_endpoint: String,
        user_id: String,
        auth: String,
        repo_paths: Vec<String>,
        git_xet_version: String,
    ) -> Self {
        Self {
            lb_endpoint,
            user_id,
            auth,
            repo_paths,
            length_singleflight: singleflight::Group::new(),
            length_cache: Arc::new(Mutex::new(HashMap::new())),
            git_xet_version,
            client: DSCASAPIClient::new(),
        }
    }

    pub async fn from_config(
        endpoint: &str,
        user_id: &str,
        auth: &str,
        repo_paths: Vec<String>,
        git_xet_version: String,
    ) -> Self {
        // optionally switch between a CAS and a local server running on CAS_GRPC_PORT and
        // CAS_HTTP_PORT
        Self::new(
            endpoint.to_string(),
            String::from(user_id),
            String::from(auth),
            repo_paths,
            git_xet_version,
        )
    }
    /*
       async fn put_impl_h2(
           &self,
           prefix: &str,
           hash: &MerkleHash,
           data: &[u8],
           chunk_boundaries: &[u64],
       ) -> Result<()> {
           debug!("H2 Put executed with {} {}", prefix, hash);
           let InitiateResponseEndpoints {
               h2,
               put_complete,
               accepted_encodings,
           } = self
               .initiate_cas_server_query(prefix, hash, data.len())
               .instrument(debug_span!("remote_client.initiate"))
               .await?;

           let encoding = choose_encoding(accepted_encodings);

           debug!("H2 Put initiate response h2 endpoint: {}, put complete endpoint {}\nh2 cert: {}, put complete cert {}", h2.endpoint, put_complete.endpoint, h2.root_ca, put_complete.root_ca);

           {
               // separate scoped to drop transport so that the connection can be reclaimed by the pool
               let transport = self
                   .dt_connection_map
                   .get_connection_for_config(
                       self.get_cas_connection_config_for_endpoint(h2.endpoint)
                           .with_root_ca(h2.root_ca),
                   )
                   .await?;
               transport
                   .put(prefix, hash, data, encoding)
                   .instrument(debug_span!("remote_client.put_h2"))
                   .await?;
           }

           debug!("Data transport completed");

           let cas_connection_config = self
               .get_cas_connection_config_for_endpoint(put_complete.endpoint)
               .with_root_ca(put_complete.root_ca);
           let grpc_client = self
               .get_grpc_connection_for_config(cas_connection_config)
               .await?;

           debug!(
               "Received grpc connection from pool: {}",
               grpc_client.endpoint
           );

           grpc_client
               .put_complete(prefix, hash, chunk_boundaries)
               .await
       }

       async fn get_impl_h2(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>> {
           debug!("H2 Get executed with {} {}", prefix, hash);

           let InitiateResponseEndpoints { h2, .. } = self
               .initiate_cas_server_query(prefix, hash, 0)
               .instrument(debug_span!("remote_client.initiate"))
               .await?;

           let cas_connection_config = self
               .get_cas_connection_config_for_endpoint(h2.endpoint)
               .with_root_ca(h2.root_ca);
           let transport = self
               .dt_connection_map
               .get_connection_for_config(cas_connection_config)
               .instrument(debug_span!("remote_client.get_transport_connection"))
               .await?;
           let data = transport
               .get(prefix, hash)
               .instrument(debug_span!("remote_client.h2_get"))
               .await?;
           drop(transport);

           debug!("Data transport completed");
           Ok(data)
       }

       async fn get_object_range_impl_h2(
           &self,
           prefix: &str,
           hash: &MerkleHash,
           ranges: Vec<(u64, u64)>,
       ) -> Result<Vec<Vec<u8>>> {
           debug!("H2 GetRange executed with {} {}", prefix, hash);

           let InitiateResponseEndpoints { h2, .. } = self
               .initiate_cas_server_query(prefix, hash, 0)
               .instrument(debug_span!("remote_client.initiate"))
               .await?;

           let cas_connection_config = self
               .get_cas_connection_config_for_endpoint(h2.endpoint)
               .with_root_ca(h2.root_ca);
           let transport = self
               .dt_connection_map
               .get_connection_for_config(cas_connection_config)
               .await?;

           let mut handlers = Vec::new();
           for range in ranges {
               handlers.push(transport.get_range(prefix, hash, range));
           }
           let results = futures::future::join_all(handlers).await;
           let errors: Vec<String> = results
               .iter()
               .filter_map(|r| r.as_deref().err().map(|s| s.to_string()))
               .collect();
           if !errors.is_empty() {
               let error_description: String = errors.join("-");
               Err(CasClientError::BatchError(error_description))?;
           }
           let data = results
               .into_iter()
               // unwrap is safe since we verified in the above if that no elements have an error
               .map(|r| r.unwrap())
               .collect_vec();
           Ok(data)
       }
    */
}

fn choose_encoding(accepted_encodings: Vec<CompressionScheme>) -> CompressionScheme {
    if accepted_encodings.is_empty() {
        return CompressionScheme::None;
    }
    if accepted_encodings.contains(&CompressionScheme::LZ4) {
        return CompressionScheme::LZ4;
    }
    CompressionScheme::None
}

fn cas_client_error_retriable(err: &CasClientError) -> bool {
    // we do not retry the logical errors
    !matches!(
        err,
        CasClientError::InvalidRange
            | CasClientError::InvalidArguments
            | CasClientError::HashMismatch
    )
}
/*
impl Client for RemoteClient {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<()> {
        // We first check if the block already exists, to avoid an unnecessary upload
        if let Ok(xorb_size) = self.get_length(prefix, hash).await {
            if xorb_size > 0 {
                return Ok(());
            }
        }
        // We could potentially narrow down the error conditions
        // further, but that gets complicated.
        // So we just do something pretty coarse-grained
        let strategy = RetryStrategy::new(PUT_MAX_RETRIES, PUT_RETRY_DELAY_MS);
        let res = strategy
            .retry(
                || async {
                    self.put_impl_h2(prefix, hash, &data, &chunk_boundaries)
                        .await
                },
                |e| {
                    let retry = cas_client_error_retriable(e);
                    if retry {
                        info!("Put error {:?}. Retrying...", e);
                    }
                    retry
                },
            )
            .await;

        if let Err(ref e) = res {
            if cas_client_error_retriable(e) {
                error!("Too many failures writing {:?}: {:?}.", hash, e);
            }
        }
        res
    }

    async fn flush(&self) -> Result<()> {
        // this client does not background so no flush is needed
        Ok(())
    }

    async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>> {
        self.get_impl_h2(prefix, hash).await
    }

    async fn get_object_range(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>> {
        self.get_object_range_impl_h2(prefix, hash, ranges).await
    }

    async fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u64> {
        let key = format!("{}:{}", prefix, hash.hex());

        let cache = self.length_cache.clone();

        // See if it's in the cache first before we try to launch it; this is cheap.
        {
            let cache = cache.lock().await;
            if let Some(v) = cache.get(&key) {
                return Ok(*v);
            }
        }
        let cas_connection_config =
            self.get_cas_connection_config_for_endpoint(self.lb_endpoint.clone());
        let connection_map = self.grpc_connection_map.clone();

        let (res, _dedup) = self
            .length_singleflight
            .work(
                &key,
                Self::get_length_from_remote(
                    connection_map,
                    cas_connection_config,
                    cache,
                    prefix.to_string(),
                    *hash,
                ),
            )
            .await;

        return match res {
            Ok(v) => Ok(v),
            Err(singleflight::SingleflightError::InternalError(e)) => Err(e),
            Err(e) => Err(CasClientError::InternalError(anyhow::Error::from(e))),
        };
    }
}
*/
/*
// static functions that can be used in spawned tasks
impl RemoteClient {
    async fn get_length_from_remote(
        connection_map: Arc<Mutex<HashMap<String, GrpcClient>>>,
        cas_connection_config: CasConnectionConfig,
        cache: Arc<Mutex<HashMap<String, u64>>>,
        prefix: String,
        hash: MerkleHash,
    ) -> Result<u64> {
        let key = format!("{}:{}", prefix, hash.hex());
        {
            let cache = cache.lock().await;
            if let Some(v) = cache.get(&key) {
                return Ok(*v);
            }
        }

        let grpc_client =
            Self::get_grpc_connection_for_config_from_map(connection_map, cas_connection_config)
                .await?;

        debug!("RemoteClient: GetLength of {}/{}", prefix, hash);

        let res = grpc_client.get_length(&prefix, &hash).await?;

        debug!(
            "RemoteClient: GetLength of {}/{} request complete",
            prefix, hash
        );

        // See if it's in the cache
        {
            let mut cache = cache.lock().await;
            let _ = cache.insert(key.clone(), res);
        }

        Ok(res)
    }

    async fn get_grpc_connection_for_config_from_map(
        grpc_connection_map: Arc<Mutex<HashMap<String, GrpcClient>>>,
        cas_connection_config: CasConnectionConfig,
    ) -> Result<GrpcClient> {
        let mut map = grpc_connection_map.lock().await;
        if let Some(client) = map.get(&cas_connection_config.endpoint) {
            return Ok(client.clone());
        }
        // yes the lock is held through to endpoint creation.
        // While strictly by locking patterns we should release the
        // lock here, create the client, then re-acquire the lock to insert
        // into the map, in practice *thousands* of threads could call this
        // method simultaneously leading to a "race" where we create
        // thousands of connections.
        //
        // Really we need to "single-flight" connection creation per endpoint.
        // Since each RemoteClient really connects to only 1 endpoint,
        // just locking the whole method here pretty much does what we need.
        let endpoint = cas_connection_config.endpoint.clone();
        let new_client = GrpcClient::new_from_connection_config(cas_connection_config).await?;
        map.insert(endpoint, new_client.clone());
        Ok(new_client)
    }
}
*/

// TODO: add retries
// #[async_trait::async_trait]
impl RemoteClient {
    pub async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<()> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };
        self.client
            .upload(&key, data, chunk_boundaries)
            .await
            .map(|_| ())
    }

    pub async fn flush(&self) -> Result<()> {
        Ok(())
    }

    pub async fn get(&self, prefix: &str, hash: &merklehash::MerkleHash) -> Result<Vec<u8>> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };
        let result = self.client.get(&key).await?;
        Ok(result)
    }

    pub async fn get_object_range(
        &self,
        prefix: &str,
        hash: &merklehash::MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };
        let xorb = self.client.get(&key).await?;
        let mut res = Vec::with_capacity(ranges.len());
        for (start, end) in ranges {
            let start = start as usize;
            let end = end as usize;
            if start > xorb.len() || end > xorb.len() {
                return Err(CasClientError::InvalidRange);
            }
            let section = &xorb[start..end];
            res.push(Vec::from(section))
        }

        Ok(res)
    }

    pub async fn get_length(&self, prefix: &str, hash: &merklehash::MerkleHash) -> Result<u64> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };
        match self.client.get_length(&key).await? {
            Some(length) => Ok(length),
            None => Err(CasClientError::XORBNotFound(*hash)),
        }
    }
}

#[derive(Debug)]
pub struct DSCASAPIClient {
    client: reqwest::Client,
}

impl Default for DSCASAPIClient {
    fn default() -> Self {
        Self::new()
    }
}

fn url_parse(url_str: String) -> Result<Url> {
    log!("parse url: {url_str}");
    match url_str.parse() {
        Ok(v) => Ok(v),
        Err(e) => {
            log!("error parsing url: {e:?}");
            Err(CasClientError::URLParseError(e))
        }
    }
}

impl DSCASAPIClient {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            // .http2_prior_knowledge()
            .build()
            .info_error("client create")
            .unwrap();
        Self { client }
    }

    pub async fn get(&self, key: &Key) -> Result<Vec<u8>> {
        let url = url_parse(format!("{SCHEME}://{CAS_ENDPOINT}/{key}"))?;
        let request = reqwest::Request::new(reqwest::Method::GET, url);
        let response = self
            .client
            .execute(request)
            .await
            .info_error("reqwest error on get")?;
        let xorb_data = response.bytes().await?;
        Ok(xorb_data.to_vec())
    }

    pub async fn exists(&self, key: &Key) -> Result<bool> {
        let url = url_parse(format!("{SCHEME}://{CAS_ENDPOINT}/{key}"))?;
        let response = self
            .client
            .head(url)
            .send()
            .await
            .info_error("reqwest error on exists")?;
        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            e => Err(CasClientError::InternalError(anyhow!(
                "unrecognized status code {e}"
            ))),
        }
    }

    pub async fn get_length(&self, key: &Key) -> Result<Option<u64>> {
        let url = url_parse(format!("{SCHEME}://{CAS_ENDPOINT}/{key}"))?;
        let response = self
            .client
            .head(url)
            .send()
            .await
            .info_error("reqwest error on get_length")?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if status != StatusCode::OK {
            return Err(CasClientError::InternalError(anyhow!(
                "unrecognized status code {status}"
            )));
        }
        let hv = match response.headers().get("Content-Length") {
            Some(hv) => hv,
            None => {
                return Err(CasClientError::InternalError(anyhow!(
                    "HEAD missing content length header"
                )))
            }
        };
        let length: u64 = hv
            .to_str()
            .map_err(|_| {
                CasClientError::InternalError(anyhow!("HEAD missing content length header"))
            })?
            .parse()
            .map_err(|_| CasClientError::InternalError(anyhow!("failed to parse length")))?;

        Ok(Some(length))
    }

    pub async fn upload<T: Into<reqwest::Body>>(
        &self,
        key: &Key,
        contents: T,
        chunk_boundaries: Vec<u64>,
    ) -> Result<bool> {
        let chunk_boundaries_query = chunk_boundaries
            .iter()
            .map(|num| num.to_string())
            .collect::<Vec<String>>()
            .join(",");
        let url: Url = url_parse(format!(
            "{SCHEME}://{CAS_ENDPOINT}/{key}?chunk_boundaries={chunk_boundaries_query}"
        ))?;

        let response = self
            .client
            .post(url)
            .body(contents.into())
            .send()
            .await
            .info_error("reqwest error on upload")?;

        let response_body = response.bytes().await?;
        let response_parsed: PostResponse = serde_json::from_reader(response_body.reader())?;

        Ok(response_parsed.was_inserted)
    }

    pub async fn shard_sync(&self, key: &Key, force_sync: bool, salt: &[u8; 32]) -> Result<bool> {
        let url = url_parse(format!("{SCHEME}://{CAS_ENDPOINT}/shard/sync"))?;
        let params = SyncShardParams {
            key: key.clone(),
            force_sync,
            salt: Vec::from(salt),
        };

        let response_value: SyncShardResponse = self.post_json(url, &params).await?;
        match response_value.response {
            SyncShardResponseType::Exists => Ok(false),
            SyncShardResponseType::SyncPerformed => Ok(true),
        }
    }

    pub async fn shard_query_file(&self, file_id: &MerkleHash) -> Result<QueryFileResponse> {
        let url = url_parse(format!("{SCHEME}://{CAS_ENDPOINT}/shard/query_file"))?;
        let params = QueryFileParams { file_id: *file_id };
        let response_value: QueryFileResponse = self.post_json(url, &params).await?;
        Ok(response_value)
    }

    pub async fn shard_query_chunk(
        &self,
        prefix: &str,
        chunk: Vec<MerkleHash>,
    ) -> Result<QueryChunkResponse> {
        let url: Url = format!("{SCHEME}://{CAS_ENDPOINT}/shard/query_chunk").parse()?;
        let params = QueryChunkParams {
            prefix: prefix.to_string(),
            chunk,
        };
        let response_value: QueryChunkResponse = self.post_json(url, &params).await?;
        Ok(response_value)
    }

    async fn post_json<ReqT, RespT>(&self, url: Url, request_body: &ReqT) -> Result<RespT>
    where
        ReqT: Serialize,
        RespT: DeserializeOwned,
    {
        let body = serde_json::to_vec(request_body)?;
        let response = self
            .client
            .post(url)
            .body(body)
            .send()
            .await
            .info_error("reqwest error on post_json")?;

        let response_bytes = response.bytes().await?;
        serde_json::from_reader(response_bytes.reader()).map_err(CasClientError::SerdeJSONError)
    }
}

lazy_static! {
    static ref TRACE_FORWARDING: AtomicBool = AtomicBool::new(false);
}

pub fn set_trace_forwarding(should_enable: bool) {
    TRACE_FORWARDING.store(should_enable, Ordering::Relaxed);
}

pub fn trace_forwarding() -> bool {
    TRACE_FORWARDING.load(Ordering::Relaxed)
}
