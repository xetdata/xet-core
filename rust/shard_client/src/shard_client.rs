use anyhow::anyhow;
use async_trait::async_trait;
use cas::key::Key;
use cas_types::shard_ops::QueryFileResponse;
use itertools::Itertools;
use mdb_shard::error::MDBShardError;
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use mdb_shard::shard_dedup_probe::ShardDedupProber;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use merkledb::aggregate_hashes::with_salt;
use retry_strategy::RetryStrategy;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn, Span};
use uuid::Uuid;

use cas_client::{CasClientError, DSCASAPIClient};
use merklehash::MerkleHash;

use crate::{error::Result, RegistrationClient, ShardClientInterface, ShardConnectionConfig};

const DEFAULT_VERSION: &str = "0.0.0";

const HTTP2_KEEPALIVE_TIMEOUT_SEC: u64 = 20;
const HTTP2_KEEPALIVE_INTERVAL_SEC: u64 = 1;
const NUM_RETRIES: usize = 5;
const BASE_RETRY_DELAY_MS: u64 = 3000;

// production ready settings

lazy_static::lazy_static! {
    static ref DEFAULT_UUID: Uuid = Uuid::new_v4();
    static ref REQUEST_COUNTER: AtomicUsize = AtomicUsize::new(0);
    static ref TRACE_FORWARDING: AtomicBool = AtomicBool::new(false);
}

// /// Adds common metadata headers to all requests. Currently, this includes
// /// authorization and xet-user-id.
// /// TODO: at some point, we should re-evaluate how we authenticate/authorize requests to CAS.
// #[derive(Debug, Clone)]
// pub struct MetadataHeaderInterceptor {
//     config: ShardConnectionConfig,
// }

// impl MetadataHeaderInterceptor {
//     fn new(config: ShardConnectionConfig) -> MetadataHeaderInterceptor {
//         MetadataHeaderInterceptor { config }
//     }
// }

// impl Interceptor for MetadataHeaderInterceptor {
//     // note original Interceptor trait accepts non-mut request
//     // but may accept mut request like in this case
//     fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
//         request.set_timeout(Duration::new(GRPC_TIMEOUT_SEC, 0));
//         let metadata = request.metadata_mut();

//         let token = MetadataValue::from_static("Bearer some-secret-token");
//         metadata.insert(AUTHORIZATION_HEADER, token);

//         // pass user_id and repo_paths received from xetconfig
//         let user_id = get_metadata_ascii_from_str_with_default(&self.config.user_id, DEFAULT_USER);
//         metadata.insert(USER_ID_HEADER, user_id);

//         let git_xet_version =
//             get_metadata_ascii_from_str_with_default(&self.config.git_xet_version, DEFAULT_VERSION);
//         metadata.insert(GIT_XET_VERSION_HEADER, git_xet_version);

//         let cas_protocol_version: MetadataValue<Ascii> =
//             MetadataValue::from_static(&cas_client::CAS_PROTOCOL_VERSION);
//         metadata.insert(CAS_PROTOCOL_VERSION_HEADER, cas_protocol_version);

//         // propagate tracing context (e.g. trace_id, span_id) to service
//         if trace_forwarding() {
//             let mut injector = HeaderInjector(metadata);
//             let propagator = opentelemetry_jaeger::Propagator::new();
//             let cur_span = Span::current();
//             let ctx = cur_span.context();
//             propagator.inject_context(&ctx, &mut injector);
//         }

//         let request_id = get_request_id();
//         metadata.insert(
//             REQUEST_ID_HEADER,
//             MetadataValue::from_str(&request_id).unwrap(),
//         );

//         Ok(request)
//     }
// }

pub fn _set_trace_forwarding(should_enable: bool) {
    TRACE_FORWARDING.store(should_enable, Ordering::Relaxed);
}

pub fn trace_forwarding() -> bool {
    TRACE_FORWARDING.load(Ordering::Relaxed)
}

// pub struct HeaderInjector<'a>(pub &'a mut MetadataMap);

// impl<'a> Injector for HeaderInjector<'a> {
//     /// Set a key and value in the HeaderMap.  Does nothing if the key or value are not valid inputs.
//     fn set(&mut self, key: &str, value: String) {
//         if let Ok(name) = MetadataKey::from_str(key) {
//             if let Ok(val) = MetadataValue::from_str(&value) {
//                 self.0.insert(name, val);
//             }
//         }
//     }
// }

// fn get_metadata_ascii_from_str_with_default(
//     value: &str,
//     default: &'static str,
// ) -> MetadataValue<Ascii> {
//     MetadataValue::from_str(value)
//         .map_err(|_| VarError::NotPresent)
//         .unwrap_or_else(|_| MetadataValue::from_static(default))
// }

pub fn get_request_id() -> String {
    format!(
        "{}.{}",
        *DEFAULT_UUID,
        REQUEST_COUNTER.load(Ordering::Relaxed)
    )
}

fn inc_request_id() {
    REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
}

/// CAS Client that uses GRPC for communication.
#[derive(Debug)]
pub struct RemoteShardClient {
    pub endpoint: String,
    client: Arc<DSCASAPIClient>,
    retry_strategy: RetryStrategy,
}

impl Clone for RemoteShardClient {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            client: self.client.clone(),
            retry_strategy: self.retry_strategy,
        }
    }
}

impl RemoteShardClient {
    pub fn new(
        endpoint: String,
        client: Arc<DSCASAPIClient>,
        retry_strategy: RetryStrategy,
    ) -> Self {
        Self {
            endpoint,
            client,
            retry_strategy,
        }
    }

    pub async fn from_config(
        shard_connection_config: ShardConnectionConfig,
    ) -> Result<RemoteShardClient> {
        debug!("Creating GrpcShardClient from config: {shard_connection_config:?}");
        let endpoint = shard_connection_config.endpoint.clone();
        let client = Arc::new(DSCASAPIClient::new());
        // Retry policy: Exponential backoff starting at BASE_RETRY_DELAY_MS and retrying NUM_RETRIES times
        let retry_strategy = RetryStrategy::new(NUM_RETRIES, BASE_RETRY_DELAY_MS);
        Ok(RemoteShardClient::new(endpoint, client, retry_strategy))
    }
}

fn is_status_retriable_and_print(e: &CasClientError) -> bool {
    warn!("{e}");
    true // TODO
         // match e {
         //     CasClientError::TonicError => todo!(),
         //     CasClientError::CacheError(_) => todo!(),
         //     CasClientError::ConfigurationError(_) => todo!(),
         //     CasClientError::URLError(_) => todo!(),
         //     CasClientError::InvalidRange => todo!(),
         //     CasClientError::InvalidArguments => todo!(),
         //     CasClientError::HashMismatch => todo!(),
         //     CasClientError::InternalError(_) => todo!(),
         //     CasClientError::XORBNotFound(_) => todo!(),
         //     CasClientError::DataTransferTimeout => todo!(),
         //     CasClientError::Grpc(_) => todo!(),
         //     CasClientError::BatchError(_) => todo!(),
         //     CasClientError::SerializationError(_) => todo!(),
         //     CasClientError::RuntimeErrorTempFileError(_) => todo!(),
         //     CasClientError::LockError => todo!(),
         //     CasClientError::IOError(_) => todo!(),
         //     CasClientError::URLParseError(_) => todo!(),
         //     CasClientError::ReqwestError(_) => todo!(),
         //     CasClientError::SerdeJSONError(_) => todo!(),
         //     _ => todo!(),
         // }
}

#[async_trait]
impl RegistrationClient for RemoteShardClient {
    #[tracing::instrument(skip_all, name = "shard.client", err, fields(prefix = prefix, hash = hash.hex().as_str(), api = "register_shard", request_id = tracing::field::Empty))]
    async fn register_shard_v1(&self, prefix: &str, hash: &MerkleHash, force: bool) -> Result<()> {
        info!("Registering shard {prefix}/{hash:?}");
        inc_request_id();
        Span::current().record("request_id", get_request_id());
        debug!(
            "GrpcShardClient Req {}: register {}/{} as shard",
            get_request_id(),
            prefix,
            hash,
        );
        // fake the salt meh
        let salt = [0u8; 32];
        self.register_shard_with_salt(prefix, hash, force, &salt)
            .await
    }

    #[tracing::instrument(skip_all, name = "shard.client", err, fields(prefix = prefix, hash = format!("{hash}"), salt = format!("{salt:x?}"), api = "register_shard_with_salt", request_id = tracing::field::Empty))]
    async fn register_shard_with_salt(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        force: bool,
        salt: &[u8; 32],
    ) -> Result<()> {
        info!("Registering shard {prefix}/{hash} (w/ salt)");
        inc_request_id();
        Span::current().record("request_id", get_request_id());
        debug!(
            "GrpcShardClient Req {}: register {prefix}/{hash} as shard",
            get_request_id(),
        );
        let key = Key {
            prefix: prefix.to_string(),
            hash: *hash,
        };

        let sync_performed = self
            .retry_strategy
            .retry(
                || async { self.client.shard_sync(&key, force, salt).await },
                is_status_retriable_and_print,
            )
            .await?;

        // It appears that both exists and sync_performed achieve the correct results.
        if sync_performed {
            info!("Shard {prefix:?}/{hash:?} already synced; skipping.");
        } else {
            info!("Shard {prefix:?}/{hash:?} synced.");
        }
        Ok(())
    }
}

#[async_trait]
impl FileReconstructor for RemoteShardClient {
    /// Query the shard server for the file reconstruction info.
    /// Returns the FileInfo for reconstructing the file and the shard ID that
    /// defines the file info.
    ///
    /// TODO: record the shards that are  
    #[tracing::instrument(skip_all, name = "shard.client", err, fields(file_hash = file_hash.hex().as_str(), api = "get_file_reconstruction_info", request_id = tracing::field::Empty))]
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> mdb_shard::error::Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        inc_request_id();
        Span::current().record("request_id", get_request_id());
        debug!(
            "GrpcShardClient Req {}. get_file_reconstruction_info for fileid of {}",
            get_request_id(),
            file_hash
        );
        let QueryFileResponse {
            reconstruction,
            key,
        } = self
            .retry_strategy
            .retry(
                || async { self.client.shard_query_file(file_hash).await },
                is_status_retriable_and_print,
            )
            .await
            .map_err(|e| MDBShardError::InternalError(anyhow!("{e}")))?;

        Ok(Some((
            MDBFileInfo {
                metadata: FileDataSequenceHeader::new(*file_hash, reconstruction.len()),
                segments: reconstruction
                    .into_iter()
                    .map(|ce| {
                        FileDataSequenceEntry::new(
                            ce.cas_id[..].try_into().unwrap(),
                            ce.unpacked_length,
                            ce.range.start,
                            ce.range.end,
                        )
                    })
                    .collect(),
            },
            Some(key.hash),
        )))
    }
}

#[async_trait]
impl ShardDedupProber for RemoteShardClient {
    #[tracing::instrument(skip_all, name = "shard.client", err, fields(prefix = prefix, chunk_hash = format!("{chunk_hash:?}"), api = "register_shard_with_salt", request_id = tracing::field::Empty))]
    async fn get_dedup_shards(
        &self,
        prefix: &str,
        chunk_hash: &[MerkleHash],
        salt: &[u8; 32],
    ) -> mdb_shard::error::Result<Vec<MerkleHash>> {
        inc_request_id();
        Span::current().record("request_id", get_request_id());
        debug!(
            "GrpcShardClient Req {}. get_dedup_shards for chunk hashes {prefix} / {chunk_hash:?}",
            get_request_id(),
        );
        let chunk = chunk_hash
            .iter()
            .filter_map(|chunk| with_salt(chunk, salt).ok())
            // .map(|salted_chunk| salted_chunk)
            .collect_vec();
        let response_info = self
            .retry_strategy
            .retry(
                || async {
                    let chunkc = chunk.clone();
                    self.client.shard_query_chunk(prefix, chunkc).await
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(|e| {
                warn!(
                    "GrpcShardClient Req {}. Error on get_dedup_shards {prefix} / {chunk_hash:?} : {e:?}",
                    get_request_id(),
                );
                MDBShardError::InternalError(anyhow::Error::from(e))
            })?;

        Ok(response_info
            .shard
            .iter()
            .flat_map(|hash| MerkleHash::try_from(&hash[..]))
            .collect_vec())
    }
}

impl ShardClientInterface for RemoteShardClient {}
