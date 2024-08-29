use super::configurations::{
    cas_storage_config_from, repo_info_from, Endpoint::*, RepoInfo, StorageConfig,
};
use super::errors::Result;
use super::FILTER_BYTES_SMUDGED;
use crate::config::XetConfig;
use crate::constants::{GIT_XET_VERSION, MAX_CONCURRENT_DOWNLOADS};
use cas_client::{new_staging_client, CachingClient, LocalClient, RemoteClient, Staging};
use futures::prelude::stream::*;
use merkledb::ObjectRange;
use merklehash::MerkleHash;
use shard_client::{GrpcShardClient, LocalShardClient, ShardClientInterface};
use std::env::current_dir;
use std::sync::Arc;
use tracing::{error, info, info_span};

pub async fn old_create_cas_client(xet: &XetConfig) -> Result<Arc<dyn Staging + Send + Sync>> {
    let cas_storage_config = cas_storage_config_from(xet).await?;
    let repo_info = repo_info_from(xet)?;
    create_cas_client(&cas_storage_config, &Some(repo_info)).await
}

///
pub async fn create_cas_client(
    cas_storage_config: &StorageConfig,
    maybe_repo_info: &Option<RepoInfo>,
) -> Result<Arc<dyn Staging + Send + Sync>> {
    // Local file system based CAS storage.
    if let FileSystem(ref path) = cas_storage_config.endpoint {
        info!("Using local CAS with path: {:?}.", path);
        let path = match path.is_absolute() {
            true => path,
            false => &current_dir()?.join(path),
        };
        let client = LocalClient::new(&path, false);
        return Ok(new_staging_client(
            client,
            cas_storage_config.staging_directory.as_deref(),
        ));
    }

    // Now we are using remote server CAS storage.
    let Server(ref endpoint) = cas_storage_config.endpoint else {
        unreachable!();
    };

    // Auth info.
    let user_id = &cas_storage_config.auth.user_id;
    let auth = &cas_storage_config.auth.login_id;

    // Usage tracking.
    let repo_paths = maybe_repo_info
        .as_ref()
        .map(|repo_info| &repo_info.repo_paths)
        .cloned()
        .unwrap_or_default();

    // Raw remote client.
    let remote_client = Arc::new(
        RemoteClient::from_config(
            &endpoint,
            user_id,
            auth,
            repo_paths,
            GIT_XET_VERSION.clone(),
        )
        .await,
    );

    // Try add in caching capability.
    let maybe_caching_client = cas_storage_config.cache_config.as_ref().and_then(|cache| {
        CachingClient::new(
            remote_client.clone(),
            &cache.cache_directory,
            cache.cache_size,
            cache.cache_blocksize,
        )
        .map_err(|e| error!("Unable to use caching CAS due to: {:?}", &e))
        .ok()
    });

    // If initiating caching was unsuccessful, fall back to only remote client.
    match maybe_caching_client {
        Some(caching_client) => {
            info!(
                "Using caching CAS with endpoint {:?}, caching at {:?}.",
                &endpoint,
                cas_storage_config
                    .cache_config
                    .as_ref()
                    .unwrap()
                    .cache_directory
            );

            Ok(new_staging_client(
                caching_client,
                cas_storage_config.staging_directory.as_deref(),
            ))
        }
        None => {
            info!("Using non-caching CAS with endpoint: {:?}.", &endpoint);
            Ok(new_staging_client(
                remote_client,
                cas_storage_config.staging_directory.as_deref(),
            ))
        }
    }

    // let client: Box<dyn Client> = if let Some(cache) = maybe_cache_config {
    //     let ret = CachingClient::new(
    //         remote_client.clone(),
    //         &cache.cache_directory,
    //         cache.cache_size,
    //         cache.cache_blocksize,
    //     );

    //     match ret {
    //         Ok(client) => {
    //             if let Some(ref path) = storage_config.staging_directory {
    //                 info!("CAS staging directory located at: {:?}.", path);
    //             }
    //             Box::new(client)
    //         }
    //         Err(e) => {
    //             error!(
    //                 "Unable to use caching CAS due to: {:?}; Falling back to non-caching CAS with endpoint: {:?}.",
    //                 &e, &endpoint
    //             );
    //             Box::new(remote_client)
    //         }
    //     }
    // } else {
    //     info!("Using non-caching CAS with endpoint: {:?}.", &endpoint);
    //     Box::new(remote_client)
    // };

    // let client = new_staging_client(client, storage_config.staging_directory.as_deref());

    // Ok(client)

    // if config.cache.enabled {
    //     let cacheclient_result = CachingClient::new(
    //         RemoteClient::from_config(
    //             endpoint,
    //             user_id,
    //             auth,
    //             repo_paths.clone(),
    //             GIT_XET_VERSION.clone(),
    //         )
    //         .await,
    //         &config.cache.path,
    //         config.cache.size,
    //         config.cache.blocksize,
    //     );
    //     match cacheclient_result {
    //         Ok(cacheclient) => {
    //             info!(
    //                 "Using Caching CAS with endpoint {:?}, prefix {:?}, caching at {:?}.",
    //                 &endpoint, &config.cas.prefix, &config.cache.path
    //             );
    //             Ok(new_staging_client_with_progressbar(
    //                 cacheclient,
    //                 config.staging_path.as_deref(),
    //             ))
    //         }
    //         Err(e) => {
    //             error!(
    //                 "Unable to use caching CAS due to: {:?}; Falling back to non-caching CAS with endpoint: {:?}.",
    //                 &e, &endpoint
    //             );
    //             let remote_client = RemoteClient::from_config(
    //                 endpoint,
    //                 user_id,
    //                 auth,
    //                 repo_paths.clone(),
    //                 GIT_XET_VERSION.clone(),
    //             )
    //             .await;
    //             Ok(new_staging_client_with_progressbar(
    //                 remote_client,
    //                 config.staging_path.as_deref(),
    //             ))
    //         }
    //     }
    // } else {
    //     info!("Using non-caching CAS with endpoint: {:?}.", &endpoint);
    //     let remote_client = RemoteClient::from_config(
    //         endpoint,
    //         user_id,
    //         auth,
    //         repo_paths.clone(),
    //         GIT_XET_VERSION.clone(),
    //     )
    //     .await;
    //     Ok(new_staging_client(
    //         remote_client,
    //         config.staging_path.as_deref(),
    //     ))
    // }
}

pub async fn create_shard_client(
    shard_storage_config: &StorageConfig,
) -> Result<Arc<dyn ShardClientInterface>> {
    info!("Shard endpoint = {:?}", shard_storage_config.endpoint);
    let client: Arc<dyn ShardClientInterface> = match &shard_storage_config.endpoint {
        Server(endpoint) => {
            let shard_connection_config = shard_client::ShardConnectionConfig {
                endpoint: endpoint.clone(),
                user_id: shard_storage_config.auth.user_id.clone(),
                git_xet_version: GIT_XET_VERSION.to_string(),
            };
            Arc::new(GrpcShardClient::from_config(shard_connection_config).await?)
        }
        FileSystem(path) => Arc::new(LocalShardClient::new(path).await?),
    };

    Ok(client)
}

/**  Wrapper to consolidate the logic for retrieving from CAS.   
 */
pub async fn get_from_cas(
    cas: &Arc<dyn Staging + Send + Sync>,
    prefix: String,
    hash: MerkleHash,
    ranges: (u64, u64),
) -> Result<Vec<u8>> {
    if ranges.0 == ranges.1 {
        return Ok(Vec::new());
    }
    let mut query_result = cas.get_object_range(&prefix, &hash, vec![ranges]).await?;
    Ok(std::mem::take(&mut query_result[0]))
}

/// Given an Vec<ObjectRange> describing a series of range of bytes,
/// slice a subrange. This does not check limits and may return shorter
/// results if the slice goes past the end of the range.
pub fn slice_object_range(v: &[ObjectRange], mut start: usize, mut len: usize) -> Vec<ObjectRange> {
    let mut ret: Vec<ObjectRange> = Vec::new();
    for i in v.iter() {
        let ilen = i.end - i.start;
        // we have not gotten to the start of the range
        if start > 0 && start >= ilen {
            // start is still after this range
            start -= ilen;
        } else {
            // either start == 0, or start < packet len.
            // Either way, we need some or all of this packet
            // and after this packet start must be = 0
            let packet_start = i.start + start;
            // the maximum length allowed is how far to end of the packet
            // OR the actual slice length requested which ever is shorter.
            let max_length_allowed = std::cmp::min(i.end - packet_start, len);
            ret.push(ObjectRange {
                hash: i.hash,
                start: packet_start,
                end: packet_start + max_length_allowed,
            });
            start = 0;
            len -= max_length_allowed;
        }
        if len == 0 {
            break;
        }
    }
    ret
}

/// Writes a collection of chunks from a Vec<ObjectRange> to a writer.
pub async fn data_from_chunks_to_writer(
    cas: &Arc<dyn Staging + Send + Sync>,
    prefix: String,
    chunks: Vec<ObjectRange>,
    writer: &mut impl std::io::Write,
) -> Result<()> {
    let mut bytes_smudged: u64 = 0;
    let mut strm = iter(chunks.into_iter().map(|objr| {
        let prefix = prefix.clone();
        get_from_cas(cas, prefix, objr.hash, (objr.start as u64, objr.end as u64))
    }))
    .buffered(*MAX_CONCURRENT_DOWNLOADS);

    while let Some(buf) = strm.next().await {
        let buf = buf?;
        bytes_smudged += buf.len() as u64;
        let s = info_span!("write_chunk");
        let _ = s.enter();
        writer.write_all(&buf)?;
    }

    FILTER_BYTES_SMUDGED.inc_by(bytes_smudged);

    Ok(())
}
