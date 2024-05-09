use crate::config::XetConfig;
use crate::constants::{GIT_XET_VERSION, LOCAL_CAS_SCHEME, MAX_CONCURRENT_DOWNLOADS};
pub use crate::data::{FILTER_BYTES_CLEANED, FILTER_BYTES_SMUDGED, FILTER_CAS_BYTES_PRODUCED};
use crate::errors::{GitXetRepoError, Result};
use cas_client::{
    new_staging_client, new_staging_client_with_progressbar, CachingClient, LocalClient,
    RemoteClient, Staging,
};
use futures::prelude::stream::*;
use merkledb::ObjectRange;
use merklehash::MerkleHash;
use std::env::current_dir;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{error, info, info_span};

pub async fn create_cas_client(config: &XetConfig) -> Result<Arc<dyn Staging + Send + Sync>> {
    info!(
        "CAS staging directory located at: {:?}.",
        &config.staging_path
    );

    let endpoint = &config.cas_endpoint().await?;
    let (user_id, _) = &config.user.get_user_id();
    let auth = &config.user.get_login_id();
    let repo_paths = config.known_remote_repo_paths();

    if let Some(fs_path) = endpoint.strip_prefix(LOCAL_CAS_SCHEME) {
        info!("Using local CAS with path: {:?}.", endpoint);
        let mut path = PathBuf::from_str(fs_path)
            .map_err(|_| GitXetRepoError::InvalidLocalCasPath(fs_path.to_string()))?;
        if !path.is_absolute() {
            path = current_dir()?.join(path);
        }
        let client = LocalClient::new(&path, false);
        Ok(new_staging_client_with_progressbar(
            client,
            config.staging_path.as_deref(),
        ))
    } else if config.cache.enabled {
        let cacheclient_result = CachingClient::new(
            RemoteClient::from_config(
                endpoint,
                user_id,
                auth,
                repo_paths.clone(),
                GIT_XET_VERSION.clone(),
            )
            .await,
            &config.cache.path,
            config.cache.size,
            config.cache.blocksize,
        );
        match cacheclient_result {
            Ok(cacheclient) => {
                info!(
                    "Using Caching CAS with endpoint {:?}, prefix {:?}, caching at {:?}.",
                    &endpoint, &config.cas.prefix, &config.cache.path
                );
                Ok(new_staging_client_with_progressbar(
                    cacheclient,
                    config.staging_path.as_deref(),
                ))
            }
            Err(e) => {
                error!(
                    "Unable to use caching CAS due to: {:?}; Falling back to non-caching CAS with endpoint: {:?}.",
                    &e, &endpoint
                );
                let remote_client = RemoteClient::from_config(
                    endpoint,
                    user_id,
                    auth,
                    repo_paths.clone(),
                    GIT_XET_VERSION.clone(),
                )
                .await;
                Ok(new_staging_client_with_progressbar(
                    remote_client,
                    config.staging_path.as_deref(),
                ))
            }
        }
    } else {
        info!("Using non-caching CAS with endpoint: {:?}.", &endpoint);
        let remote_client = RemoteClient::from_config(
            endpoint,
            user_id,
            auth,
            repo_paths.clone(),
            GIT_XET_VERSION.clone(),
        )
        .await;
        Ok(new_staging_client(
            remote_client,
            config.staging_path.as_deref(),
        ))
    }
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
    let mut query_result = cas
        .get_object_range(&prefix, &hash, vec![ranges])
        .await
        .map_err(|e| GitXetRepoError::Other(format!("Error fetching Xorb {hash:?}: {e:?}.")))?;
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
    .buffered(MAX_CONCURRENT_DOWNLOADS);

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
