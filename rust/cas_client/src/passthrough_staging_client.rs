use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;

use merklehash::MerkleHash;

use crate::interface::{CasClientError, Client};
use crate::staging_trait::*;

/// The PassthroughStagingClient is a simple wrapper around
/// a Client that provides the trait implementations required for StagingClient
/// All staging operations are no-op.
#[derive(Debug)]
pub struct PassthroughStagingClient {
    client: Arc<dyn Client + Sync + Send>,
}

impl PassthroughStagingClient {
    /// Create a new passthrough staging client which wraps any other client.
    /// All operations are simply passthrough to the internal client.
    /// All staging operations are no-op.
    pub fn new(client: Arc<dyn Client + Sync + Send>) -> PassthroughStagingClient {
        PassthroughStagingClient { client }
    }
}

impl Staging for PassthroughStagingClient {}

#[async_trait]
impl StagingUpload for PassthroughStagingClient {
    /// Upload all staged will upload everything to the remote client.
    /// TODO : Caller may need to be wary of a HashMismatch error which will
    /// indicate that the local staging environment has been corrupted somehow.
    async fn upload_all_staged(
        &self,
        _max_concurrent: usize,
        _retain: bool,
    ) -> Result<(), CasClientError> {
        Ok(())
    }
}

#[async_trait]
impl StagingBypassable for PassthroughStagingClient {
    fn get_direct_client(&mut self) -> Arc<dyn Client + Send + Sync> {
        self.client.clone()
    }
}

#[async_trait]
impl StagingInspect for PassthroughStagingClient {
    async fn list_all_staged(&self) -> Result<Vec<String>, CasClientError> {
        Ok(vec![])
    }

    async fn get_length_staged(
        &self,
        _prefix: &str,
        hash: &MerkleHash,
    ) -> Result<usize, CasClientError> {
        Err(CasClientError::XORBNotFound(*hash))
    }

    async fn get_length_remote(
        &self,
        prefix: &str,
        hash: &MerkleHash,
    ) -> Result<usize, CasClientError> {
        let item = self.client.get_length(prefix, hash).await?;

        Ok(item as usize)
    }

    fn get_staging_path(&self) -> PathBuf {
        PathBuf::default()
    }

    fn get_staging_size(&self) -> Result<usize, CasClientError> {
        Ok(0)
    }
}

#[async_trait]
impl Client for PassthroughStagingClient {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<(), CasClientError> {
        self.client.put(prefix, hash, data, chunk_boundaries).await
    }

    async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>, CasClientError> {
        self.client.get(prefix, hash).await
    }

    async fn get_object_range(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>, CasClientError> {
        self.client.get_object_range(prefix, hash, ranges).await
    }

    async fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u64, CasClientError> {
        self.client.get_length(prefix, hash).await
    }
}
