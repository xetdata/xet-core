use async_trait::async_trait;
use merklehash::MerkleHash;
use std::sync::Arc;
use thiserror::Error;

/// A Client to the CAS (Content Addressed Storage) service to allow storage and
/// management of XORBs (Xet Object Remote Block). A XORB represents a collection
/// of arbitrary bytes. These bytes are hashed according to a Xet Merkle Hash
/// producing a Merkle Tree. XORBs in the CAS are identified by a combination of
/// a prefix namespacing the XORB and the hash at the root of the Merkle Tree.
#[async_trait]
pub trait Client: core::fmt::Debug {
    /// Insert the provided data into the CAS as a XORB indicated by the prefix and hash.
    /// The hash will be verified on the server-side according to the chunk boundaries.
    /// Chunk Boundaries must be complete; i.e. the last entry in chunk boundary
    /// must be the length of data. For instance, if data="helloworld" with 2 chunks
    /// ["hello" "world"], chunk_boundaries should be [5, 10].
    /// Empty data and empty chunk boundaries are not accepted.
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<(), CasClientError>;

    /// Reads all of the contents for the indicated XORB, returning the data or an error
    /// if an issue occurred.
    async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>, CasClientError>;

    /// Reads the requested ranges for the indicated object. Each range is a tuple of
    /// start byte (inclusive) to end byte (exclusive). Will return the contents for
    /// the ranges if they exist in the order specified. If there are issues fetching
    /// any of the ranges, then an Error will be returned.
    async fn get_object_range(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>, CasClientError>;

    /// Gets the length of the XORB or an error if an issue occurred.
    async fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u64, CasClientError>;
}

/*
 * If T implements Client, Arc<T> also implements Client
 */
#[async_trait]
impl<T: Client + Sync + Send> Client for Arc<T> {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<(), CasClientError> {
        (**self).put(prefix, hash, data, chunk_boundaries).await
    }

    async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>, CasClientError> {
        (**self).get(prefix, hash).await
    }

    async fn get_object_range(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>, CasClientError> {
        (**self).get_object_range(prefix, hash, ranges).await
    }

    async fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u64, CasClientError> {
        (**self).get_length(prefix, hash).await
    }
}

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum CasClientError {
    #[error("{0}")]
    Grpc(anyhow::Error),
    #[error("Invalid Range Read")]
    InvalidRange,
    #[error("Invalid Arguments")]
    InvalidArguments,
    #[error("Hash Mismatch")]
    HashMismatch,
    #[error("Other Internal Error: {0}")]
    InternalError(anyhow::Error),
    #[error("not found")]
    XORBNotFound(MerkleHash),
    #[error("data transfer timeout")]
    DataTransferTimeout,
    #[error("Client connection error {0}")]
    GrpcClientError(#[from] anyhow::Error),
    #[error("Connection pooling error")]
    ConnectionPooling(#[from] crate::cas_connection_pool::CasConnectionPoolError),
    #[error("Batch Error: {0}")]
    BatchError(String),
}

impl PartialEq for CasClientError {
    fn eq(&self, other: &CasClientError) -> bool {
        match (self, other) {
            (&CasClientError::Grpc(_), &CasClientError::Grpc(_)) => true,
            (CasClientError::InvalidRange, CasClientError::InvalidRange) => true,
            (CasClientError::InvalidArguments, CasClientError::InvalidArguments) => true,
            (CasClientError::HashMismatch, CasClientError::HashMismatch) => true,
            (CasClientError::InternalError(_), CasClientError::InternalError(_)) => true,
            (CasClientError::XORBNotFound(a), CasClientError::XORBNotFound(b)) => a == b,
            (CasClientError::DataTransferTimeout, CasClientError::DataTransferTimeout) => true,
            (CasClientError::GrpcClientError(_), CasClientError::GrpcClientError(_)) => true,
            (CasClientError::ConnectionPooling(_), CasClientError::ConnectionPooling(_)) => true,
            (CasClientError::BatchError(_), CasClientError::BatchError(_)) => true,
            _ => false,
        }
    }
}
