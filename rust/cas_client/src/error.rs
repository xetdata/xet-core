use cache::CacheError;
use merklehash::MerkleHash;
use tonic::metadata::errors::InvalidMetadataValue;
use xet_error::Error;

use crate::cas_connection_pool::CasConnectionPoolError;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum CasClientError {
    #[error("Tonic RPC error.")]
    TonicError,

    #[error("CAS Cache Error: {0}")]
    CacheError(#[from] CacheError),

    #[error("Metadata error: {0}")]
    MetadataParsingError(#[from] InvalidMetadataValue),

    #[error("CAS Connection Pool Error")]
    CasConnectionPoolError(#[from] CasConnectionPoolError),

    #[error("Invalid Range Read")]
    InvalidRange,

    #[error("Invalid Arguments")]
    InvalidArguments,

    #[error("Hash Mismatch")]
    HashMismatch,

    #[error("Other Internal Error: {0}")]
    InternalError(anyhow::Error),

    #[error("CAS Hash not found")]
    XORBNotFound(MerkleHash),

    #[error("Data transfer timeout")]
    DataTransferTimeout,

    #[error("Client connection error {0}")]
    Grpc(#[from] anyhow::Error),

    #[error("Batch Error: {0}")]
    BatchError(String),
}

// Define our own result type here (this seems to be the standard).
pub type Result<T> = std::result::Result<T, CasClientError>;

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
        }
    }
}
