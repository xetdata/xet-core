use std::sync::PoisonError;

use cache::CacheError;
use http::uri::InvalidUri;
use merklehash::MerkleHash;
use xet_error::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum CasClientError {
    #[error("Tonic RPC error.")]
    TonicError,

    #[error("CAS Cache Error: {0}")]
    CacheError(#[from] CacheError),

    #[error("Configuration Error: {0} ")]
    ConfigurationError(String),

    #[error("URL Parsing Error.")]
    URLError(#[from] InvalidUri),

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

    #[error("Serialization Error: {0}")]
    SerializationError(#[from] bincode::Error),
    #[error("Runtime Error (Temp files): {0}")]
    RuntimeErrorTempFileError(#[from] tempfile::PersistError),
    #[error("LockError")]
    LockError,

    #[error("IOError: {0}")]
    IOError(#[from] std::io::Error),

    #[error("URLParseError: {0}")]
    URLParseError(#[from] url::ParseError),

    #[error("reqwest Error: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("serde_json Error: {0}")]
    SerdeJSONError(#[from] serde_json::Error),
}

impl<T> From<PoisonError<T>> for CasClientError {
    fn from(_value: PoisonError<T>) -> Self {
        CasClientError::LockError
    }
}

// Define our own result type here (this seems to be the standard).
pub type Result<T> = std::result::Result<T, CasClientError>;

impl PartialEq for CasClientError {
    fn eq(&self, other: &CasClientError) -> bool {
        match (self, other) {
            (CasClientError::XORBNotFound(a), CasClientError::XORBNotFound(b)) => a == b,
            (e1, e2) => std::mem::discriminant(e1) == std::mem::discriminant(e2),
        }
    }
}
