use std::sync::PoisonError;

use xet_error::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum ShardClientError {
    #[error("File I/O error")]
    IOError(#[from] std::io::Error),

    #[error("LMDB Error: {0}")]
    ShardDedupDBError(String),

    #[error("Data Parsing Error")]
    DataParsingError(String),

    #[error("Error : {0}")]
    Other(String),

    #[error("MerkleDB Shard Error : {0}")]
    MDBShardError(#[from] mdb_shard::error::MDBShardError),

    #[error("CAS Client error: {0}")]
    CasClientError(#[from] cas_client::CasClientError),

    #[error("LockError")]
    LockError,
}

impl<T> From<PoisonError<T>> for ShardClientError {
    fn from(_value: PoisonError<T>) -> Self {
        ShardClientError::LockError
    }
}

// Define our own result type here (this seems to be the standard).
pub type Result<T> = std::result::Result<T, ShardClientError>;
