use rusoto_core::RusotoError;
use xet_error::Error;

use parutils::ParallelError;

#[derive(Error, Debug)]
pub enum XetS3Error {
    #[error("File I/O error")]
    IOError(#[from] std::io::Error),

    #[error("S3 Get error: {0}")]
    GetObjectError(#[from] RusotoError<rusoto_s3::GetObjectError>),

    #[error("S3 Error: {0}")]
    Error(String),
}

// Define our own result type here (this seems to be the standard).
pub type Result<T> = std::result::Result<T, XetS3Error>;

#[allow(dead_code)]
pub fn convert_parallel_error(e: ParallelError<XetS3Error>) -> XetS3Error {
    match e {
        ParallelError::JoinError => XetS3Error::Error("Join error".into()),
        ParallelError::TaskError(t) => t,
    }
}
