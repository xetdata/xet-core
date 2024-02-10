use std::io;
use xet_error::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum FileOperationError {
    #[error("File I/O error: {0}")]
    IOError(#[from] io::Error),

    #[error("Repository Error: {0}")]
    Other(#[from] anyhow::Error),

    #[error("Invalid Arguments: {0}")]
    InvalidArguments(String),

    #[error("Illegal Operation: {0}")]
    IllegalOperation(String),
}

// Define our own result type here (this seems to be the standard).
pub type Result<T> = std::result::Result<T, FileOperationError>;
