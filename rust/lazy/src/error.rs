use std::io;

use xet_error::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum LazyError {
    #[error("I/O error: {0}")]
    IOError(#[from] io::Error),

    #[error("Invalid strategy in rule: {0}")]
    InvalidStrategy(String),

    #[error("Invalid path in rule: {0}")]
    InvalidPath(String),

    #[error("Invalid rule syntax: {0}")]
    InvalidRule(String),

    #[error("No rules matching given path: {0}")]
    NoMatchingRule(String),
}

pub type Result<T> = std::result::Result<T, LazyError>;
