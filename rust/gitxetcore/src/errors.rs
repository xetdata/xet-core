use std::any::Any;
use std::io;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::process::{ExitCode, Termination};

use merklehash::MerkleHash;
use s3::XetS3Error;
use thiserror::Error;

use crate::config::ConfigError;
use cas_client::CasClientError;
use parutils::ParallelError;

#[derive(Error, Debug)]
pub enum GitXetRepoError {
    #[error("File I/O error : {0}")]
    IOError(#[from] io::Error),

    #[error("CAS Communication Error : {0}")]
    NetworkIOError(#[from] CasClientError),

    #[error("Unable to parse string as hex hash value.")]
    HashStringParsingFailure(#[from] merklehash::DataHashHexParseError),

    #[error("MerkleDBError : {0}")]
    MerkleDBError(#[from] merkledb::error::MerkleDBError),

    #[error("MerkleDB Shard Error : {0}")]
    MDBShardError(#[from] mdb_shard::error::MDBShardError),

    #[error("CAS Error : {0}")]
    CasClientError(String),

    #[error("File Reconstruction Failed")]
    FileReconstructionFailed(MerkleHash),

    #[error("Hash not found")]
    HashNotFound,

    #[error("StreamParsingError: {0}")]
    StreamParseError(String),

    #[error("Parse header error : {0}")]
    StreamParseHeader(#[from] ParseIntError),

    #[error("Data Parsing Error")]
    DataParsingError(String),

    #[error("UTF-8 Parse Error")]
    Utf8Parse(#[from] std::str::Utf8Error),

    #[error("Git Error : {0}")]
    GitRepoError(#[from] git2::Error),

    #[error("JSON Error : {0}")]
    JSONError(#[from] serde_json::error::Error),

    #[error("config error: {0}")]
    ConfigError(#[from] ConfigError),

    // TODO: Internal errors should be converted away from anyhow
    #[error("Internal Error : {0}")]
    InternalError(#[from] anyhow::Error),

    #[error("Error : {0}")]
    Other(String),

    #[error("{0}")]
    RefusingToOverwriteLocalChanges(String),

    #[error("Invalid Operation : {0}")]
    InvalidOperation(String),

    #[error("Repo not discoverable")]
    RepoNotDiscoverable,

    #[error("no remotes found for current repo")]
    RepoHasNoRemotes,

    #[error("invalid remote")]
    InvalidRemote,

    #[error("local CAS path: {0} invalid")]
    InvalidLocalCasPath(String),

    #[error("log file: {0} couldn't be created: {1}")]
    InvalidLogPath(PathBuf, io::Error),

    #[error("file: {0} not found")]
    FileNotFound(PathBuf),

    #[error("S3 Error: {0}")]
    S3Error(#[from] XetS3Error),

    #[error("Unable to check Windows edition")]
    WindowsEditionCheckError,

    #[error("Authentication Error: {0}")]
    AuthError(anyhow::Error),
}

// Define our own result type here (this seems to be the standard).
pub type Result<T> = std::result::Result<T, GitXetRepoError>;

// For error checking
impl PartialEq for GitXetRepoError {
    fn eq(&self, other: &GitXetRepoError) -> bool {
        match (self, other) {
            (GitXetRepoError::IOError(ref e1), GitXetRepoError::IOError(ref e2)) => {
                e1.kind() == e2.kind()
            }
            (GitXetRepoError::MerkleDBError(ref e1), GitXetRepoError::MerkleDBError(ref e2)) => {
                e1 == e2
            }
            _ => false,
        }
    }
}

/// Allows for termination of the main() function with specific error codes.
/// Note, since Termination trait and Result<T, E> are not defined in our crate,
/// we cannot implement Termination for our Result<T> type.
pub enum MainReturn {
    Success,
    Error(GitXetRepoError),
    Panic(Box<dyn Any + Send>), // TODO: fix sync issues when trying to catch panics in gitxet.rs#main()
}

// Note: stable as of rust-1.61: https://blog.rust-lang.org/2022/05/19/Rust-1.61.0.html#custom-exit-codes-from-main
impl Termination for MainReturn {
    fn report(self) -> ExitCode {
        match self {
            MainReturn::Success => ExitCode::SUCCESS,
            MainReturn::Error(err) => {
                // TODO: should we exit with separate error codes for different errors
                eprintln!("{err}");
                ExitCode::FAILURE
            }
            MainReturn::Panic(e) => {
                eprintln!("{e:?}");
                ExitCode::from(255)
            }
        }
    }
}

#[allow(dead_code)]
pub fn convert_cas_error(err: CasClientError) -> Result<()> {
    match err {
        CasClientError::Grpc(_) => Err(GitXetRepoError::NetworkIOError(err)),
        _ => Err(GitXetRepoError::CasClientError(
            "CAS Error: ".to_owned() + &err.to_string(),
        )),
    }
}

#[allow(dead_code)]
pub fn convert_parallel_error(e: ParallelError<GitXetRepoError>) -> GitXetRepoError {
    match e {
        ParallelError::JoinError => GitXetRepoError::Other("Join error".into()),
        ParallelError::TaskError(t) => t,
    }
}
