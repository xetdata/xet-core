use std::any::Any;
use std::io;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::process::{ExitCode, Termination};

use lazy::error::LazyError;
use merklehash::MerkleHash;
use s3::XetS3Error;
use xet_error::Error;

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

    #[error("invalid remote: {0}")]
    InvalidRemote(String),

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

    #[error("Xet Repo operation attempted before repo is initialized : {0}")]
    RepoUninitialized(String),

    #[error("Repo Salt Unavailable: {0}")]
    RepoSaltUnavailable(String),

    #[error("Subtask scheduling error: {0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("Lazy Config Error : {0}")]
    LazyConfigError(#[from] LazyError),

    #[error("ShardClient Error: {0}")]
    ShardClientError(#[from] shard_client::error::ShardClientError),
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

impl From<GitXetRepoError> for ExitCode {
    fn from(value: GitXetRepoError) -> Self {
        ExitCode::from(match value {
            GitXetRepoError::IOError(_) => 2,
            GitXetRepoError::NetworkIOError(_) => 3,
            GitXetRepoError::HashStringParsingFailure(_) => 4,
            GitXetRepoError::MerkleDBError(_) => 5,
            GitXetRepoError::MDBShardError(_) => 6,
            GitXetRepoError::CasClientError(_) => 7,
            GitXetRepoError::FileReconstructionFailed(_) => 8,
            GitXetRepoError::HashNotFound => 9,
            GitXetRepoError::StreamParseError(_) => 10,
            GitXetRepoError::StreamParseHeader(_) => 11,
            GitXetRepoError::DataParsingError(_) => 12,
            GitXetRepoError::Utf8Parse(_) => 13,
            GitXetRepoError::GitRepoError(_) => 14,
            GitXetRepoError::JSONError(_) => 15,
            GitXetRepoError::ConfigError(_) => 16,
            GitXetRepoError::InternalError(_) => 17,
            GitXetRepoError::Other(_) => 18,
            GitXetRepoError::RefusingToOverwriteLocalChanges(_) => 19,
            GitXetRepoError::InvalidOperation(_) => 20,
            GitXetRepoError::RepoNotDiscoverable => 21,
            GitXetRepoError::RepoHasNoRemotes => 22,
            GitXetRepoError::InvalidRemote(_) => 23,
            GitXetRepoError::InvalidLocalCasPath(_) => 24,
            GitXetRepoError::InvalidLogPath(_, _) => 25,
            GitXetRepoError::FileNotFound(_) => 26,
            GitXetRepoError::S3Error(_) => 27,
            GitXetRepoError::WindowsEditionCheckError => 28,
            GitXetRepoError::AuthError(_) => 29,
            GitXetRepoError::RepoUninitialized(_) => 30,
            GitXetRepoError::RepoSaltUnavailable(_) => 31,
            GitXetRepoError::LazyConfigError(_) => 32,
            GitXetRepoError::JoinError(_) => 33,
            GitXetRepoError::ShardClientError(_) => 34,
        })
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
                eprintln!("{err}");
                err.into()
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
