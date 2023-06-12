use std::io;
use std::path::PathBuf;
use thiserror::Error;
use xet_config::CfgError;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("couldn't load config")]
    ConfigLoadError,

    #[error("internal git issue: {0}")]
    GitError(#[from] git2::Error),

    #[error("Invalid args: {0}")]
    InvalidArgs(String),

    #[error(transparent)]
    Config(#[from] CfgError),

    #[error("Key is required for this command")]
    KeyRequired,

    // Note that this should never actually be thrown since clap's group()
    // macro should prevent illegal combinations of args.
    #[error("BUG: Invalid combination of args parsed")]
    InvalidCombination,

    #[error("Home directory path is unknown. $HOME env variable is needed.")]
    HomePathUnknown,

    #[error("BUG: state of the config is not valid: {0}")]
    ConfigBug(String),

    #[error("cas.path: {0} is not a valid: {1}")]
    InvalidCasEndpoint(String, anyhow::Error),

    #[error("cas.path needs to specified when going to a non-standard xetea url: {0}")]
    UnspecifiedCas(String),

    #[error("cas.prefix: {0} has invalid characters. Valid characters include ascii alphanumeric and: {1}")]
    InvalidCasPrefix(String, &'static str),

    #[error("cache.path: {0} couldn't be created: {1}")]
    InvalidCachePath(PathBuf, io::Error),

    #[error("cache.path: {0} is not a directory")]
    CachePathNotDir(PathBuf),

    #[error("cache.path: {0} is not writeable")]
    CachePathReadOnly(PathBuf),

    #[error("log.level: {0} is not one of {{'error'|'warn'|'info'|'debug'|'trace'}}")]
    InvalidLogLevel(String),

    #[error("log.path: {0} is not a file")]
    LogPathNotFile(PathBuf),

    #[error("log.path: {0} is not writeable")]
    LogPathReadOnly(PathBuf),

    #[error("merkledb: {0} doesn't have a parent directory or couldn't create one")]
    InvalidMerkleDBParent(PathBuf),

    #[error("merkledb: {0} is not a file")]
    MerkleDBNotFile(PathBuf),

    #[error("merkledb: {0} is not a directory")]
    MerkleDBNotDir(PathBuf),

    #[error("merkledb: {0} is not writeable")]
    MerkleDBReadOnly(PathBuf),

    #[error("summarydb: {0} doesn't have a parent directory or couldn't create one")]
    InvalidSummaryDBParent(PathBuf),

    #[error("summarydb: {0} is not a file")]
    SummaryDBNotFile(PathBuf),

    #[error("summarydb: {0} is not writeable")]
    SummaryDBReadOnly(PathBuf),

    #[error("repo_path: {0} is not an existing directory")]
    RepoPathNotExistingDir(PathBuf),

    #[error("staging_path: {0} couldn't be created: {1}")]
    StagingDirNotCreated(PathBuf, io::Error),

    #[error("staging_path: {0} not a directory")]
    StagingPathNotDir(PathBuf),

    #[error("config: unsupported configuration {0}")]
    UnsupportedConfiguration(String),

    #[error("Unknown I/O error: {0}")]
    IOError(#[from] io::Error),

    #[error("git remote: {0} is not valid")]
    InvalidGitRemote(String),

    #[error("{0}")]
    InvalidUserCommandOutput(String),

    #[error("axe.enabled: {0} invalid. Valid inputs are true / false")]
    InvalidAxeEnabled(String),

    #[error("Could not find profile: {0} in config")]
    ProfileNotFound(String),
}
