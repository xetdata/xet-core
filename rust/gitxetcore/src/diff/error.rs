use anyhow::anyhow;
use thiserror::Error;

#[derive(Error, Debug)]
#[allow(dead_code)] // we want to allow Unknown for ease of development.
pub enum DiffError {
    #[error("Invalid hashes provided: {0}")]
    InvalidHashes(String),
    #[error("Summary for commit before the change not found")]
    BeforeSummaryNotFound,
    #[error("Summary for commit after the change not found")]
    AfterSummaryNotFound,
    #[error("Summaries for file not found")]
    NoSummaries,
    #[error("File contents are the same")]
    NoDiff,
    #[error("Not running in a repo dir")]
    NotInRepoDir,
    #[error("Couldn't create summary: {0}")]
    FailedSummaryCalculation(#[from] anyhow::Error),
    #[error("Internal issue occurred: {0}")]
    Unknown(String),
}

impl DiffError {
    /// Provides the error-code for the indicated error
    pub fn get_code(&self) -> u8 {
        match self {
            DiffError::InvalidHashes(_) => 1,
            DiffError::BeforeSummaryNotFound => 2,
            DiffError::AfterSummaryNotFound => 3,
            DiffError::NoSummaries => 4,
            DiffError::NoDiff => 5,
            DiffError::NotInRepoDir => 6,
            DiffError::FailedSummaryCalculation(_) => 7,
            DiffError::Unknown(_) => 255,
        }
    }
}

impl Clone for DiffError {
    fn clone(&self) -> Self {
        match self {
            DiffError::InvalidHashes(e) => DiffError::InvalidHashes(e.clone()),
            DiffError::BeforeSummaryNotFound => DiffError::BeforeSummaryNotFound,
            DiffError::AfterSummaryNotFound => DiffError::AfterSummaryNotFound,
            DiffError::NoSummaries => DiffError::NoSummaries,
            DiffError::NoDiff => DiffError::NoDiff,
            DiffError::NotInRepoDir => DiffError::NotInRepoDir,
            DiffError::FailedSummaryCalculation(e) => {
                DiffError::FailedSummaryCalculation(anyhow!(e.to_string()))
            }
            DiffError::Unknown(e) => DiffError::Unknown(e.clone()),
        }
    }
}
