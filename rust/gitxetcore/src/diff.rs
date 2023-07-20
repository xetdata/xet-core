use std::fmt::Debug;

use clap::Args;
use tracing::{debug, info};

use error::DiffError;
use merklehash::MerkleHash;
use processor::SummaryDiffProcessor;

use crate::config::XetConfig;
use crate::diff::csv::CsvSummaryDiffProcessor;
use crate::diff::fetcher::SummaryFetcher;
use crate::diff::output::{DiffOutput, SummaryDiff};
use crate::errors;
use crate::summaries::analysis::FileSummary;

mod csv;
mod error;
mod fetcher;
mod output;
mod processor;
mod util;

#[derive(Args, Debug, Default)]
pub struct DiffArgs {
    /// The merkle hash of the data for the file in the `before_commit`. If this and the `before_id`
    /// is absent, then the file is assumed to have been added. The hash should be hex-encoded.
    #[clap(long)]
    pub before_hash: Option<String>,

    /// The merkle hash of the data for the file in the `after_commit`. If this is absent, then
    /// the file is assumed to have been deleted. The hash should be hex-encoded.
    #[clap(long)]
    pub after_hash: Option<String>,

    /// The git blob-id for the file in the `before_commit`. If a `before_hash` wasn't specified,
    /// then the summary will be computed from the contents in this id. Note that if both the
    /// `before_id` and `before_hash` are specified, the `before_hash` takes precidence.
    #[clap(long)]
    pub before_id: Option<String>,

    /// The git blob-id for the file in the `after_commit`. If an `after_hash` wasn't specified,
    /// then the summary will be computed from the contents in this id. Note that if both the
    /// `after_id` and `after_hash` are specified, the `after_hash` takes precidence.
    #[clap(long)]
    pub after_id: Option<String>,

    /// The file path. Used only to determine what types of summaries to include in the diff.
    #[clap(long)]
    pub file_path: String
}

impl DiffArgs {
    /// Checks that the args are valid.
    ///
    /// Validity here means:
    /// * at least one of the fields has been set.
    /// * `before_hash` and `after_hash`, if specified, are hex-encoded [MerkleHash]es.
    /// * `before_id`
    fn validate(&self) -> Result<(), DiffError> {
        if self.before_hash.is_none()
            && self.after_hash.is_none()
            && self.before_id.is_none()
            && self.after_id.is_none()
        {
            return Err(DiffError::InvalidHashes(
                "No hashes or ids provided".to_string(),
            ));
        }
        // If before == after, then there isn't anything to diff as the contents
        // are the same. We check this first for the hashes, then if those were
        // not specified, compare the ids (as hashes have precedence over ids).
        if let (Some(before), Some(after)) = (self.before_hash.as_ref(), self.after_hash.as_ref()) {
            if before == after {
                return Err(DiffError::NoDiff);
            }
        } else if let (None, None) = (self.before_hash.as_ref(), self.after_hash.as_ref()) {
            if let (Some(before), Some(after)) = (self.before_id.as_ref(), self.after_id.as_ref()) {
                if before == after {
                    return Err(DiffError::NoDiff);
                }
            }
        }

        if let Some(ref hash) = self.before_hash {
            MerkleHash::from_hex(hash).map_err(|_| {
                DiffError::InvalidHashes("before_hash not valid hex string".to_string())
            })?;
        }

        if let Some(ref hash) = self.after_hash {
            MerkleHash::from_hex(hash).map_err(|_| {
                DiffError::InvalidHashes("after_hash not valid hex string".to_string())
            })?;
        }
        Ok(())
    }
}

pub async fn diff_command(config: XetConfig, args: &DiffArgs) -> errors::Result<()> {
    info!("Invoking diff command with the following args:{:?}", args);

    let diff_output = get_summary_diffs(config, args)
        .await
        .unwrap_or_else(DiffOutput::from);
    debug!("Extracted diff output: {:?}", diff_output);
    print_diff_output(&diff_output)
}

/// Serialize the [DiffOutput] to stdout.
///
/// We currently serialize the contents using the `serde_json` crate.
fn print_diff_output(out: &DiffOutput) -> errors::Result<()> {
    let content = serde_json::to_string(out)?;
    println!("{content}");
    Ok(())
}

/// Retrieve the summary diffs for the provided file
async fn get_summary_diffs(config: XetConfig, args: &DiffArgs) -> Result<DiffOutput, DiffError> {
    args.validate()?;

    let fetcher = SummaryFetcher::new(config).await?;
    let before = fetcher.get_summary(&args.file_path, args.before_hash.as_ref(), args.before_id.as_ref())?;
    let after = fetcher.get_summary(&args.file_path, args.after_hash.as_ref(), args.after_id.as_ref())?;

    let summaries = run_diffs(before.get(), after.get())?;

    Ok(DiffOutput {
        status: 0,
        error_details: None,
        summaries,
    })
}

/// Performs the diff calculation across all summary types, assembling the results into a
/// Vec<SummaryDiff>. Currently, each summary runs serially and if there is an issue, it will
/// early-terminate, skipping all subsequent summaries.
///
/// TODO: we can generate an enum of the `SummaryDiffProcessor` types and use the enum_dispatch
///       crate to dynamically run all of these implementations.
fn run_diffs(
    before_summary: Option<&FileSummary>,
    after_summary: Option<&FileSummary>,
) -> Result<Vec<SummaryDiff>, DiffError> {
    let mut diffs = vec![];

    // csv diff
    let csv_proc = CsvSummaryDiffProcessor {};
    let diff = csv_proc.get_diff(before_summary, after_summary)?;
    diffs.push(diff);

    Ok(diffs)
}

#[cfg(test)]
mod tests {
    use crate::diff::error::DiffError::{InvalidHashes, NoDiff};

    use super::*;

    #[test]
    fn test_input_validation_no_hashes() {
        let args = DiffArgs::default();
        let err = args.validate().expect_err("");
        assert_eq!(err.get_code(), InvalidHashes("".to_string()).get_code());
    }

    #[test]
    fn test_input_validation_equal_hashes() {
        let hash =
            Some("7bc1c074a2f2d52fab65c8733e29e9f03b0ea98e5ca5b5efec9b363d54f7defa".to_string());
        let args = DiffArgs {
            before_hash: hash.clone(),
            after_hash: hash,
            ..Default::default()
        };
        let err = args.validate().expect_err("");
        assert_eq!(err.get_code(), NoDiff.get_code());
    }

    #[test]
    fn test_input_validateion_not_a_hash() {
        let hash =
            Some("7bc1c074a2f2d52fab65c8733e29e9f03b0ea98e5ca5b5efec9b363d54f7defa".to_string());
        let hash2 = Some("abz".to_string());
        let args = DiffArgs {
            before_hash: hash,
            after_hash: hash2,
            ..Default::default()
        };
        let err = args.validate().expect_err("");
        assert_eq!(err.get_code(), InvalidHashes("".to_string()).get_code());
    }

    #[test]
    fn test_input_validation_ok_hashes() {
        let hash =
            Some("7bc1c074a2f2d52fab65c8733e29e9f03b0ea98e5ca5b5efec9b363d54f7defa".to_string());
        let hash2 =
            Some("baa08f1b8998f02a25f224727bff42724c6904ef136ea0afb5ea809f60c1ec0e".to_string());
        let mut args = DiffArgs {
            before_hash: hash,
            after_hash: hash2.clone(),
            ..Default::default()
        };
        args.validate().unwrap();
        // args hashes: (Some(), None)
        args.after_hash = None;
        args.validate().unwrap();
        // args hashes: (None, Some())
        args.before_hash = None;
        args.after_hash = hash2;
        args.validate().unwrap();
    }

    #[test]
    fn test_input_validation_ok_blobs() {
        let blob1 = Some("24a2f6b".to_string());
        let blob2 = Some("60cf438".to_string());
        let mut args = DiffArgs {
            before_id: blob1,
            after_id: blob2.clone(),
            ..Default::default()
        };
        args.validate().unwrap();
        // args hashes: (Some(), None)
        args.after_id = None;
        args.validate().unwrap();
        // args hashes: (None, Some())
        args.before_id = None;
        args.after_id = blob2;
        args.validate().unwrap();
    }

    #[test]
    fn test_input_validation_equal_blobs() {
        let blob_id = Some("24a2f6b".to_string());
        let args = DiffArgs {
            before_id: blob_id.clone(),
            after_id: blob_id,
            ..Default::default()
        };
        let err = args.validate().expect_err("");
        assert_eq!(err.get_code(), NoDiff.get_code());
    }

    #[test]
    fn test_input_validation_ok_mixed() {
        let hash1 =
            Some("7bc1c074a2f2d52fab65c8733e29e9f03b0ea98e5ca5b5efec9b363d54f7defa".to_string());
        let hash2 =
            Some("baa08f1b8998f02a25f224727bff42724c6904ef136ea0afb5ea809f60c1ec0e".to_string());
        let blob1 = Some("24a2f6b".to_string());
        let blob2 = Some("60cf438".to_string());

        let args = DiffArgs {
            before_hash: hash1.clone(),
            after_id: blob2.clone(),
            ..Default::default()
        };
        args.validate().unwrap();

        let args = DiffArgs {
            before_id: blob1.clone(),
            after_hash: hash2.clone(),
            ..Default::default()
        };
        args.validate().unwrap();

        let args = DiffArgs {
            before_hash: hash1,
            before_id: blob1,
            after_hash: hash2,
            after_id: blob2,
            file_path: "foo/bar.baz".to_string(),
        };
        args.validate().unwrap();
    }
}
