use std::fs::File;
use std::io::{stdin, stdout, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::str::FromStr;

use clap::Args;
use mdb_shard::shard_version::ShardVersion;
use merklehash::MerkleHash;

use crate::config::XetConfig;
use crate::constants::{GIT_MAX_PACKET_SIZE, GIT_NOTES_MERKLEDB_V1_REF_NAME};
use crate::data::PointerFileTranslator;
use crate::data::{get_mdb_version, mdbv1};
use crate::errors::GitXetRepoError::{FileNotFound, HashNotFound};
use crate::errors::{self, GitXetRepoError};
use crate::stream::data_iterators::AsyncFileIterator;

use xet_error::Error;

#[derive(Args, Debug)]
/// Outputs a file to stdout given a pointer file or with passthrough option -p set a non-pointer file as input.
/// If a filename is not provided, the pointer file is read from stdin.
///
/// Inverse of the pointer command.
///
/// ```ignore
/// git xet pointer -f input | git xet --cas smudge > output
/// # input and output should be the same file
/// ```
///
pub struct SmudgeArgs {
    /// If passthrough is set (default is disabled),
    /// we will also "smudge" non-pointer files, by passing it through
    /// direct to stdout.
    #[clap(short, long)]
    passthrough: bool,

    #[clap(long, short)]
    filename: Option<PathBuf>,

    /// Lookup file by hash id
    #[clap(long, short)]
    id: Option<String>,

    #[clap(long, short)]
    output: Option<PathBuf>,

    #[clap(long, short)]
    range: Option<RangeInput>,
}

/// implement the smudge file command.
/// If the filename is provided, we read the file.
/// Otherwise we read from stdin
pub async fn smudge_command_impl(
    translator: &PointerFileTranslator,
    args: &SmudgeArgs,
) -> errors::Result<()> {
    // Set up the output.
    let mut output: Box<dyn Write + Send + Sync> = match &args.output {
        Some(filename) => {
            let f = File::create(filename)?;
            Box::new(BufWriter::new(f))
        }
        None => Box::new(stdout()),
    };

    let range = args.range.as_ref().map(|range| (range.0, range.1));

    if let Some(hash) = &args.id {
        let hash = MerkleHash::from_hex(hash)?;

        translator
            .smudge_file_from_hash(args.filename.clone(), &hash, &mut output, range)
            .await?;
    } else {
        // read the pointer from either filename or stdin
        let file: Box<dyn Read + Send + Sync> = match &args.filename {
            Some(filename) => {
                // fail fast if file does not exist
                if !filename.exists() {
                    return Err(FileNotFound(filename.clone()));
                }
                let f = File::open(filename)?;
                Box::new(BufReader::new(f))
            }
            None => Box::new(stdin()),
        };

        let async_file = AsyncFileIterator::new(file, GIT_MAX_PACKET_SIZE);

        translator
            .smudge_file(
                &PathBuf::new(),
                async_file,
                &mut output,
                args.passthrough,
                range,
            )
            .await?;
    }

    Ok(())
}

pub async fn smudge_command(config: &XetConfig, args: &SmudgeArgs) -> errors::Result<()> {
    // The V1 path is the only one not needing
    if let Some(path) = config.repo_path_if_present.as_ref() {
        if get_mdb_version(path, config)? == ShardVersion::V1 {
            let pft = PointerFileTranslator::v1_from_config(config).await?;
            let ret = smudge_command_impl(&pft, args).await;

            if let Err(HashNotFound) = ret {
                // 'merkledb extract-git'
                mdbv1::merge_merkledb_from_git(
                    config,
                    &mdbv1::find_git_db(None)?,
                    GIT_NOTES_MERKLEDB_V1_REF_NAME,
                )
                .await
                .map_err(GitXetRepoError::from)?;

                pft.reload_mdb().await;

                return smudge_command_impl(&pft, args).await;
            }

            return Ok(());
        }
    }

    let pft = PointerFileTranslator::v2_from_config_smudge_only(config).await?;
    smudge_command_impl(&pft, args).await
}

/// The error for parsing our custom range type
#[non_exhaustive]
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RangeInputError {
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("{0} is an invalid count of values for a range. Must contain 2 values only.")]
    InvalidArgumentCount(usize),

    #[error("Invalid range: end ({1}) must be greater than or equal to start ({0}).")]
    InvalidRange(usize, usize),
}

/// A custom type for our range input, takes a comma-delimited string and parses out
/// a start and end of the range. The range is inclusive.
#[derive(Debug, PartialEq, Eq)]
struct RangeInput(usize, usize);

impl FromStr for RangeInput {
    type Err = RangeInputError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let vals: Vec<&str> = s.trim().split(',').collect();

        if vals.len() != 2 {
            return Err(RangeInputError::InvalidArgumentCount(vals.len()));
        }
        let val1 = vals[0].trim().parse::<usize>()?;
        let val2 = vals[1].trim().parse::<usize>()?;

        if val2 < val1 {
            return Err(RangeInputError::InvalidRange(val1, val2));
        }

        Ok(Self(val1, val2))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_input_parsing() {
        // positive test cases
        assert_eq!(RangeInput::from_str("100,200"), Ok(RangeInput(100, 200)));
        assert_eq!(RangeInput::from_str("100, 200"), Ok(RangeInput(100, 200)));
        assert_eq!(
            RangeInput::from_str(" 100, 200   "),
            Ok(RangeInput(100, 200))
        );

        // error conditions
        assert!(RangeInput::from_str("-1, 2").is_err());
        assert!(RangeInput::from_str("").is_err());
        assert!(RangeInput::from_str("100").is_err());
        assert!(RangeInput::from_str("100,200,300").is_err());
        assert!(RangeInput::from_str("trash,200").is_err());
        assert!(RangeInput::from_str("trash,garbage").is_err());
        assert!(RangeInput::from_str("200,100").is_err());
    }
}
