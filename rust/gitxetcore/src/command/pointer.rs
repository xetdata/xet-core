use std::fs::File;
use std::io::{stdin, BufReader, Read};
use std::path::PathBuf;

use clap::Args;

use merkledb::prelude_v2::*;
use merkledb::{chunk_target_default, MerkleMemDB};
use pointer_file::PointerFile;

use crate::errors;
use crate::errors::GitXetRepoError::FileNotFound;

#[derive(Args, Debug)]
/// Generates a pointer file to stdout given a filename as an input
/// If a filename is not provided, the file is read from stdin.
///
/// Inverse of the smudge command.
///
/// ```ignore
/// # input and output should be the same file
/// git xet pointer -f input | git xet --cas smudge > output
/// ```
pub struct PointerArgs {
    #[clap(long, short)]
    filename: Option<PathBuf>,
}

/// simply synchronously takes a file and chunks it,
/// and returns a pointer file.
pub fn file_to_pointer(filename_str: &str, reader: &mut impl Read) -> anyhow::Result<PointerFile> {
    // create an empty merkledb and just chunk the file
    let mut mdb: MerkleMemDB = MerkleMemDB::default();
    let nodes: Vec<_> = chunk_target_default(reader)
        .into_iter()
        .map(|x| mdb.add_chunk(&x).0)
        .collect();
    let filenode = mdb.merge_to_file(&nodes);

    // build the pointer file and return it
    let pointer_file: PointerFile =
        PointerFile::init_from_info(filename_str, &filenode.hash().hex(), filenode.len() as u64);
    Ok(pointer_file)
}

/// implement the pointer file command.
/// If the filename is provided, we read the file.
/// Otherwise we read from stdin
pub fn pointer_command(args: &PointerArgs) -> errors::Result<()> {
    // create the pointer from either fileanme or stdin
    let pointer = match &args.filename {
        Some(filename) => {
            // fail fast if file does not exist
            if !filename.exists() {
                return Err(FileNotFound(filename.clone()));
            }
            let f = File::open(filename)?;
            let mut reader = BufReader::new(f);
            file_to_pointer(&format!("{filename:?}"), &mut reader)?
        }
        None => file_to_pointer("<stdin>", &mut stdin())?,
    };

    // print the result
    println!("{}", pointer.to_string());
    Ok(())
}
