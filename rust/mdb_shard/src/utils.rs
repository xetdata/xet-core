use lazy_static::lazy_static;
use merklehash::MerkleHash;
use regex::Regex;
use std::{ffi::OsStr, ops::Deref, path::Path};
use uuid::Uuid;

lazy_static! {
    static ref MERKLE_DB_FILE_PATTERN: Regex =
        Regex::new(r"((^)|(.*/))(?P<hash>[0-9a-fA-F]{64})\.mdb$").unwrap();
}

/// Parses a shard filename.  If the filename matches the shard filename pattern,
/// then Some(hash) is returned, where hash is the CAS hash of the merkledb file.  
/// If the filename does not match, None is returned.
#[inline]
pub fn parse_shard_filename<P: AsRef<Path>>(filename: P) -> Option<MerkleHash> {
    let filename: &Path = filename.as_ref();
    let filename = filename.to_str().unwrap_or("");

    MERKLE_DB_FILE_PATTERN
        .captures(filename)
        .map(|capture| MerkleHash::from_hex(capture.name("hash").unwrap().as_str()).unwrap())
}

#[inline]
pub fn truncate_hash(hash: &MerkleHash) -> u64 {
    hash.deref()[0]
}

pub fn shard_file_name(hash: &MerkleHash) -> String {
    format!("{}.mdb", hash.hex())
}

pub fn temp_shard_file_name() -> String {
    let uuid = Uuid::new_v4();
    format!(".{uuid}.mdb_temp")
}

pub fn is_temp_shard_file(p: &Path) -> bool {
    p.file_name()
        .unwrap_or_else(|| OsStr::new(""))
        .to_str()
        .unwrap_or("")
        .ends_with("mdb_temp")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard_format::test_routines::rng_hash;

    #[test]
    fn test_regex() {
        let mh = rng_hash(0);

        assert!(parse_shard_filename(&format!("/Users/me/temp/{}.mdb", mh.hex())).is_some());
        assert!(parse_shard_filename(&format!("{}.mdb", mh.hex())).is_some());
        assert!(parse_shard_filename(&format!("other_{}.mdb", mh.hex())).is_none());
    }
}
