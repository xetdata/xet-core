use lazy_static::lazy_static;
use merklehash::MerkleHash;
use regex::Regex;
use std::{ffi::OsStr, ops::Deref, path::Path};
use uuid::Uuid;

lazy_static! {
    static ref MERKLE_DB_FILE_PATTERN: Regex = Regex::new(
        r"((^)|(.*/))(S(?P<stage_idx>[0-9a-fA-F]{8,})-)?(?P<hash>[0-9a-fA-F]{64})\.mdb$"
    )
    .unwrap();
}

/// then Some(hash) is returned, where hash is the CAS hash of the merkledb file.  
/// If the filename does not match, None is returned.
/// If the shard is a staged shard with an index, then that is returned as the second part.
#[inline]
pub fn parse_shard_filename(filename: &str) -> Option<(MerkleHash, Option<u64>)> {
    MERKLE_DB_FILE_PATTERN.captures(filename).map(|capture| {
        (
            MerkleHash::from_hex(capture.name("hash").unwrap().as_str()).unwrap(),
            capture
                .name("stage_idx")
                .map(|s| u64::from_str_radix(s.as_str(), 16).unwrap()),
        )
    })
}

#[inline]
pub fn truncate_hash(hash: &MerkleHash) -> u64 {
    hash.deref()[0]
}

pub fn shard_file_name(hash: &MerkleHash) -> String {
    format!("{}.mdb", hash.hex())
}

pub fn staged_shard_file_name(hash: &MerkleHash, creation_index: Option<u64>) -> String {
    if let Some(idx) = creation_index {
        // Encode creation_index in hex
        format!("S{:08x}-{}.mdb", idx, hash.hex())
    } else {
        shard_file_name(hash)
    }
}

pub fn temp_shard_file_name() -> String {
    let uuid = Uuid::new_v4();
    format!(".{uuid}.mdb_temp")
}

pub fn is_shard_file<P: AsRef<Path>>(p: P) -> bool {
    let p: &Path = p.as_ref();
    let name = p
        .file_name()
        .unwrap_or_else(|| OsStr::new(""))
        .to_str()
        .unwrap_or("");

    if !name.ends_with("mdb") {
        return false;
    }

    parse_shard_filename(name).is_some()
}

pub fn is_temp_shard_file<P: AsRef<Path>>(p: P) -> bool {
    let p: &Path = p.as_ref();
    p.file_name()
        .unwrap_or_else(|| OsStr::new(""))
        .to_str()
        .unwrap_or("")
        .ends_with("mdb_temp")
}

pub fn is_staged_shard_file<P: AsRef<Path>>(p: P) -> bool {
    let p: &Path = p.as_ref();
    let name = p
        .file_name()
        .unwrap_or_else(|| OsStr::new(""))
        .to_str()
        .unwrap_or("");

    if !name.ends_with("mdb") || !name.starts_with('S') {
        return false;
    }

    matches!(parse_shard_filename(name), Some((_, Some(_))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard_file::test_routines::rng_hash;
    use crate::utils::is_shard_file;

    #[test]
    fn test_regex() {
        let mh = rng_hash(0);

        assert!(parse_shard_filename(&format!("/Users/me/temp/{}.mdb", mh.hex())).is_some());
        assert!(parse_shard_filename(&format!("{}.mdb", mh.hex())).is_some());
        assert!(parse_shard_filename(&format!("other_{}.mdb", mh.hex())).is_none());
    }

    #[test]

    fn test_staging_number() {
        for n in [0, 18, 89, 135, 938809, 39399339] {
            let mh = rng_hash(n);
            let filename = staged_shard_file_name(&mh, Some(n));
            let p = parse_shard_filename(&filename).unwrap();
            assert_eq!(p.0, mh);
            assert_eq!(p.1, Some(n));

            assert!(is_shard_file(&filename));
            assert!(is_staged_shard_file(&filename));
        }
    }
}
