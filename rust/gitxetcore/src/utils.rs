use crate::errors;
use crate::git_integration::GitRepo;

use merklehash::{DataHashHexParseError, MerkleHash};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::{io, io::Write};
use tempfile::NamedTempFile;

/// Construct a file name for a MDBShard stored under cache and session dir.
pub fn local_shard_name(hash: &MerkleHash) -> PathBuf {
    PathBuf::from(hash.to_string()).with_extension("mdb")
}

/// Construct a file name for a MDBShardMeta stored under session dir.
#[allow(dead_code)]
pub fn local_meta_name(hash: &MerkleHash) -> PathBuf {
    PathBuf::from(hash.to_string()).with_extension("meta")
}

pub fn is_shard_file(path: &Path) -> bool {
    path.extension().and_then(OsStr::to_str) == Some("mdb")
}

pub fn is_meta_file(path: &Path) -> bool {
    path.extension().and_then(OsStr::to_str) == Some("meta")
}

pub fn shard_to_meta(path: &Path) -> PathBuf {
    path.with_extension("meta")
}

pub fn meta_to_shard(path: &Path) -> PathBuf {
    path.with_extension("mdb")
}

pub fn shard_path_to_hash(path: &Path) -> Result<MerkleHash, DataHashHexParseError> {
    let hash = MerkleHash::from_hex(
        path.with_extension("")
            .file_name()
            .unwrap_or_default()
            .to_str()
            .unwrap_or_default(),
    )?;

    Ok(hash)
}

/// Write all bytes
pub fn write_all_file_safe(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if !path.as_os_str().is_empty() {
        let dir = path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unable to find parent path from {path:?}"),
            )
        })?;

        // Make sure dir exists.
        if !dir.exists() {
            std::fs::create_dir_all(dir)?;
        }

        let mut tempfile = create_temp_file(dir, "")?;
        tempfile.write_all(bytes)?;
        tempfile.persist(path).map_err(|e| e.error)?;
    }

    Ok(())
}

pub fn create_temp_file(dir: &Path, suffix: &str) -> io::Result<NamedTempFile> {
    let tempfile = tempfile::Builder::new()
        .prefix(&format!("{}.", std::process::id()))
        .suffix(suffix)
        .tempfile_in(dir)?;

    Ok(tempfile)
}

/// Walks the ref notes of head repo, takes in notes that do not exist in base.
pub async fn merge_git_notes(base: &Path, head: &Path, notesref: &str) -> errors::Result<()> {
    let base = GitRepo::open(Some(base))?;

    let base_notes_oids = base
        .xet_notes_name_iterator(notesref)?
        .collect::<HashSet<_>>();

    let repo = GitRepo::open(Some(head))?;

    for (oid, blob) in repo.xet_notes_content_iterator(notesref)? {
        if !base_notes_oids.contains(&oid) {
            repo.add_xet_note(&blob)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use merklehash::*;
    use std::fs;
    use std::str::FromStr;
    use tempfile::TempDir;

    use crate::utils::*;

    #[test]
    fn test_file_name_utils() -> Result<()> {
        let hash_str: String = "1".repeat(64);
        let hash = DataHash::from_hex(&hash_str)?;

        let shard_file_name = local_shard_name(&hash);
        let meta_file_name = local_meta_name(&hash);

        let dirs = ["xet", "..", ".git/xet", "asdi/../evca/..", "/"];

        for dir in dirs.iter().flat_map(|d| PathBuf::from_str(d)) {
            let shard = dir.join(&shard_file_name);
            let meta = dir.join(&meta_file_name);

            assert!(is_shard_file(&shard));
            assert!(is_meta_file(&meta));
            assert_eq!(shard_to_meta(&shard), meta);
            assert_eq!(meta_to_shard(&meta), shard);
        }

        Ok(())
    }

    #[test]
    fn test_small_file_write() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let bytes = vec![1u8; 1000];
        let file_name = tmp_dir.path().join("data");

        write_all_file_safe(&file_name, &bytes)?;

        assert_eq!(fs::read(file_name)?, bytes);

        Ok(())
    }
}
