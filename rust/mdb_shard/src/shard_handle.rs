use crate::{shard_file::MDBShardInfo, utils::parse_shard_filename};
use merkledb::error::{MerkleDBError, Result};
use merklehash::MerkleHash;
use std::path::{Path, PathBuf};
use tracing::info;

/// When a specific implementation of the  
///
#[derive(Debug, Clone, Default)]
pub struct MDBShardFile {
    pub shard_hash: MerkleHash,
    pub path: PathBuf,
    pub shard: MDBShardInfo,
}

impl MDBShardFile {
    /// Loads the MDBShardFile struct from
    ///
    pub fn load_from_file(path: &Path) -> Result<Self> {
        if let Some(shard_hash) = parse_shard_filename(path.to_str().unwrap()) {
            let mut f = std::fs::File::open(path)?;
            Ok(Self {
                shard_hash,
                path: std::fs::canonicalize(path)?,
                shard: MDBShardInfo::load_from_file(&mut f)?,
            })
        } else {
            Err(MerkleDBError::BadFilename(format!(
                "{path:?} not a valid MerkleDB filename."
            )))
        }
    }

    pub fn load_all(path: &Path) -> Result<Vec<Self>> {
        let mut shards = Vec::new();

        if path.is_dir() {
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                if let Some(file_name) = entry.file_name().to_str() {
                    if let Some(h) = parse_shard_filename(file_name) {
                        shards.push((h, std::fs::canonicalize(entry.path())?));
                    }
                    info!("Found shard file '{file_name:?}'.");
                }
            }
        } else if let Some(file_name) = path.to_str() {
            if let Some(h) = parse_shard_filename(file_name) {
                shards.push((h, std::fs::canonicalize(path)?));
                info!("Registerd shard file '{file_name:?}'.");
            } else {
                return Err(MerkleDBError::BadFilename(format!(
                    "Filename {file_name} not valid shard file name."
                )));
            }
        }

        let mut ret = Vec::with_capacity(shards.len());

        for (shard_hash, path) in shards {
            let mut f = std::fs::File::open(&path)?;

            ret.push(MDBShardFile {
                shard_hash,
                path,
                shard: MDBShardInfo::load_from_file(&mut f)?,
            });
        }
        Ok(ret)
    }
}
