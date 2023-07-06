use crate::error::{MDBShardError, Result};
use crate::{shard_file::MDBShardInfo, utils::parse_shard_filename};
use merklehash::{compute_data_hash, MerkleHash};
use std::io::{BufReader, Read, Seek};
use std::path::{Path, PathBuf};
use tracing::{error, info, warn};

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
            Err(MDBShardError::BadFilename(format!(
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
                return Err(MDBShardError::BadFilename(format!(
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

    pub fn get_reader(&self) -> Result<BufReader<std::fs::File>> {
        Ok(BufReader::with_capacity(
            2048,
            std::fs::File::open(&self.path)?,
        ))
    }

    pub fn verify_mdb_shard_debug_only(&self) {
        #[cfg(debug_assertions)]
        {
            self.verify_shard_integrity();
        }
    }

    pub fn verify_shard_integrity(&self) {
        info!("Verifying shard integrity for shard {:?}", &self.path);

        info!("Header : {:?}", self.shard.header);
        info!("Metadata : {:?}", self.shard.metadata);

        let mut reader = self
            .get_reader()
            .map_err(|e| {
                error!("Error getting reader: {e:?}");
                e
            })
            .unwrap();

        let mut data = Vec::with_capacity(self.shard.num_bytes() as usize);
        reader.read_to_end(&mut data).unwrap();

        // Check the hash
        let hash = compute_data_hash(&data[..]);

        // Check the parsed shard from the filename.
        if let Some(parsed_shard_hash) = parse_shard_filename(&self.path.to_string_lossy()) {
            if hash != parsed_shard_hash {
                error!("Hash parsed from filename does not match the computed hash; hash from filename={parsed_shard_hash:?}, hash of file={hash:?}");
            }
        } else {
            warn!("Unable to obtain hash from filename.");
        }

        // Check the file info sections
        reader.rewind().unwrap();

        let fir = MDBShardInfo::read_file_info_ranges(&mut reader)
            .map_err(|e| {
                error!("Error reading file info ranges : {e:?}");
                e
            })
            .unwrap();

        debug_assert_eq!(fir.len() as u64, self.shard.metadata.file_lookup_num_entry);
        info!("Integrity test passed for shard {:?}", &self.path);

        // TODO: More parts; but this will at least succeed on the server end.
    }
}
