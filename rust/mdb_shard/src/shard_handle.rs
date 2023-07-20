use crate::error::{MDBShardError, Result};
use crate::file_structs::{FileDataSequenceEntry, MDBFileInfo};
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
    pub fn new(shard_hash: MerkleHash, path: PathBuf, shard: MDBShardInfo) -> Result<Self> {
        let s = Self {
            shard_hash,
            path,
            shard,
        };

        s.verify_shard_integrity_debug_only();
        Ok(s)
    }

    /// Loads the MDBShardFile struct from
    ///
    pub fn load_from_file(path: &Path) -> Result<Self> {
        if let Some(shard_hash) = parse_shard_filename(path.to_str().unwrap()) {
            let mut f = std::fs::File::open(path)?;
            Ok(Self::new(
                shard_hash,
                std::fs::canonicalize(path)?,
                MDBShardInfo::load_from_file(&mut f)?,
            )?)
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

        #[cfg(debug_assertions)]
        {
            // In debug mode, verify all shards on loading to catch errors earlier.
            for s in ret.iter() {
                s.verify_shard_integrity_debug_only();
            }
        }
        Ok(ret)
    }

    #[inline]
    pub fn get_reader(&self) -> Result<BufReader<std::fs::File>> {
        Ok(BufReader::with_capacity(
            2048,
            std::fs::File::open(&self.path)?,
        ))
    }

    #[inline]
    pub fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<MDBFileInfo>> {
        self.shard
            .get_file_reconstruction_info(&mut self.get_reader()?, file_hash)
    }

    #[inline]
    pub fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> Result<Option<(usize, FileDataSequenceEntry)>> {
        self.shard
            .chunk_hash_dedup_query(&mut self.get_reader()?, query_hashes)
    }

    #[inline]
    pub fn verify_shard_integrity_debug_only(&self) {
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
        assert_eq!(hash, self.shard_hash);

        // Check the parsed shard from the filename.
        let parsed_shard_hash = parse_shard_filename(&self.path).unwrap();
        assert_eq!(hash, parsed_shard_hash);

        reader.rewind().unwrap();

        // Check the parsed shard from the filename.
        if let Some(parsed_shard_hash) = parse_shard_filename(&self.path) {
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
