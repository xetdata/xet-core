use crate::error::{MDBShardError, Result};
use crate::serialization_utils::*;
use merkledb::MerkleMemDB;
use merklehash::MerkleHash;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::sync::Arc;
use std::vec;
use tracing::info;

use crate::cas_structs::*;
use crate::file_structs::*;
use crate::shard_in_memory::MDBInMemoryShard;
use crate::shard_version;
use crate::utils::truncate_hash;

pub const MDB_SHARD_TARGET_SIZE: u64 = 64 * 1024 * 1024;
pub const MDB_SHARD_MIN_TARGET_SIZE: u64 = 48 * 1024 * 1024;

// Same size for FileDataSequenceHeader and FileDataSequenceEntry
const MDB_FILE_INFO_ENTRY_SIZE: u64 = (size_of::<[u64; 4]>() + 4 * size_of::<u32>()) as u64;
// Same size for CASChunkSequenceHeader and CASChunkSequenceEntry
const MDB_CAS_INFO_ENTRY_SIZE: u64 = (size_of::<[u64; 4]>() + 4 * size_of::<u32>()) as u64;
const MDB_SHARD_FOOTER_SIZE: i64 = size_of::<MDBShardFileFooter>() as i64;

// At the start of each shard file, insert "MerkleDB Shard" plus a magic-number sequence of random bytes to ensure
// that we are able to quickly identify a CAS file as a shard file.
const MDB_SHARD_HEADER_TAG: [u8; 32] = [
    b'M', b'e', b'r', b'k', b'l', b'e', b'D', b'B', b' ', b'S', b'h', b'a', b'r', b'd', 0, 85, 105,
    103, 69, 106, 123, 129, 87, 131, 165, 189, 217, 92, 205, 209, 74, 169,
];

#[derive(Clone, Debug)]
pub struct MDBShardFileHeader {
    // Header to be determined?  "XetHub MDB Shard File Version 1"
    pub tag: [u8; 32],
    pub version: u64,
    pub footer_size: u64,
}

impl Default for MDBShardFileHeader {
    fn default() -> Self {
        Self {
            tag: MDB_SHARD_HEADER_TAG,
            version: shard_version::MDB_SHARD_HEADER_VERSION,
            footer_size: MDB_SHARD_FOOTER_SIZE as u64,
        }
    }
}

impl MDBShardFileHeader {
    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize> {
        writer.write_all(&MDB_SHARD_HEADER_TAG)?;
        write_u64(writer, self.version)?;
        write_u64(writer, self.footer_size)?;

        Ok(size_of::<MDBShardFileHeader>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
        let mut tag = [0u8; 32];
        reader.read_exact(&mut tag)?;

        if tag != MDB_SHARD_HEADER_TAG {
            return Err(MDBShardError::ShardVersionError(
                "File does not appear to be a valid Merkle DB Shard file (Wrong Magic Number)."
                    .to_owned(),
            ));
        }

        Ok(Self {
            tag,
            version: read_u64(reader)?,
            footer_size: read_u64(reader)?,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct MDBShardFileFooter {
    pub version: u64,
    pub file_info_offset: u64,
    pub file_lookup_offset: u64,
    pub file_lookup_num_entry: u64,
    pub cas_info_offset: u64,
    pub cas_lookup_offset: u64,
    pub cas_lookup_num_entry: u64,
    pub chunk_lookup_offset: u64,
    pub chunk_lookup_num_entry: u64,
    _buffer: [u64; 8], // More locations to stick in here if needed.
    pub materialized_bytes: u64,
    pub stored_bytes: u64,
    pub footer_offset: u64, // Always last.
}

impl Default for MDBShardFileFooter {
    fn default() -> Self {
        Self {
            version: shard_version::MDB_SHARD_FOOTER_VERSION,
            file_info_offset: 0,
            file_lookup_offset: 0,
            file_lookup_num_entry: 0,
            cas_info_offset: 0,
            cas_lookup_offset: 0,
            cas_lookup_num_entry: 0,
            chunk_lookup_offset: 0,
            chunk_lookup_num_entry: 0,
            _buffer: [0u64; 8],
            materialized_bytes: 0,
            stored_bytes: 0,
            footer_offset: 0,
        }
    }
}

impl MDBShardFileFooter {
    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize> {
        write_u64(writer, self.version)?;
        write_u64(writer, self.file_info_offset)?;
        write_u64(writer, self.file_lookup_offset)?;
        write_u64(writer, self.file_lookup_num_entry)?;
        write_u64(writer, self.cas_info_offset)?;
        write_u64(writer, self.cas_lookup_offset)?;
        write_u64(writer, self.cas_lookup_num_entry)?;
        write_u64(writer, self.chunk_lookup_offset)?;
        write_u64(writer, self.chunk_lookup_num_entry)?;
        write_u64s(writer, &self._buffer)?;
        write_u64(writer, self.materialized_bytes)?;
        write_u64(writer, self.stored_bytes)?;
        write_u64(writer, self.footer_offset)?;

        Ok(size_of::<MDBShardFileFooter>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
        let mut obj = Self {
            version: read_u64(reader)?,
            file_info_offset: read_u64(reader)?,
            file_lookup_offset: read_u64(reader)?,
            file_lookup_num_entry: read_u64(reader)?,
            cas_info_offset: read_u64(reader)?,
            cas_lookup_offset: read_u64(reader)?,
            cas_lookup_num_entry: read_u64(reader)?,
            chunk_lookup_offset: read_u64(reader)?,
            chunk_lookup_num_entry: read_u64(reader)?,
            ..Default::default()
        };
        read_u64s(reader, &mut obj._buffer)?;
        obj.materialized_bytes = read_u64(reader)?;
        obj.stored_bytes = read_u64(reader)?;
        obj.footer_offset = read_u64(reader)?;

        Ok(obj)
    }
}

/// File info.  This is a list of the file hash content to be downloaded.
///
/// Each file consists of a FileDataSequenceHeader following
/// a sequence of FileDataSequenceEntry.
///
/// [
///     FileDataSequenceHeader, // u32 index in File lookup directs here.
///     [
///         FileDataSequenceEntry,
///     ],
/// ], // Repeats per file.
///
/// ----------------------------------------------------------------------------
///  
/// File info lookup.  This is a lookup of a truncated file hash to the
/// location index in the File info section.
///
/// Sorted Vec<(u64, u32)> on the u64.
///
/// The first entry is the u64 truncated file hash, and the next entry is the
/// index in the file info section of the element that starts the file reconstruction section.
///
/// ----------------------------------------------------------------------------
///
/// CAS info.  This is a list of chunks in order of appearance in the CAS chunks.
///
/// Each CAS consists of a CASChunkSequenceHeader following
/// a sequence of CASChunkSequenceEntry.
///
/// [
///     CASChunkSequenceHeader, // u32 index in CAS lookup directs here.
///     [
///         CASChunkSequenceEntry, // (u32, u32) index in Chunk lookup directs here.
///     ],
/// ], // Repeats per CAS.
///
/// ----------------------------------------------------------------------------
///
/// CAS info lookup.  This is a lookup of a truncated CAS hash to the
/// location index in the CAS info section.
///
/// Sorted Vec<(u64, u32)> on the u64.
///
/// The first entry is the u64 truncated CAS block hash, and the next entry is the
/// index in the cas info section of the element that starts the cas entry section.
///
/// ----------------------------------------------------------------------------
///
/// Chunk info lookup. This is a lookup of a truncated CAS chunk hash to the
/// location in the CAS info section.
///
/// Sorted Vec<(u64, (u32, u32))> on the u64.
///
/// The first entry is the u64 truncated CAS chunk in a CAS block, the first u32 is the index
/// in the CAS info section that is the start of the CAS block, and the subsequent u32 gives
/// the offset index of the chunk in that CAS block.

#[derive(Clone, Default, Debug)]
pub struct MDBShardInfo {
    pub header: MDBShardFileHeader,
    pub metadata: MDBShardFileFooter,
}

impl MDBShardInfo {
    pub fn load_from_file<R: Read + Seek>(reader: &mut R) -> Result<Self> {
        let mut obj = Self::default();

        // Move cursor to beginning of shard file.
        reader.rewind()?;
        obj.header = MDBShardFileHeader::deserialize(reader)?;

        // Move cursor to end of shard file minus footer size.
        reader.seek(SeekFrom::End(-MDB_SHARD_FOOTER_SIZE))?;
        obj.metadata = MDBShardFileFooter::deserialize(reader)?;

        Ok(obj)
    }

    pub fn serialize_from_v1<W: Write>(writer: &mut W, mdb: &MerkleMemDB) -> Result<Self> {
        let mdb = MDBInMemoryShard::convert_from_v1(mdb)?;
        MDBShardInfo::serialize_from(writer, &mdb)
    }

    pub fn serialize_from<W: Write>(writer: &mut W, mdb: &MDBInMemoryShard) -> Result<Self> {
        let mut shard = MDBShardInfo::default();

        let mut bytes_pos: usize = 0;

        // Write shard header.
        bytes_pos += shard.header.serialize(writer)?;

        // Write file info.
        shard.metadata.file_info_offset = bytes_pos as u64;
        let ((file_lookup_keys, file_lookup_vals), bytes_written) =
            Self::convert_and_save_file_info(writer, &mdb.file_content)?;
        bytes_pos += bytes_written;

        // Write file info lookup table.
        shard.metadata.file_lookup_offset = bytes_pos as u64;
        shard.metadata.file_lookup_num_entry = file_lookup_keys.len() as u64;
        for (&e1, &e2) in file_lookup_keys.iter().zip(file_lookup_vals.iter()) {
            write_u64(writer, e1)?;
            write_u32(writer, e2)?;
        }
        bytes_pos +=
            size_of::<u64>() * file_lookup_keys.len() + size_of::<u32>() * file_lookup_vals.len();

        // Release memory.
        drop(file_lookup_keys);
        drop(file_lookup_vals);

        // Write CAS info.
        shard.metadata.cas_info_offset = bytes_pos as u64;
        let (
            (cas_lookup_keys, cas_lookup_vals),
            (chunk_lookup_keys, chunk_lookup_vals),
            bytes_written,
        ) = Self::convert_and_save_cas_info(writer, &mdb.cas_content)?;
        bytes_pos += bytes_written;

        // Write cas info lookup table.
        shard.metadata.cas_lookup_offset = bytes_pos as u64;
        shard.metadata.cas_lookup_num_entry = cas_lookup_keys.len() as u64;
        for (&e1, &e2) in cas_lookup_keys.iter().zip(cas_lookup_vals.iter()) {
            write_u64(writer, e1)?;
            write_u32(writer, e2)?;
        }
        bytes_pos +=
            size_of::<u64>() * cas_lookup_keys.len() + size_of::<u32>() * cas_lookup_vals.len();

        // Write chunk lookup table.
        shard.metadata.chunk_lookup_offset = bytes_pos as u64;
        shard.metadata.chunk_lookup_num_entry = chunk_lookup_keys.len() as u64;
        for (&e1, &e2) in chunk_lookup_keys.iter().zip(chunk_lookup_vals.iter()) {
            write_u64(writer, e1)?;
            write_u32(writer, e2.0)?;
            write_u32(writer, e2.1)?;
        }
        bytes_pos +=
            size_of::<u64>() * chunk_lookup_keys.len() + size_of::<u64>() * chunk_lookup_vals.len();

        // Update repo size information.
        shard.metadata.materialized_bytes = mdb.materialized_bytes();
        shard.metadata.stored_bytes = mdb.stored_bytes();

        // Update footer offset.
        shard.metadata.footer_offset = bytes_pos as u64;

        // Write shard footer.
        shard.metadata.serialize(writer)?;

        Ok(shard)
    }

    #[allow(clippy::type_complexity)]
    fn convert_and_save_file_info<W: Write>(
        writer: &mut W,
        file_content: &BTreeMap<MerkleHash, MDBFileInfo>,
    ) -> Result<(
        (Vec<u64>, Vec<u32>), // File Lookup Info
        usize,                // Bytes used for File Content Info
    )> {
        // File info lookup table.
        let mut file_lookup_keys = Vec::<u64>::with_capacity(file_content.len());
        let mut file_lookup_vals = Vec::<u32>::with_capacity(file_content.len());

        let mut index: u32 = 0;
        let mut bytes_written = 0;

        for (file_hash, content) in file_content {
            file_lookup_keys.push(truncate_hash(file_hash));
            file_lookup_vals.push(index);

            bytes_written += content.metadata.serialize(writer)?;

            for file_segment in content.segments.iter() {
                bytes_written += file_segment.serialize(writer)?;
            }

            index += 1 + content.metadata.num_entries;
        }

        // Serialize a single block of 00 bytes as a guard for sequential reading.
        bytes_written += FileDataSequenceHeader::default().serialize(writer)?;

        // No need to sort because BTreeMap is ordered and we truncate by the first 8 bytes.
        Ok(((file_lookup_keys, file_lookup_vals), bytes_written))
    }

    #[allow(clippy::type_complexity)]
    fn convert_and_save_cas_info<W: Write>(
        writer: &mut W,
        cas_content: &BTreeMap<MerkleHash, Arc<MDBCASInfo>>,
    ) -> Result<(
        (Vec<u64>, Vec<u32>),        // CAS Lookup Info
        (Vec<u64>, Vec<(u32, u32)>), // Chunk Lookup Info
        usize,                       // Bytes used for CAS Content Info
    )> {
        // CAS info lookup table.
        let mut cas_lookup_keys = Vec::<u64>::with_capacity(cas_content.len());
        let mut cas_lookup_vals = Vec::<u32>::with_capacity(cas_content.len());

        // Chunk lookup table.
        let mut chunk_lookup_keys = Vec::<u64>::with_capacity(cas_content.len()); // may grow
        let mut chunk_lookup_vals = Vec::<(u32, u32)>::with_capacity(cas_content.len()); // may grow

        let mut index: u32 = 0;
        let mut bytes_written = 0;

        for (cas_hash, content) in cas_content {
            cas_lookup_keys.push(truncate_hash(cas_hash));
            cas_lookup_vals.push(index);

            bytes_written += content.metadata.serialize(writer)?;

            for (i, chunk) in content.chunks.iter().enumerate() {
                bytes_written += chunk.serialize(writer)?;

                chunk_lookup_keys.push(truncate_hash(&chunk.chunk_hash));
                chunk_lookup_vals.push((index, i as u32));
            }

            index += 1 + content.chunks.len() as u32;
        }

        // Serialize a single block of 00 bytes as a guard for sequential reading.
        bytes_written += CASChunkSequenceHeader::default().serialize(writer)?;

        // No need to sort cas_lookup_ because BTreeMap is ordered and we truncate by the first 8 bytes.

        // Sort chunk lookup table by key.
        let mut chunk_lookup_combined = chunk_lookup_keys
            .iter()
            .zip(chunk_lookup_vals.iter())
            .collect::<Vec<_>>();

        chunk_lookup_combined.sort_unstable_by_key(|&(k, _)| k);

        Ok((
            (cas_lookup_keys, cas_lookup_vals),
            (
                chunk_lookup_combined.iter().map(|&(k, _)| *k).collect(),
                chunk_lookup_combined.iter().map(|&(_, v)| *v).collect(),
            ),
            bytes_written,
        ))
    }

    pub fn get_file_info_index_by_hash<R: Read + Seek>(
        &self,
        reader: &mut R,
        file_hash: &MerkleHash,
        dest_indices: &mut [u32; 8],
    ) -> Result<usize> {
        let num_indices = search_on_sorted_u64s(
            reader,
            self.metadata.file_lookup_offset,
            self.metadata.file_lookup_num_entry,
            truncate_hash(file_hash),
            read_u32::<R>,
            dest_indices,
        )?;

        // Assume no more than 8 collisions.
        if num_indices < dest_indices.len() {
            Ok(num_indices)
        } else {
            Err(MDBShardError::TruncatedHashCollisionError(truncate_hash(
                file_hash,
            )))
        }
    }

    pub fn get_cas_info_index_by_hash<R: Read + Seek>(
        &self,
        reader: &mut R,
        cas_hash: &MerkleHash,
        dest_indices: &mut [u32; 8],
    ) -> Result<usize> {
        let num_indices = search_on_sorted_u64s(
            reader,
            self.metadata.cas_lookup_offset,
            self.metadata.cas_lookup_num_entry,
            truncate_hash(cas_hash),
            read_u32::<R>,
            dest_indices,
        )?;

        // Assume no more than 8 collisions.
        if num_indices < dest_indices.len() {
            Ok(num_indices)
        } else {
            Err(MDBShardError::TruncatedHashCollisionError(truncate_hash(
                cas_hash,
            )))
        }
    }

    pub fn get_cas_info_index_by_chunk<R: Read + Seek>(
        &self,
        reader: &mut R,
        chunk_hash: &MerkleHash,
        dest_indices: &mut [(u32, u32); 8],
    ) -> Result<usize> {
        let num_indices = search_on_sorted_u64s(
            reader,
            self.metadata.chunk_lookup_offset,
            self.metadata.chunk_lookup_num_entry,
            truncate_hash(chunk_hash),
            |reader| (Ok((read_u32(reader)?, read_u32(reader)?))),
            dest_indices,
        )?;

        // Chunk lookup hashes are Ok to have (many) collisions,
        // we will use a subset of collisions to do dedup.
        if num_indices == dest_indices.len() {
            info!(
                "Found {:?} or more collisions when searching for truncated hash {:?}",
                dest_indices.len(),
                truncate_hash(chunk_hash)
            );
        }

        Ok(num_indices)
    }

    /// Reads the file info from a specific index.
    pub fn read_file_info<R: Read + Seek>(
        &self,
        reader: &mut R,
        file_entry_index: u32,
        file_hash: &MerkleHash,
    ) -> Result<Option<MDBFileInfo>> {
        reader.seek(SeekFrom::Start(
            self.metadata.file_info_offset + MDB_FILE_INFO_ENTRY_SIZE * (file_entry_index as u64),
        ))?;

        let file_header = FileDataSequenceHeader::deserialize(reader)?;
        if file_header.file_hash != *file_hash {
            return Ok(None);
        }

        let mut mdb_file = MDBFileInfo {
            metadata: file_header.clone(),
            ..Default::default()
        };

        for _ in 0..file_header.num_entries {
            mdb_file
                .segments
                .push(FileDataSequenceEntry::deserialize(reader)?);
        }

        Ok(Some(mdb_file))
    }

    pub fn read_cas_info_from<R: Read + Seek>(
        &self,
        reader: &mut R,
        cas_entry_index: u32,
        chunk_offset: u32,
        chunk_hash: &MerkleHash,
    ) -> Result<Option<MDBCASInfo>> {
        reader.seek(SeekFrom::Start(
            self.metadata.cas_info_offset + MDB_CAS_INFO_ENTRY_SIZE * (cas_entry_index as u64),
        ))?;

        let cas_header = CASChunkSequenceHeader::deserialize(reader)?;

        // Jump forward to the chunk at chunk_offset.
        reader.seek(SeekFrom::Current(
            MDB_CAS_INFO_ENTRY_SIZE as i64 * chunk_offset as i64,
        ))?;

        // Check if the first chunk hash match.
        let first_chunk = CASChunkSequenceEntry::deserialize(reader)?;
        if first_chunk.chunk_hash != *chunk_hash {
            return Ok(None);
        }

        let mut mdb_cas = MDBCASInfo {
            metadata: cas_header.clone(),
            chunks: vec![first_chunk],
        };

        // Read everything else until the CAS block end.
        let chunks_to_read = (cas_header.num_entries - chunk_offset) as usize;

        for _ in 1..chunks_to_read {
            mdb_cas
                .chunks
                .push(CASChunkSequenceEntry::deserialize(reader)?);
        }

        Ok(Some(mdb_cas))
    }

    // Given a file pointer, returns the information needed to reconstruct the file.
    // The information is stored in the destination vector dest_results.  The function
    // returns true if the file hash was found, and false otherwise.
    pub fn get_file_reconstruction_info<R: Read + Seek>(
        &self,
        reader: &mut R,
        file_hash: &MerkleHash,
    ) -> Result<Option<MDBFileInfo>> {
        // Search in file info lookup table.
        let mut dest_indices = [0u32; 8];
        let num_indices = self.get_file_info_index_by_hash(reader, file_hash, &mut dest_indices)?;

        // Check each file info if the file hash matches.
        for &file_entry_index in dest_indices.iter().take(num_indices) {
            if let Some(mdb_file) = self.read_file_info(reader, file_entry_index, file_hash)? {
                return Ok(Some(mdb_file));
            }
        }

        Ok(None)
    }

    // Performs a query of block hashes against a known block hash, matching
    // as many of the values in query_hashes as possible.  It returns the number
    // of entries matched from the input hashes, the CAS block hash of the match,
    // and the range matched from that block.
    pub fn chunk_hash_dedup_query<R: Read + Seek>(
        &self,
        reader: &mut R,
        query_hashes: &[MerkleHash],
    ) -> Result<Option<(usize, FileDataSequenceEntry)>> {
        if query_hashes.is_empty() {
            return Ok(None);
        }

        // Lookup CAS block from chunk lookup.
        let mut dest_indices = [(0u32, 0u32); 8];
        let num_indices =
            self.get_cas_info_index_by_chunk(reader, &query_hashes[0], &mut dest_indices)?;

        // Sequentially match chunks in that block.
        for &(cas_index, chunk_offset) in dest_indices.iter().take(num_indices) {
            if let Some(cas) =
                self.read_cas_info_from(reader, cas_index, chunk_offset, &query_hashes[0])?
            {
                // First chunk hash matches, try match subsequent chunks.
                let mut match_index = 1;

                while match_index < query_hashes.len() && match_index < cas.chunks.len() {
                    if query_hashes[match_index] != cas.chunks[match_index].chunk_hash {
                        break;
                    }

                    match_index += 1;
                }

                // The start of the next unmatched chunk, or end of the entire cas block.
                let matched_bytes_end = if match_index < cas.chunks.len() {
                    cas.chunks[match_index].chunk_byte_range_start
                } else {
                    cas.metadata.num_bytes_in_cas
                };

                return Ok(Some((
                    match_index,
                    FileDataSequenceEntry::from_cas_entries(
                        &cas.metadata,
                        &cas.chunks[0..match_index],
                        matched_bytes_end,
                    ),
                )));
            }
        }

        Ok(None)
    }

    pub fn num_cas_entries(&self) -> usize {
        self.metadata.cas_lookup_num_entry as usize
    }

    pub fn num_file_entries(&self) -> usize {
        self.metadata.file_lookup_num_entry as usize
    }

    /// Returns the number of bytes in the shard
    pub fn num_bytes(&self) -> u64 {
        self.metadata.footer_offset + size_of::<MDBShardFileFooter>() as u64
    }

    pub fn materialized_bytes(&self) -> u64 {
        self.metadata.materialized_bytes
    }

    pub fn stored_bytes(&self) -> u64 {
        self.metadata.stored_bytes
    }

    /// returns the number of bytes that is fixed and not part of any content; i.e. would be part of an empty shard.
    pub fn non_content_byte_size() -> u64 {
        (size_of::<MDBShardFileFooter>() + size_of::<MDBShardFileHeader>()) as u64 // Header and footer
        + size_of::<FileDataSequenceHeader>() as u64 // Guard block for scanning.
        + size_of::<CASChunkSequenceHeader>() as u64 // Guard block for scanning.
    }

    pub fn print_report(&self) {
        eprintln!(
            "Byte size of file info: {}",
            self.metadata.file_lookup_offset - self.metadata.file_info_offset
        );
        eprintln!(
            "Byte size of file lookup: {}",
            self.metadata.cas_info_offset - self.metadata.file_lookup_offset
        );
        eprintln!(
            "Byte size of cas info: {}",
            self.metadata.cas_lookup_offset - self.metadata.cas_info_offset
        );
        eprintln!(
            "Byte size of cas lookup: {}",
            self.metadata.chunk_lookup_offset - self.metadata.cas_lookup_offset
        );
        eprintln!(
            "Byte size of chunk lookup: {}",
            self.metadata.footer_offset - self.metadata.chunk_lookup_offset
        );
    }

    pub fn read_file_info_ranges<R: Read + Seek>(
        reader: &mut R,
    ) -> Result<Vec<(MerkleHash, (u64, u64))>> {
        let mut ret = Vec::new();

        let _shard_header = MDBShardFileHeader::deserialize(reader)?;

        loop {
            let header = FileDataSequenceHeader::deserialize(reader)?;

            if header.file_hash == MerkleHash::default() {
                break;
            }

            let byte_start = reader.stream_position()?;

            reader.seek(SeekFrom::Current(
                (header.num_entries * (size_of::<FileDataSequenceEntry>() as u32)) as i64,
            ))?;
            let byte_end = reader.stream_position()?;

            ret.push((header.file_hash, (byte_start, byte_end)));
        }

        Ok(ret)
    }
}

pub mod test_routines {
    use std::io::{Cursor, Read, Seek};

    use crate::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo};
    use crate::error::Result;
    use crate::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
    use crate::shard_file::MDBShardInfo;
    use crate::shard_in_memory::MDBInMemoryShard;
    use merklehash::MerkleHash;
    use rand::rngs::{SmallRng, StdRng};
    use rand::{Rng, SeedableRng};

    pub fn simple_hash(n: u64) -> MerkleHash {
        MerkleHash::from([n, 1, 0, 0])
    }
    pub fn rng_hash(seed: u64) -> MerkleHash {
        let mut rng = SmallRng::seed_from_u64(seed);
        MerkleHash::from([rng.gen(), rng.gen(), rng.gen(), rng.gen()])
    }

    pub fn convert_to_file(shard: &MDBInMemoryShard) -> Result<Vec<u8>> {
        let mut buffer = Vec::<u8>::new();

        MDBShardInfo::serialize_from(&mut buffer, shard)?;

        Ok(buffer)
    }

    #[allow(clippy::type_complexity)]
    pub fn gen_specific_shard(
        cas_nodes: &[(u64, &[(u64, u32)])],
        file_nodes: &[(u64, &[(u64, (u32, u32))])],
    ) -> Result<MDBInMemoryShard> {
        let mut shard = MDBInMemoryShard::default();

        for (hash, chunks) in cas_nodes {
            let mut cas_block = Vec::<_>::new();
            let mut pos = 0u32;

            for (h, s) in chunks.iter() {
                cas_block.push(CASChunkSequenceEntry::new(simple_hash(*h), pos, *s));
                pos += s
            }

            shard.add_cas_block(MDBCASInfo {
                metadata: CASChunkSequenceHeader::new(simple_hash(*hash), chunks.len(), pos),
                chunks: cas_block,
            })?;
        }

        for (file_hash, segments) in file_nodes {
            let file_contents: Vec<_> = segments
                .iter()
                .map(|(h, (lb, ub))| {
                    FileDataSequenceEntry::new(simple_hash(*h), *ub - *lb, *lb, *ub)
                })
                .collect();

            shard.add_file_reconstruction_info(MDBFileInfo {
                metadata: FileDataSequenceHeader::new(simple_hash(*file_hash), segments.len()),
                segments: file_contents,
            })?;
        }

        Ok(shard)
    }

    pub fn gen_random_shard(
        seed: u64,
        cas_block_sizes: &[usize],
        file_chunk_range_sizes: &[usize],
    ) -> Result<MDBInMemoryShard> {
        // generate the cas content stuff.
        let mut shard = MDBInMemoryShard::default();
        let mut rng = StdRng::seed_from_u64(seed);

        for cas_block_size in cas_block_sizes {
            let mut cas_block = Vec::<_>::new();
            let mut pos = 0u32;

            for _ in 0..*cas_block_size {
                cas_block.push(CASChunkSequenceEntry::new(
                    rng_hash(rng.gen()),
                    rng.gen_range(10000..20000),
                    pos,
                ));
                pos += rng.gen_range(10000..20000);
            }

            shard.add_cas_block(MDBCASInfo {
                metadata: CASChunkSequenceHeader::new(rng_hash(rng.gen()), *cas_block_size, pos),
                chunks: cas_block,
            })?;
        }

        for file_block_size in file_chunk_range_sizes {
            let file_hash = rng_hash(rng.gen());

            let file_contents: Vec<_> = (0..*file_block_size)
                .map(|_| {
                    let lb = rng.gen_range(0..10000);
                    let ub = lb + rng.gen_range(0..10000);
                    FileDataSequenceEntry::new(rng_hash(rng.gen()), ub - lb, lb, ub)
                })
                .collect();

            shard.add_file_reconstruction_info(MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, *file_block_size),
                segments: file_contents,
            })?;
        }

        Ok(shard)
    }

    pub fn verify_mdb_shard(shard: &MDBInMemoryShard) -> Result<()> {
        let buffer = convert_to_file(shard)?;

        verify_mdb_shards_match(shard, Cursor::new(&buffer))
    }

    pub fn verify_mdb_shards_match<R: Read + Seek>(
        mem_shard: &MDBInMemoryShard,
        shard_info: R,
    ) -> Result<()> {
        let mut cursor = shard_info;
        // Now, test that the results on queries from the
        let shard_file = MDBShardInfo::load_from_file(&mut cursor)?;

        assert_eq!(mem_shard.shard_file_size(), shard_file.num_bytes());
        assert_eq!(
            mem_shard.materialized_bytes(),
            shard_file.materialized_bytes()
        );
        assert_eq!(mem_shard.stored_bytes(), shard_file.stored_bytes());

        for (k, cas_block) in mem_shard.cas_content.iter() {
            // Go through and test queries on both the in-memory shard and the
            // serialized shard, making sure that they match completely.

            for i in 0..cas_block.chunks.len() {
                // Test the dedup query over a few hashes in which all the
                // hashes queried are part of the cas_block.
                let query_hashes_1: Vec<MerkleHash> = cas_block.chunks
                    [i..(i + 3).min(cas_block.chunks.len())]
                    .iter()
                    .map(|c| c.chunk_hash)
                    .collect();
                let n_items_to_read = query_hashes_1.len();

                // Also test the dedup query over a few hashes in which some of the
                // hashes are part of the query, and the last is not.
                let mut query_hashes_2 = query_hashes_1.clone();
                query_hashes_2.push(rng_hash(1000000 + i as u64));

                let lb = cas_block.chunks[i].chunk_byte_range_start;
                let ub = if i + 3 >= cas_block.chunks.len() {
                    cas_block.metadata.num_bytes_in_cas
                } else {
                    cas_block.chunks[i + 3].chunk_byte_range_start
                };

                for query_hashes in [&query_hashes_1, &query_hashes_2] {
                    let result_m = mem_shard.chunk_hash_dedup_query(query_hashes).unwrap();

                    let result_f = shard_file
                        .chunk_hash_dedup_query(&mut cursor, query_hashes)?
                        .unwrap();

                    // Returns a tuple of (num chunks matched, FileDataSequenceEntry)
                    assert_eq!(result_m.0, n_items_to_read);
                    assert_eq!(result_f.0, n_items_to_read);

                    // Make sure it gives the correct CAS block hash as the second part of the
                    assert_eq!(result_m.1.cas_hash, *k);
                    assert_eq!(result_f.1.cas_hash, *k);

                    // Make sure the bounds are correct
                    assert_eq!(
                        (
                            result_m.1.chunk_byte_range_start,
                            result_m.1.chunk_byte_range_end
                        ),
                        (lb, ub)
                    );
                    assert_eq!(
                        (
                            result_f.1.chunk_byte_range_start,
                            result_f.1.chunk_byte_range_end
                        ),
                        (lb, ub)
                    );

                    // Make sure everything else equal.
                    assert_eq!(result_m, result_f);
                }
            }
        }

        // Test get file reconstruction info.
        // Against some valid hashes,
        let mut query_hashes: Vec<MerkleHash> =
            mem_shard.file_content.iter().map(|file| *file.0).collect();
        // and a few (very likely) invalid somes.
        for i in 0..3 {
            query_hashes.push(rng_hash(1000000 + i as u64));
        }

        for k in query_hashes.iter() {
            let result_m = mem_shard.get_file_reconstruction_info(k);
            let result_f = shard_file.get_file_reconstruction_info(&mut cursor, k)?;

            // Make sure two queries return same results.
            assert_eq!(result_m, result_f);

            // Make sure retriving the expected file.
            if result_m.is_some() {
                assert_eq!(result_m.unwrap().metadata.file_hash, *k);
                assert_eq!(result_f.unwrap().metadata.file_hash, *k);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Result;

    use super::test_routines::*;

    #[test]
    fn test_simple() -> Result<()> {
        let shard = gen_random_shard(0, &[1, 1], &[1])?;

        verify_mdb_shard(&shard)?;

        Ok(())
    }

    #[test]
    fn test_specific() -> Result<()> {
        let mem_shard_1 = gen_specific_shard(&[(0, &[(11, 5)])], &[(100, &[(200, (0, 5))])])?;
        verify_mdb_shard(&mem_shard_1)
    }

    #[test]
    fn test_multiple() -> Result<()> {
        let shard = gen_random_shard(0, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6])?;

        verify_mdb_shard(&shard)?;

        Ok(())
    }

    #[test]
    fn test_corner_cases_empty() -> Result<()> {
        let shard = gen_random_shard(0, &[0], &[0])?;

        verify_mdb_shard(&shard)?;

        Ok(())
    }

    #[test]
    fn test_corner_cases_empty_entries() -> Result<()> {
        let shard = gen_random_shard(0, &[5, 6, 0, 10, 0], &[3, 4, 5, 0, 4, 0])?;

        verify_mdb_shard(&shard)?;

        Ok(())
    }
}
