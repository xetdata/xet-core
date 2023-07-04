use crate::serialization_utils::*;
use merklehash::MerkleHash;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{Cursor, Read, Write};
use std::mem::{size_of, take};

/// Each file consists of a FileDataSequenceHeader following
/// a sequence of FileDataSequenceEntry.

const INTERSHARD_REFERENCE_VERSION: u32 = 0;
const INTERSHARD_REFERENCE_DEFAULT_FLAGS: u32 = 0;
const INTERSHARD_REFERENCE_SIZE_CAP: usize = 512;

// For this one, since the
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IntershardReferenceSequenceHeader {
    pub version: u32,
    pub num_entries: u32,
    pub _unused: u64,
}

impl IntershardReferenceSequenceHeader {
    pub fn new<I: TryInto<u32>>(num_entries: I) -> Self
    where
        <I as TryInto<u32>>::Error: std::fmt::Debug,
    {
        Self {
            version: INTERSHARD_REFERENCE_VERSION,
            num_entries: num_entries.try_into().unwrap(),
            _unused: 0,
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer_cur = std::io::Cursor::new(&mut buf[..]);
            let writer = &mut writer_cur;

            write_u32(writer, self.version)?;
            write_u32(writer, self.num_entries)?;
            write_u64(writer, self._unused)?;
        }

        writer.write_all(&buf[..])?;

        Ok(size_of::<IntershardReferenceSequenceHeader>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = std::io::Cursor::new(&v);
        let reader = &mut reader_curs;

        Ok(Self {
            version: read_u32(reader)?,
            num_entries: read_u32(reader)?,
            _unused: read_u64(reader)?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct IntershardReferenceSequenceEntry {
    pub shard_hash: MerkleHash,
    pub flags: u32,
    pub total_dedup_hit_count: u32,
    pub _buffer: [u64; 4],
}

impl IntershardReferenceSequenceEntry {
    pub fn new<I1: TryInto<u32>>(shard_hash: MerkleHash, total_dedup_hit_count: I1) -> Self
    where
        <I1 as TryInto<u32>>::Error: std::fmt::Debug,
    {
        Self {
            shard_hash,
            flags: INTERSHARD_REFERENCE_DEFAULT_FLAGS,
            total_dedup_hit_count: total_dedup_hit_count.try_into().unwrap(),
            _buffer: Default::default(),
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer_cur = std::io::Cursor::new(&mut buf[..]);
            let writer = &mut writer_cur;

            write_hash(writer, &self.shard_hash)?;
            write_u32(writer, self.flags)?;
            write_u32(writer, self.total_dedup_hit_count)?;
        }

        writer.write_all(&buf[..])?;

        Ok(size_of::<IntershardReferenceSequenceEntry>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<IntershardReferenceSequenceEntry>()];
        reader.read_exact(&mut v[..])?;

        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;

        Ok(Self {
            shard_hash: read_hash(reader)?,
            flags: read_u32(reader)?,
            total_dedup_hit_count: read_u32(reader)?,
            _buffer: Default::default(),
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct IntershardReferenceSequence {
    pub metadata: IntershardReferenceSequenceHeader,
    pub entries: Vec<IntershardReferenceSequenceEntry>,
}

impl IntershardReferenceSequence {
    pub fn num_bytes(&self) -> u64 {
        (size_of::<IntershardReferenceSequenceHeader>()
            + self.entries.len() * size_of::<IntershardReferenceSequenceEntry>()) as u64
    }

    pub fn merge_in(&mut self, other: &IntershardReferenceSequence) {
        let entries = take(&mut self.entries);
        let mut local_hm: HashMap<MerkleHash, IntershardReferenceSequenceEntry> = entries
            .into_iter()
            .map(|irse| (irse.shard_hash, irse))
            .collect();

        for irse in other.entries.iter() {
            let entry = local_hm
                .entry(irse.shard_hash)
                .or_insert_with(|| IntershardReferenceSequenceEntry::new(irse.shard_hash, 0));
            entry.total_dedup_hit_count = entry
                .total_dedup_hit_count
                .saturating_add(irse.total_dedup_hit_count);
        }

        // Collect the entries at the end.
        self.entries = local_hm.into_values().collect();

        if self.entries.len() > INTERSHARD_REFERENCE_SIZE_CAP {
            self.entries
                .sort_unstable_by_key(|e| u64::MAX - (e.total_dedup_hit_count as u64));

            self.entries
                .resize(INTERSHARD_REFERENCE_SIZE_CAP, Default::default());
        }
    }
}
