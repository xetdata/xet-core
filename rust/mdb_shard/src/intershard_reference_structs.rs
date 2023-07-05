use crate::serialization_utils::*;
use merklehash::MerkleHash;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{Cursor, Read, Write};
use std::mem::{size_of, take};

/// Each file consists of a FileDataSequenceHeader following
/// a sequence of FileDataSequenceEntry.

const INTERSHARD_REFERENCE_VERSION: u32 = 0;
const INTERSHARD_REFERENCE_SIZE_CAP: usize = 512;

// For this one, since the
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IntershardReferenceSequenceHeader {
    // Version this as this will likely evolve.
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
    pub total_dedup_hit_count: u32,
}

impl IntershardReferenceSequenceEntry {
    pub fn new<I1: TryInto<u32>>(shard_hash: MerkleHash, total_dedup_hit_count: I1) -> Self
    where
        <I1 as TryInto<u32>>::Error: std::fmt::Debug,
    {
        Self {
            shard_hash,
            total_dedup_hit_count: total_dedup_hit_count.try_into().unwrap(),
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer_cur = std::io::Cursor::new(&mut buf[..]);
            let writer = &mut writer_cur;

            write_hash(writer, &self.shard_hash)?;
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
            total_dedup_hit_count: read_u32(reader)?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct IntershardReferenceSequence {
    pub metadata: IntershardReferenceSequenceHeader,
    pub entries: Vec<IntershardReferenceSequenceEntry>,
}

impl IntershardReferenceSequence {
    /// Construct from an iterator over (hash, count) pairs.
    pub fn from_counts<I1: TryInto<u32> + Copy>(vec: &Vec<(MerkleHash, I1)>) -> Self
    where
        <I1 as TryInto<u32>>::Error: std::fmt::Debug,
    {
        let mut entries: Vec<IntershardReferenceSequenceEntry> =
            Vec::from_iter(vec.iter().map(|(h, count)| {
                let total_dedup_hit_count: u32 = (*count).try_into().unwrap_or(u32::MAX);

                IntershardReferenceSequenceEntry {
                    shard_hash: *h,
                    total_dedup_hit_count,
                }
            }));

        entries.sort_unstable_by_key(|e| u64::MAX - e.total_dedup_hit_count as u64);

        if entries.len() > INTERSHARD_REFERENCE_SIZE_CAP {
            entries.resize(INTERSHARD_REFERENCE_SIZE_CAP, Default::default());
        }

        Self {
            metadata: IntershardReferenceSequenceHeader::new(entries.len()),
            entries,
        }
    }

    pub fn num_bytes(&self) -> u64 {
        (size_of::<IntershardReferenceSequenceHeader>()
            + self.entries.len() * size_of::<IntershardReferenceSequenceEntry>()) as u64
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn merge(self, other: IntershardReferenceSequence) -> Self {
        let mut s = self;

        let entries = take(&mut s.entries);
        let mut local_hm: HashMap<MerkleHash, IntershardReferenceSequenceEntry> = entries
            .into_iter()
            .map(|irse| (irse.shard_hash, irse))
            .collect();

        for irse in other.entries.into_iter() {
            let entry = local_hm
                .entry(irse.shard_hash)
                .or_insert_with(|| IntershardReferenceSequenceEntry::new(irse.shard_hash, 0));
            entry.total_dedup_hit_count = entry
                .total_dedup_hit_count
                .saturating_add(irse.total_dedup_hit_count);
        }

        // Collect the entries at the end.
        s.entries = local_hm.into_values().collect();

        // Sort them in reverse order by number of hits
        s.entries
            .sort_unstable_by_key(|e| u64::MAX - (e.total_dedup_hit_count as u64));

        if s.entries.len() > INTERSHARD_REFERENCE_SIZE_CAP {
            s.entries
                .resize(INTERSHARD_REFERENCE_SIZE_CAP, Default::default());
        }

        s
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut n_bytes = 0;

        n_bytes += self.metadata.serialize(writer)?;

        for isre in self.entries.iter() {
            n_bytes += isre.serialize(writer)?;
        }

        Ok(n_bytes)
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let metadata = IntershardReferenceSequenceHeader::deserialize(reader)?;

        let mut entries = Vec::with_capacity(metadata.num_entries as usize);
        for _ in 0..metadata.num_entries {
            entries.push(IntershardReferenceSequenceEntry::deserialize(reader)?);
        }

        Ok(Self { metadata, entries })
    }
}
