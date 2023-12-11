use std::{collections::HashSet, hash::Hasher};

// Only the first 2**NUM_INDEX_BITS - 2 shards are indexed.  This allows us to limit the size of the
// table.  Each chunk in a shard takes up 64 bytes, and each shard is a maximum of 64 MB, so each
// shard can have a maximum of around 1M chunks.  However, in practice, the shards are usually much
// smaller, so allow more indices to index more shards.

const NUM_INDEX_BITS: usize = 16;
const INDEX_BITS_MASK: u64 = (1u64 << NUM_INDEX_BITS) - 1;
// The index value for index overflows and multiple values.
const INDEX_BAD_INFORMATION: u64 = INDEX_BITS_MASK;
const KEY_BITS_MASK: u64 = !INDEX_BITS_MASK;

#[derive(Eq)]
pub struct ChunkHashKey(u64);

impl PartialEq for ChunkHashKey {
    fn eq(&self, other: &Self) -> bool {
        (self.0 & KEY_BITS_MASK) == (other.0 & KEY_BITS_MASK)
    }
}

impl std::hash::Hash for ChunkHashKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.0 & KEY_BITS_MASK).hash(state);
    }
}

#[derive(Default)]
pub struct ChunkHashTable {
    lookup: HashSet<ChunkHashKey>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ChunkHashTableQueryResult {
    NotFound,
    Present(usize),
    PresentNoLocation,
}

impl ChunkHashTable {
    pub const MAX_INDEX: usize = INDEX_BAD_INFORMATION as usize;

    fn make_kv_pair(hash: u64, index: usize) -> ChunkHashKey {
        let index_part = (index as u64).min(INDEX_BAD_INFORMATION);
        let hash_part = hash & KEY_BITS_MASK;

        ChunkHashKey(hash_part | index_part)
    }

    fn extract_index(kv_pair: &ChunkHashKey) -> ChunkHashTableQueryResult {
        let ret = kv_pair.0 & INDEX_BITS_MASK;
        if ret == INDEX_BAD_INFORMATION {
            ChunkHashTableQueryResult::PresentNoLocation
        } else {
            ChunkHashTableQueryResult::Present(ret as usize)
        }
    }

    /// Insert a block of chunks into the hash table
    pub fn insert(&mut self, chunks: &[u64], index: usize) {
        for h in chunks {
            if !self.lookup.insert(Self::make_kv_pair(*h, index)) {
                // In this case
                self.lookup
                    .replace(Self::make_kv_pair(*h, INDEX_BAD_INFORMATION as usize));
            }
        }
    }

    /// Query the hash table for the index
    pub fn query(&self, h: u64) -> ChunkHashTableQueryResult {
        if let Some(kv_pair) = self.lookup.get(&ChunkHashKey(h)) {
            Self::extract_index(kv_pair)
        } else {
            ChunkHashTableQueryResult::NotFound
        }
    }

    pub fn clear(&mut self) {
        self.lookup.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.lookup.is_empty()
    }

    pub fn len(&self) -> usize {
        self.lookup.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to shift hash values
    fn shift_hash(hash: u64) -> u64 {
        hash << 30
    }

    #[test]
    fn test_basic_insert_and_query() {
        let mut hash_table = ChunkHashTable::default();
        let hash = shift_hash(12345u64);
        let index = 10usize;

        hash_table.insert(&[hash], index);

        match hash_table.query(hash) {
            ChunkHashTableQueryResult::Present(found_index) => assert_eq!(found_index, index),
            _ => panic!("Chunk not found or incorrect query result type"),
        }
    }

    #[test]
    fn test_multiple_inserts_and_queries() {
        let mut hash_table = ChunkHashTable::default();
        let hashes = [
            shift_hash(12345u64),
            shift_hash(54321u64),
            shift_hash(67890u64),
        ];
        let indices = [10usize, 20usize, 30usize];

        for (i, &hash) in hashes.iter().enumerate() {
            hash_table.insert(&[hash], indices[i]);
        }

        for (i, &hash) in hashes.iter().enumerate() {
            match hash_table.query(hash) {
                ChunkHashTableQueryResult::Present(found_index) => {
                    assert_eq!(found_index, indices[i])
                }
                _ => panic!("Chunk not found or incorrect query result type"),
            }
        }
    }

    #[test]
    fn test_index_overflow() {
        let mut hash_table = ChunkHashTable::default();
        let hash = shift_hash(12345u64);
        let index = ChunkHashTable::MAX_INDEX + 1;

        hash_table.insert(&[hash], index);

        match hash_table.query(hash) {
            ChunkHashTableQueryResult::PresentNoLocation => (),
            _ => panic!("Overflow not handled correctly"),
        }
    }

    #[test]
    fn test_collision_handling() {
        let mut hash_table = ChunkHashTable::default();
        let hash = shift_hash(12345u64);
        let indices = [10usize, 10usize];

        for &index in &indices {
            hash_table.insert(&[hash], index);
        }

        match hash_table.query(hash) {
            ChunkHashTableQueryResult::PresentNoLocation => (),
            _ => panic!("Collision not handled correctly"),
        }
    }

    #[test]
    fn test_query_non_existent_chunk() {
        let hash_table = ChunkHashTable::default();
        let hash = shift_hash(12345u64);

        assert_eq!(hash_table.query(hash), ChunkHashTableQueryResult::NotFound);
    }

    #[test]
    fn test_clear_functionality() {
        let mut hash_table = ChunkHashTable::default();
        let hash = shift_hash(12345u64);
        let index = 10usize;

        hash_table.insert(&[hash], index);
        hash_table.clear();

        assert_eq!(hash_table.len(), 0);
    }

    #[test]
    fn test_size_check() {
        let mut hash_table = ChunkHashTable::default();
        let hashes = [
            shift_hash(12345u64),
            shift_hash(54321u64),
            shift_hash(67890u64),
        ];

        for &hash in &hashes {
            hash_table.insert(&[hash], 10usize);
        }

        assert_eq!(hash_table.len(), hashes.len());
    }

    #[test]
    fn test_boundary_conditions() {
        let mut hash_table = ChunkHashTable::default();
        let min_hash = shift_hash(0u64);
        let max_hash = shift_hash(u64::MAX >> 30); // Ensure shifting doesn't overflow
        let index = 10usize;

        hash_table.insert(&[min_hash, max_hash], index);

        assert_eq!(
            hash_table.query(min_hash),
            ChunkHashTableQueryResult::Present(index)
        );
        assert_eq!(
            hash_table.query(max_hash),
            ChunkHashTableQueryResult::Present(index)
        );
    }
}
