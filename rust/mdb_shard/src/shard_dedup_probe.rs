use async_trait::async_trait;

use merklehash::MerkleHash;

use crate::error::Result;

#[async_trait(? Send)]
pub trait ShardDedupProber {
    /// Probes which shards provides dedup information for a chunk.
    /// Returns a list of shard hashes with key under 'prefix',
    /// Err(_) if an error occured.
    async fn get_dedup_shards(
        &self,
        prefix: &str,
        chunk_hash: &[MerkleHash],
        salt: &[u8; 32],
    ) -> Result<Vec<MerkleHash>>;
}
