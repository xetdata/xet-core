use cas::key::Key;
use merklehash::MerkleHash;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryFileParams {
    pub file_id: MerkleHash,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Range {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CASReconstructionTerm {
    pub cas_id: Vec<u8>,
    pub unpacked_length: u64,
    pub range: Range,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryFileResponse {
    pub reconstruction: Vec<CASReconstructionTerm>,
    pub key: Key,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SyncShardParams {
    pub key: Key,
    pub force_sync: bool,
    pub salt: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SyncShardResponseType {
    Exists,
    SyncPerformed,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SyncShardResponse {
    pub response: SyncShardResponseType,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryChunkParams {
    pub prefix: String,
    pub chunk: Vec<MerkleHash>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryChunkResponse {
    pub shard: Vec<Vec<u8>>,
}
