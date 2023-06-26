use async_trait::async_trait;
use mdb_shard::error::MDBShardError;
use merklehash::MerkleHash;
pub mod reconstruction;
pub mod shard_client;
// we reexport FileDataSequenceEntry
pub use crate::shard_client::GrpcShardClient;
pub use mdb_shard::file_structs::FileDataSequenceEntry;

/// Container for information required to set up and handle
/// Shard connections
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardConnectionConfig {
    pub endpoint: String,
    pub user_id: String,
    pub git_xet_version: String,
}

impl ShardConnectionConfig {
    /// creates a new ShardConnectionConfig with given endpoint and user_id
    pub fn new(endpoint: String, user_id: String, git_xet_version: String) -> Self {
        ShardConnectionConfig {
            endpoint,
            user_id,
            git_xet_version,
        }
    }
}

/// A Client to the Shard service. The shard service
/// provides for
/// 1. the ingestion of Shard information from CAS to the Shard service
/// 2. querying of file->reconstruction information
#[async_trait]
pub trait RegistrationClient {
    /// Requests the service to add a shard file currently stored in CAS under the prefix/hash
    async fn register_shard(&self, prefix: &str, hash: &MerkleHash) -> Result<(), MDBShardError>;
}
