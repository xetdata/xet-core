#![cfg_attr(feature = "strict", deny(warnings))]

mod async_chunk_iterator;
mod chunk_iterator;
pub mod constants;

pub mod aggregate_hashes;
mod internal_methods;
mod merkledb_debug;
mod merkledb_reconstruction;
mod merkledbbase;
mod merklenode;

mod merkledb_highlevel_v1;
mod merkledb_ingestion_v1;
mod merkledbv1;

mod merkledb_highlevel_v2;
mod merkledbv2;

mod merklememdb;

mod tests;

pub mod error;

pub use crate::merkledb_highlevel_v1::InsertionStaging;
pub use async_chunk_iterator::{
    async_chunk_target, async_chunk_target_default, async_low_variance_chunk_target, AsyncIterator,
    CompleteType, GenType, GeneratorState, YieldType,
};
pub use chunk_iterator::{chunk_target, chunk_target_default, low_variance_chunk_target, Chunk};
pub use merkledbv1::MerkleDBV1;
pub use merkledbv2::MerkleDBV2;
pub use merklememdb::MerkleMemDB;
pub use merklenode::{MerkleNode, MerkleNodeAttributes, MerkleNodeId, NodeDataType, ObjectRange};
pub mod prelude {
    pub use crate::merkledb_debug::MerkleDBDebugMethods;
    pub use crate::merkledb_highlevel_v1::MerkleDBHighLevelMethodsV1;
    pub use crate::merkledb_ingestion_v1::MerkleDBIngestionMethodsV1;
    pub use crate::merkledb_reconstruction::MerkleDBReconstruction;
    pub use crate::merkledbbase::MerkleDBBase;
    pub use crate::merkledbv1::MerkleDBV1;
}

pub mod prelude_v2 {
    pub use crate::merkledb_debug::MerkleDBDebugMethods;
    pub use crate::merkledb_highlevel_v2::MerkleDBHighLevelMethodsV2;
    pub use crate::merkledb_reconstruction::MerkleDBReconstruction;
    pub use crate::merkledbbase::MerkleDBBase;
}
pub mod detail {
    pub use crate::merklenode::hash_node_sequence;
}
