use async_trait::async_trait;
use merklehash::MerkleHash;

use crate::{error::MDBShardError, file_structs::MDBFileInfo};

#[async_trait]
pub trait FileReconstructor {
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>, MDBShardError>;
}
