use std::sync::Arc;

use tracing::error;

use cas_client::RemoteClient;
use merkledb::ObjectRange;

use crate::data::cas_interface::{data_from_chunks_to_writer, slice_object_range};
use crate::errors::{GitXetRepoError, Result};

use super::cas_interface::CasClient;

/// Manages the smudigng of a single set of blocks, that does not
/// require a full copy of the MerkleDB to hang around.
#[derive(Clone)]
pub struct MiniPointerFileSmudger {
    pub cas: Arc<CasClient>,
    pub prefix: String,
    pub blocks: Vec<ObjectRange>,
}

impl MiniPointerFileSmudger {
    fn derive_ranged_blocks(&self, range: Option<(usize, usize)>) -> Result<Vec<ObjectRange>> {
        match range {
            Some((start, end)) => {
                // we expect callers to validate the range, but just in case, check it anyway.
                if end < start {
                    let msg = format!(
                        "End range value requested ({end}) is less than start range value ({start})"
                    );
                    error!(msg);
                    return Err(GitXetRepoError::Other(msg));
                }
                Ok(slice_object_range(&self.blocks, start, end - start))
            }
            None => Ok(self.blocks.clone()),
        }
    }

    pub async fn smudge_to_writer(
        &self,
        writer: &mut impl std::io::Write,
        range: Option<(usize, usize)>,
    ) -> Result<()> {
        let ranged_blocks = self.derive_ranged_blocks(range)?;
        data_from_chunks_to_writer(&self.cas, self.prefix.clone(), ranged_blocks, writer).await?;
        Ok(())
    }
}
