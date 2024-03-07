use mdb_shard::shard_version::ShardVersion;

use crate::constants::*;

// Map from MDB version to ref notes canonical name
pub fn get_merkledb_notes_name(version: &ShardVersion) -> &'static str {
    match version {
        ShardVersion::V1 => GIT_NOTES_MERKLEDB_V1_REF_NAME,
        ShardVersion::V2 => GIT_NOTES_MERKLEDB_V2_REF_NAME,
        &ShardVersion::Uninitialized => "",
    }
}
