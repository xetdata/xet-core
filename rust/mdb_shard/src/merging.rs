use crate::error::Result;
use crate::set_operations::shard_set_union;
use crate::shard_handle::MDBShardFile;
use crate::utils::shard_file_name;
use crate::utils::temp_shard_file_name;
use merklehash::compute_data_hash;
use merklehash::MerkleHash;
use std::collections::HashSet;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::mem::swap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tracing::debug;

fn write_shard(target_directory: &Path, data: &[u8]) -> Result<(MerkleHash, PathBuf)> {
    let shard_hash = compute_data_hash(data);
    let temp_file_name = target_directory.join(temp_shard_file_name());
    let mut out_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&temp_file_name)?;

    out_file.write_all(data)?;

    let full_file_name = target_directory.join(shard_file_name(&shard_hash));

    std::fs::rename(&temp_file_name, &full_file_name)?;

    Ok((shard_hash, std::fs::canonicalize(full_file_name)?))
}

// Merge a collection of shards.
// After calling this, the passed in shards may be invalid -- i.e. may refer to a shard that doesn't exist.
// All shards are either merged into shards in the result directory or moved to that directory (if not there already).
//
// Ordering of staged shards is preserved.

#[allow(clippy::needless_range_loop)] // The alternative is less readable IMO
pub fn consolidate_shards_in_directory(
    session_directory: &Path,
    target_max_size: u64,
) -> Result<Vec<MDBShardFile>> {
    let mut shards: Vec<(SystemTime, _)> = MDBShardFile::load_all(session_directory)?
        .into_iter()
        .map(|sf| Ok((std::fs::metadata(&sf.path)?.modified()?, sf)))
        .collect::<Result<Vec<_>>>()?;

    shards.sort_unstable_by_key(|(t, _)| *t);

    // Make not mutable
    let shards: Vec<_> = shards.into_iter().map(|(_, s)| s).collect();

    let mut finished_shards = Vec::<MDBShardFile>::with_capacity(shards.len());
    let mut finished_shard_hashes = HashSet::<MerkleHash>::with_capacity(shards.len());

    let mut cur_data = Vec::<u8>::with_capacity(target_max_size as usize);
    let mut alt_data = Vec::<u8>::with_capacity(target_max_size as usize);
    let mut out_data = Vec::<u8>::with_capacity(target_max_size as usize);

    let mut cur_idx = 0;

    while cur_idx < shards.len() {
        let cur_sfi: &MDBShardFile = &shards[cur_idx];

        // Now, see how many we can consolidate.
        let mut ub_idx = cur_idx + 1;
        let mut current_size = cur_sfi.shard.num_bytes();

        for idx in (cur_idx + 1).. {
            if idx == shards.len()
                || shards[idx].shard.num_bytes() + current_size >= target_max_size
            {
                ub_idx = idx;
                break;
            }
            current_size += shards[idx].shard.num_bytes()
        }

        // We can't consolidate any here.
        if ub_idx == cur_idx + 1 {
            finished_shard_hashes.insert(cur_sfi.shard_hash);
            finished_shards.push(cur_sfi.clone());
        } else {
            // Get the current data in a buffer
            let mut cur_shard_info = cur_sfi.shard.clone();

            cur_data.clear();
            std::fs::File::open(&cur_sfi.path)?.read_to_end(&mut cur_data)?;

            // Now, merge in everything in memory
            for i in (cur_idx + 1)..ub_idx {
                let sfi = &shards[i];

                alt_data.clear();
                std::fs::File::open(&sfi.path)?.read_to_end(&mut alt_data)?;

                // Now merge the main shard
                out_data.clear();

                // Merge these in to the current shard.
                cur_shard_info = shard_set_union(
                    &cur_shard_info,
                    &mut Cursor::new(&cur_data),
                    &sfi.shard,
                    &mut Cursor::new(&alt_data),
                    &mut out_data,
                )?;

                swap(&mut cur_data, &mut out_data);
            }
            let (shard_hash, full_file_name) = write_shard(session_directory, &cur_data)?;

            let new_sfi = MDBShardFile {
                shard_hash,
                path: std::fs::canonicalize(&full_file_name)?,
                shard: cur_shard_info,
            };

            debug!(
                "Created merged shard {:?} from shards {:?}",
                &new_sfi.path,
                shards[cur_idx..ub_idx].iter().map(|sfi| &sfi.path)
            );

            // Put the new shard on the return stack and move to the next unmerged one
            finished_shard_hashes.insert(new_sfi.shard_hash);
            finished_shards.push(new_sfi);

            // Delete the old ones.
            for sfi in shards[cur_idx..ub_idx].iter() {
                if finished_shard_hashes.contains(&sfi.shard_hash) {
                    // In rare cases, there could be empty shards or shards with
                    // duplicate entries and we don't want to delete any shards
                    // we've already finished
                    continue;
                }

                debug!(
                    "consolidate_shards: Removing {:?}; info merged to {:?}",
                    &sfi.path,
                    &finished_shards.last().unwrap().shard_hash
                );

                std::fs::remove_file(&sfi.path)?;
            }
        }

        cur_idx = ub_idx;
    }

    Ok(finished_shards)
}
