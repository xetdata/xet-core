use crate::error::Result;
use crate::intershard_reference_structs::write_out_with_new_intershard_reference_section;
use crate::set_operations::shard_set_union;
use crate::shard_handle::MDBShardFile;
use merklehash::MerkleHash;
use std::collections::HashMap;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Read;
use std::mem::swap;
use std::path::Path;

// Merge a collection of shards.
// After calling this, the passed in shards may be invalid -- i.e. may refer to a shard that doesn't exist.
// All shards are either merged into shards in the result directory or moved to that directory (if not there already).
//
// Ordering of staged shards is preserved.

#[allow(clippy::needless_range_loop)] // The alternative is less readable IMO
pub fn consolidate_shards_in_session_directory(
    session_directory: &Path,
    target_max_size: u64,
) -> Result<Vec<MDBShardFile>> {
    let mut shards = MDBShardFile::load_all(session_directory)?;

    for s in shards.iter() {
        debug_assert!(
            s.staging_index.is_some(),
            "Shard in staging directory without staging index."
        );
    }

    shards.sort_unstable_by_key(|s| s.staging_index.unwrap_or(u64::MAX));

    // Make not mutable
    let shards = shards;

    let mut finished_shards = Vec::<MDBShardFile>::with_capacity(shards.len());

    // Unfortunately, the hashes of some shards may change and all later references in the shard hints to
    // these shards must then also change.
    let mut hash_replacement_map = HashMap::<MerkleHash, Option<MerkleHash>>::new();

    let mut cur_data = Vec::<u8>::with_capacity(target_max_size as usize);
    let mut alt_data = Vec::<u8>::with_capacity(target_max_size as usize);
    let mut out_data = Vec::<u8>::with_capacity(target_max_size as usize);

    let mut cur_idx = 0;

    while cur_idx < shards.len() {
        let cur_sfi = &shards[cur_idx];

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

        let mut files_to_delete = Vec::<&Path>::with_capacity(16);

        // We can't consolidate any here.
        if ub_idx == cur_idx + 1 {
            let new_si = {
                if hash_replacement_map.is_empty() {
                    cur_sfi.clone()
                } else {
                    let mut irs = cur_sfi.get_intershard_references()?;

                    if irs.remap_references(&hash_replacement_map) {
                        let new_si = write_out_with_new_intershard_reference_section(
                            &cur_sfi.shard,
                            cur_sfi.staging_index,
                            &mut BufReader::new(std::fs::File::open(&cur_sfi.path)?),
                            session_directory,
                            irs,
                        )?;
                        hash_replacement_map.insert(cur_sfi.shard_hash, Some(new_si.shard_hash));

                        files_to_delete.push(&cur_sfi.path);

                        new_si
                    } else {
                        cur_sfi.clone()
                    }
                }
            };

            finished_shards.push(new_si);
            cur_idx += 1;
        } else {
            // Get the current data in a buffer
            let mut cur_shard_info = cur_sfi.shard.clone();

            cur_data.clear();
            std::fs::File::open(&cur_sfi.path)?.read_to_end(&mut cur_data)?;
            let mut new_irs = cur_sfi
                .shard
                .get_intershard_references(&mut Cursor::new(&mut cur_data))?;

            files_to_delete.push(&cur_sfi.path);

            // Now, merge in everything in memory
            for i in (cur_idx + 1)..ub_idx {
                let si = &shards[i];

                alt_data.clear();
                std::fs::File::open(&si.path)?.read_to_end(&mut alt_data)?;

                files_to_delete.push(&si.path);

                // Pull in the new intershard references and merge them.
                let irs = si
                    .shard
                    .get_intershard_references(&mut Cursor::new(&mut alt_data))?;
                new_irs = new_irs.merge(irs);

                // Now merge the main shard
                out_data.clear();

                // Merge these in to the current shard.
                cur_shard_info = shard_set_union(
                    &cur_shard_info,
                    &mut Cursor::new(&cur_data),
                    &si.shard,
                    &mut Cursor::new(&alt_data),
                    &mut out_data,
                )?;

                swap(&mut cur_data, &mut out_data);
            }

            for i in cur_idx..ub_idx {
                // Set the remapping to clear any references to this shard.
                // These will be cleared out with the new merge.
                hash_replacement_map.insert(shards[i].shard_hash, None);
            }

            new_irs.remap_references(&hash_replacement_map);

            // Now, write out the new file.
            let new_sfi = write_out_with_new_intershard_reference_section(
                &cur_shard_info,
                cur_sfi.staging_index,
                &mut Cursor::new(&cur_data),
                session_directory,
                new_irs,
            )?;

            for i in cur_idx..ub_idx {
                // Set the remapping to clear any references to this shard.
                // These will be cleared out with the new merge.
                hash_replacement_map.insert(shards[i].shard_hash, Some(new_sfi.shard_hash));
            }

            // Delete the old ones.
            for p in files_to_delete.iter() {
                if *p != new_sfi.path {
                    std::fs::remove_file(*p)?;
                }
            }

            // Put the new shard on the return stack and move to the next unmerged one
            finished_shards.push(new_sfi);
            cur_idx = ub_idx;
        }
    }

    Ok(finished_shards)
}
