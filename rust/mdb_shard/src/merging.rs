use crate::set_operations::shard_set_union;
use crate::shard_handle::MDBShardFile;
use crate::utils::shard_file_name;
use crate::utils::temp_shard_file_name;
use merkledb::error::Result;
use merklehash::compute_data_hash;
use merklehash::MerkleHash;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::mem::swap;
use std::path::{Path, PathBuf};
use tracing::info;

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
pub fn merge_shards(
    result_directory: &Path,
    shards: Vec<MDBShardFile>,
    target_min_size: u64,
) -> Result<Vec<MDBShardFile>> {
    // Will be used in a few places later.
    let move_to_result_directory = |mut si: MDBShardFile| -> Result<MDBShardFile> {
        if si.path.parent().unwrap_or(result_directory) != result_directory {
            let new_path = result_directory.join(si.path.file_name().unwrap());
            info!("Moving shard {:?} to {:?}", &si.path, &new_path);
            std::fs::rename(&si.path, &new_path)?;
            si.path = new_path;
        }
        Ok(si)
    };

    // Any shards that are over or near the threshhold shard size should be considered done.
    let mut completed_shards = Vec::<MDBShardFile>::new();

    let mut queue = Vec::with_capacity(shards.len());

    for si in shards {
        if si.shard.num_bytes() >= target_min_size {
            let new_si = move_to_result_directory(si)?;
            completed_shards.push(new_si);
        } else {
            queue.push(si);
        }
    }

    // Queue up the shards by size, with the smallest on the top of the stack.  This then means that we merge the smallest
    // shards as a heuristic to maximize consolidation.
    queue.sort_unstable_by_key(|si: &MDBShardFile| (-(si.shard.num_bytes() as isize)));

    // Set up in-memory buffers.
    let mut cur_data = Vec::<u8>::with_capacity(2 * target_min_size as usize);
    let mut alt_data = Vec::<u8>::with_capacity(2 * target_min_size as usize);
    let mut out_data = Vec::<u8>::with_capacity(2 * target_min_size as usize);

    loop {
        match queue.len() {
            0 => {
                break;
            }
            1 => {
                let si = queue.pop().unwrap();

                // Nothing needs to be done about this shard, it's already correct.
                // We should just move it into the target directory.
                let new_si = move_to_result_directory(si)?;

                completed_shards.push(new_si);

                break;
            }
            _ => {
                let mut shards_to_remove = Vec::new();
                // Load the next shard in to memory.
                let mut cur_si = queue.pop().unwrap();

                cur_data.clear();
                std::fs::File::open(&cur_si.path)?.read_to_end(&mut cur_data)?;

                shards_to_remove.push(cur_si.path.clone());

                // Merge as many from the queue as possible into this one.
                loop {
                    // There should always be something in the queue
                    let alt_si = queue.pop().unwrap();

                    alt_data.clear();
                    std::fs::File::open(&alt_si.path)?.read_to_end(&mut alt_data)?;
                    shards_to_remove.push(alt_si.path);

                    out_data.clear();

                    // Merge these in to the current shard.
                    let new_shard = shard_set_union(
                        &cur_si.shard,
                        &mut Cursor::new(&cur_data),
                        &alt_si.shard,
                        &mut Cursor::new(&alt_data),
                        &mut out_data,
                    )?;

                    if new_shard.num_bytes() > target_min_size || queue.is_empty() {
                        let (shard_hash, full_file_name) =
                            write_shard(result_directory, &out_data)?;

                        let si = MDBShardFile {
                            shard_hash,
                            path: std::fs::canonicalize(&full_file_name)?,
                            shard: new_shard,
                        };
                        info!(
                            "Created merged shard {:?} from shards {:?}",
                            &si.path, &shards_to_remove
                        );

                        completed_shards.push(si);

                        // Delete all the previous ones, now that this has been successfully written out.
                        for path in shards_to_remove {
                            info!("Removing shard {:?}", &path);
                            std::fs::remove_file(&path)?;
                        }

                        break;
                    } else {
                        swap(&mut cur_data, &mut out_data);
                        cur_si.shard = new_shard;
                        cur_si.path = PathBuf::default();
                        cur_si.shard_hash = MerkleHash::default();
                    }
                }
            }
        }
    }

    Ok(completed_shards)
}

pub fn consolidate_shards_in_directory(
    directory: &Path,
    target_min_size: u64,
) -> Result<Vec<MDBShardFile>> {
    let shards = MDBShardFile::load_all(directory)?;

    let consolidated_shards = merge_shards(directory, shards, target_min_size)?;
    Ok(consolidated_shards)
}
