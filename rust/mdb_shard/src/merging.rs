use crate::error::Result;
use crate::intershard_reference_structs::write_out_with_new_intershard_reference_section;
use crate::intershard_reference_structs::IntershardReferenceSequence;
use crate::set_operations::shard_set_union;
use crate::shard_file::MDBShardInfo;
use crate::shard_handle::MDBShardFile;
use crate::utils::truncate_hash;
use merkledb::constants::TARGET_CDC_CHUNK_SIZE;
use merklehash::MerkleHash;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::mem::swap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::SystemTime;
use tracing::debug;

fn add_shard_to_cas_to_shard_lookup(
    lookup: &mut HashMap<u64, Rc<MerkleHash>>,
    sfi: &MDBShardFile,
) -> Result<()> {
    let current_shard = Rc::new(sfi.shard_hash);

    let cas_map = sfi.read_full_cas_lookup()?;

    for (h, _) in cas_map {
        lookup.insert(h, current_shard.clone());
    }

    Ok(())
}

fn add_lookups_to_intershard_reference_section<R: Read + Seek>(
    intershard_ref_lookup: &HashMap<u64, Rc<MerkleHash>>,
    si: &MDBShardInfo,
    reader: &mut R,
) -> Result<Option<IntershardReferenceSequence>> {
    let mut new_irs_lookup = HashMap::<MerkleHash, u32>::new();

    if !(intershard_ref_lookup.is_empty() || si.num_file_entries() == 0) {
        for i in 0..(si.num_file_entries() as u32) {
            let fi = si.get_file_info_by_index(reader, i)?;

            for entry in fi.segments {
                let h = truncate_hash(&entry.cas_hash);
                if let Some(shard_hash) = intershard_ref_lookup.get(&h) {
                    let e = new_irs_lookup.entry(*shard_hash.as_ref()).or_default();

                    let num_chunks_rounded: u32 = ((entry.unpacked_segment_bytes
                        + (TARGET_CDC_CHUNK_SIZE as u32 / 2))
                        / TARGET_CDC_CHUNK_SIZE as u32)
                        .max(1);

                    *e = e.saturating_add(num_chunks_rounded);
                }
            }
        }
    }

    // Add in the new lookup if appropriate
    Ok(if !new_irs_lookup.is_empty() {
        let existing_irs = si.get_intershard_references(reader)?;
        let new_irs = IntershardReferenceSequence::from_counts(new_irs_lookup.into_iter());
        let merged_irs = existing_irs.merge(new_irs);

        Some(merged_irs)
    } else {
        None
    })
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

    let mut intershard_lookup = HashMap::<u64, Rc<MerkleHash>>::new();

    {
        while cur_idx < shards.len() {
            let cur_sfi: &MDBShardFile = &shards[cur_idx];

            // Now, see how many we can consolidate.
            let mut ub_idx = cur_idx + 1;
            let mut current_size = cur_sfi.shard.num_bytes();

            // Do we have to remove any shards along the way?
            let mut shards_to_remove = Vec::<(MerkleHash, PathBuf)>::new();

            for idx in (cur_idx + 1).. {
                if idx == shards.len()
                    || shards[idx].shard.num_bytes() + current_size >= target_max_size
                {
                    ub_idx = idx;
                    break;
                }
                current_size += shards[idx].shard.num_bytes()
            }

            if ub_idx == cur_idx + 1 {
                // We can't consolidate any here, so just see if we need to add anything new
                // to the intershard lookups
                let new_sfi = {
                    let mut reader = &mut cur_sfi.get_reader()?;

                    // Have the intershard lookups changed here?  If so, write out the shard and change it.
                    if let Some(new_irs) = add_lookups_to_intershard_reference_section(
                        &intershard_lookup,
                        &cur_sfi.shard,
                        &mut reader,
                    )? {
                        let new_sfi = write_out_with_new_intershard_reference_section(
                            &cur_sfi.shard,
                            &mut reader,
                            session_directory,
                            new_irs,
                        )?;

                        shards_to_remove.push((cur_sfi.shard_hash, cur_sfi.path.to_path_buf()));
                        new_sfi
                    } else {
                        cur_sfi.clone()
                    }
                };

                add_shard_to_cas_to_shard_lookup(&mut intershard_lookup, &new_sfi)?;

                finished_shard_hashes.insert(new_sfi.shard_hash);
                finished_shards.push(new_sfi);
            } else {
                // We have one or more shards to merge, so do this all in memory.

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

                // Have the intershard references changed or been added to?  If so change it and write out the shard
                // with the changed version.  If not, write it directly.
                let new_sfi = {
                    if let Some(new_irs) = add_lookups_to_intershard_reference_section(
                        &intershard_lookup,
                        &cur_sfi.shard,
                        &mut Cursor::new(&cur_data),
                    )? {
                        write_out_with_new_intershard_reference_section(
                            &cur_sfi.shard,
                            &mut Cursor::new(&cur_data),
                            session_directory,
                            new_irs,
                        )?
                    } else {
                        MDBShardFile::write_out_from_reader(
                            session_directory,
                            &mut Cursor::new(&cur_data),
                        )?
                    }
                };

                debug!(
                    "Created merged shard {:?} from shards {:?}",
                    &new_sfi.path,
                    shards[cur_idx..ub_idx].iter().map(|sfi| &sfi.path)
                );

                finished_shard_hashes.insert(new_sfi.shard_hash);
                finished_shards.push(new_sfi.clone());
                add_shard_to_cas_to_shard_lookup(&mut intershard_lookup, &new_sfi)?;
                finished_shards.push(new_sfi);

                // Delete the old ones.
                for sfi in shards[cur_idx..ub_idx].iter() {
                    shards_to_remove.push((sfi.shard_hash, sfi.path.to_path_buf()));
                }
            }

            for (shard_hash, path) in shards_to_remove.iter() {
                if finished_shard_hashes.contains(shard_hash) {
                    // In rare cases, there could be empty shards or shards with
                    // duplicate entries and we don't want to delete any shards
                    // we've already finished
                    continue;
                }
                debug!(
                    "consolidate_shards: Removing {:?}; info merged to {:?}",
                    &path,
                    &finished_shards.last().unwrap().shard_hash
                );

                std::fs::remove_file(&path)?;
            }

            cur_idx = ub_idx;
        }
    }

    Ok(finished_shards)
}
