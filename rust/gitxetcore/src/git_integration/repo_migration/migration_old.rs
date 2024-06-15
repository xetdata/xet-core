use crate::data::{is_xet_pointer_file, PointerFileTranslatorV2};
use crate::errors::Result;
use crate::git_integration::git_xet_repo::GITATTRIBUTES_CONTENT;
use crate::git_integration::GitXetRepo;
use crate::stream::stdout_process_stream::AsyncStdoutDataIterator;
use crate::utils::ref_to_oid;
use blake3::Hash;
use git2::{Commit, Object, ObjectType, Oid, Repository, Tree};
use more_asserts::assert_ge;
use progress_reporting::DataProgressReporter;
use serde::de;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use super::data_import::*;
use super::reporting::*; 
use super::utils::*; 

// Tree processsing functions. 
fn get_nonnote_tree_dependents(src : &Repository, oid : Oid) -> Vec<Oid> { 
    let mut dependents = vec![];

                    let Some(tree) = obj.as_tree() else { 
                        tr_warn!("Tree Oid {oid} not actually accessible as tree, ignoring.");
                        return vec![];
                    }; 

                    for entry in tree.iter() {
                        dependents.push(entry.id());
                    }
                dependents
}

fn convert_nonnote_tree(src : &Repository, dest : &Repository, obj : Object, is_base_dir_tree : bool, entry_tr_map : &HashMap<Oid, Oid>) -> Result<Oid> {
    let oid = obj.id();

                    let Some(tree) = obj.as_tree() else { 
                        tr_warn!("Tree {oid} not actually a tree in source repo, passing through.");
                        return Ok(oid);
                    };

                    let mut tree_builder = dest.treebuilder(None)?;

                    for entry in tree.iter() {
                        let src_entry_oid = entry.id();

                        let Some(dest_entry_oid) = entry_tr_map.get(&src_entry_oid) else {
                            tr_panic!("Logic error: ignoring {src_entry_oid} in tree {t_oid}.");
                            continue; 
                        };

                    if is_base_dir_tree {
                        if let Some(".gitattributes") = entry.name() {
                            continue;
                        }
                    }

                }
                
                if is_base_dir_tree {
                    let gitattributes_oid = dest.blob(GITATTRIBUTES_CONTENT.as_bytes())?;
                    // Add in the .gitattributes entry explicitly, as this is a root commit.
                    tree_builder.insert(".gitattributes", gitattributes_oid, 0o100644)?;
                }

                    let new_oid = tree_builder.write()?;
                    Ok(new_oid)

}

fn get_note_tree_dependents(src : &Repository, oid : Oid, known_tr_map : HashMap<Oid, Oid>) -> Vec<Oid> { 
    let mut dependents = vec![];
    let mut name_dependents = vec![];

                    let Some(tree) = obj.as_tree() else { 
                        tr_warn!("Tree Oid {oid} not actually accessible as tree, ignoring.");
                        return vec![];
                    }; 

                    for entry in tree.iter() {
                        dependents.push(entry.id());
                            
                        // Possibly the names are oids (in the case of notes), and possibly
                        // these are attached to note objects.  Convert these correctly.
                        if let Some(attached_oid) = extract_name_oid(oid, &entry) {
                            if !known_tr_map.contains_key(&attached_oid) {
                                dependents.push(attached_oid);
                            }
                        }
                    }
                dependents
}

fn convert_note_tree(src : &Repository, dest : &Repository, obj : Object, entry_tr_map : &HashMap<Oid, Oid>, attached_tr_map : &HashMap<Oid, Oid>) -> Result<Oid> {
    let oid = obj.id();

                    let Some(tree) = obj.as_tree() else { 
                        tr_warn!("Tree {oid} not actually a tree in source repo, passing through.");
                        return Ok(oid);
                    };

                    let mut tree_builder = dest.treebuilder(None)?;

                    for entry in tree.iter() {
                        let src_entry_oid = entry.id();

                        let Some(dest_entry_oid) = entry_tr_map.get(&src_entry_oid) else {
                            tr_panic!("Logic error: ignoring {src_entry_oid} in tree {t_oid}.");
                            continue; 
                        };

                        let Some(name) = entry.name() else { 
                            tree_builder.insert(
                            entry.name_bytes(), 
                                dest_entry_oid, entry.filemode_raw())?;
                            continue;
                        };

                        if let Some(src_attached_oid) = extract_name_oid(oid, &entry) {
                            if let Some(dest_oid) = attached_tr_map.get(&src_attached_oid) {
                                tree_builder.insert(
                                dest_attached_oid.to_string().as_bytes(), dest_entry_oid, entry.filemode_raw())?;
                            } else {
                                tr_panic!("Logic error: ignoring {src_attached_oid} in tree {oid}.");

                                // Still do this in the worst case...  
                                tree_builder.insert(
                                entry.name_bytes(), 
                                    dest_entry_oid, entry.filemode_raw())?;

                            }
                        } else {
                            tree_builder.insert(
                            entry.name_bytes(), 
                                dest_entry_oid, entry.filemode_raw())?;
                        }
                    }

                    let new_oid = tree_builder.write()?;
                    Ok(new_oid)

}


// Commit processing functions.

fn get_commit_dependents(src : &Repository, oid : Oid) -> Vec<Oid> {
    let mut dependents = vec![];
                    let Some(commit) = obj.as_commit() else { 
                        tr_warn!("Commit Oid {oid} not actually accessible as commit, ignoring.");
                        return vec![];
                    }; 

                    // tree_id is a dependent. 
                    dependents.push(commit.tree_id()); 

                    // Commits reference the root trees.  
                    root_tree_oids.insert(commit.tree_id());
                    
                    // all parents  
                    for parent in commit.parents() {
                        dependents.push(parent.id());
                    }

                    // Commit messages (e.g. merges) may reference other commits as Oids.
                    if let Some(msg) = commit.message() {
                        for named_oid in extract_str_oids(&src, msg) {
                            dependents.push(named_oid);
                        }
                    }
dependents
                }


fn convert_commit(src : &Repository, dest : &Repository, obj : Object, tr_map : &HashMap<Oid, Oid>, msg_tr_map : &HashMap<Oid, Oid>) -> Result<Oid> {
    let oid = obj.id();
    
                    let Some(src_commit) = obj.as_commit() else { 
                        tr_warn!("Commit Oid {oid} in notes not actually accessible as commit, ignoring.");
                        return Ok(oid);
                    };

                    let commit_oid = src_commit.id(); 
                    let src_tree_oid = src_commit.tree_id(); 

                    // All the referenced commits here should be to locally translated objects tracked by 
                    // the above dependencies.  
                    let Some(&new_tree_id) = tr_map.get(&src_tree_oid)
                        else {
                            tr_panic!("Logic error; passing {src_tree_oid} through in commit {commit_oid}.");
                            return Ok(oid);
                        };

                    let new_parents = Vec::with_capacity(src_commit.parent_count());

                    for parent in src_commit.parents() {
                        let src_parent_oid = parent.id();
                    
                        let Some(new_parent_id) = tr_map.get(&src_parent_oid) else {
                            tr_panic!("Logic error; ignoring parent {src_parent_oid} in commit {commit_oid}.");
                            continue; 
                        };

                        new_parents.push(src_parent_oid);
                    }

                    let new_commit_msg = {
                        if let Some(msg) = src_commit.message() {
                            let new_msg = replace_oids(&src, msg, &msg_tr_map); 
                            new_msg
                        } else { 
                            unsafe { std::string::from_utf8_unchecked(src_commit.message_raw_bytes()) }
                        }
                    };

                    let new_tree = dest.find_tree(new_tree_id)?;

                    // Create a new commit in the destination repository
                    let new_commit_id = dest.commit(
                        None, // Do not update HEAD
                        &src_commit.author().to_owned(),
                        &src_commit.committer().to_owned(), // Preserves timestamp in this signature
                        &new_commit_msg, 
                        &new_tree, // Tree to attach to the new commit
                        &new_parents.iter().collect::<Vec<_>>()[..],
                    )?;

                    Ok(
                    new_commit_id)
                }

// Tag processsing functions. 

fn get_tag_dependents(src : &Repository, oid : Oid) -> Vec<Oid> {
                    let tag = repo.find_tag(tag_oid).map_err(|e| {
                        tr_warn!("Tag Oid {oid} not actually accessible as tag, ignoring ({e})");
                        e
                    }) else {
                        return vec![];
                    };

                    vec![tag.target_id()]

}

fn convert_tag(src : &Repository, dest : &Repository, obj : Object, tr_map : &HashMap<Oid, Oid>) -> Result<Oid> {
                    // Notes can attach themselves to tags  
                    let Ok(tag) = src.find_tag(tag_oid) else { 
                        tr_warn!("Tag Oid {oid} in notes not actually a tag, skipping.");
                        return Ok(tag_oid);
                    };

                    let old_target_id = tag.target_id();
                    let Some(&new_target_id) = l_tr_map.get(&old_target_id) else {
                        tr_panic!("Logic Error; target_id {old_target_id} untranslated, skipping"); 
                        return Ok(tag_oid);
                    };

                    let Ok(obj) = dest.find_object(new_target_id, None).map_err(|e| {
                    if src.find_object(old_target_id, None).is_ok() {
                        tr_panic!("Logic Error; target_id {old_target_id} untranslated to dest object: {e}");
                     } else {
                        tr_warn!("Tag target {old_target_id} for tag {} was not valid, skipping import.");
                     }
                    }) else {
                        return Ok(tag_oid);
                    };

                    let signature = tag.tagger().unwrap_or_else(||src.signature());

                    let new_tag_oid = dest.tag(unsafe { std::str::from_utf8_unchecked(tag.name_bytes())}, new_target_id, 
                     &signature, unsafe { std::str::from_utf8_unchecked(tag.message_bytes()) }, true)?;

                     Ok(new_tag_oid)
                }


fn port_blobs_directly(src : &Repository, dest : &Repository, blobs : Vec<Oid>, progress_reporting : Arc<DataProgressReporter>) -> Result<Vec<Oid, Oid>> {
   
    let mut tr_table = Vec::with_capacity(blobs.len());

    for b_oid in blobs {
        if dest.find_blob(b_oid).is_ok() {
            tr_table.push((b_oid, b_oid));
            continue;
        }

        let src_blob = src.find_blob(b_oid).map_err(|e| {
            tr_warn!("Referenced blob {b_oid} not valid in source repo; ignoring.");
            e
        }) else {
            continue;
        };

        let content = src_blob.content();
        let new_oid = dest.blob(content)?;

        trace_print!("BlobTR: {b_oid} -> {new_oid}");
        debug_assert_eq!(b_oid, new_oid);
        tr_table.push((b_oid, new_oid));
        progress_reporting.register_progress(Some(1), Some(content.len()));
    }


Ok(tr_table)
}

async fn convert_all_blobs_with_import(src : Arc<Repository>, dest : &GitXetRepo, blobs : Vec<Oid>, 
progress_reporting : Arc<DataProgressReporter>) -> Result<Vec<(Oid, Oid)> > {

   let mut tr_table = Vec::with_capacity(blobs.len());

   // Now, go through and convert all the tree objects.  
   // Blob conversion.

        // Set up the pointer file translator.
        let pft = Arc::new(
            PointerFileTranslatorV2::from_config(&xet_repo.xet_config, xet_repo.repo_salt().await?)
                .await?,
        );

        // Now, we need to only convert blobs that are reachable by a tree.  Other blobs (e.g. notes)
        // should not actually be put through the filter processes but rather translated over directly.
        let mut filterable_blobs = HashSet::new();

        for t_oid in trees_to_convert.iter() {
            let tree = src.find_tree(*t_oid)?;

            for item in tree.iter() {
                if matches!(item.kind(), Some(ObjectType::Blob)) {
                    filterable_blobs.insert(item.id());
                }
            }
        }

        // Now, run the bulk of the blob processing in parallel as
        let blob_processing_permits = Arc::new(Semaphore::new(MAX_CONCURRENT_BLOB_PROCESSING));
        let mut blob_processing_pool = JoinSet::<Result<(Oid, Vec<u8>)>>::new();

        // Now, with everything there, first go through and convert all the blobs.
        for b_oid in blobs_to_convert {
            // Add this to the filtering pool.
            let blob_processing_permits = blob_processing_permits.clone();
            let progress_reporting = progress_reporting.clone();
            let src = src.clone();
            let pft = pft.clone();
            let src_repo = src_repo.clone();
            let repo_init_tracking = repo_init_tracking.clone();

            blob_processing_pool.spawn(async move {
                let _permit = blob_processing_permits.acquire_owned().await?;

                let src_data = src.find_blob(b_oid)?.content().to_vec();

                let new_data = translate_blob_contents(
                    &src_repo,
                    pft.clone(),
                    b_oid,
                    progress_reporting,
                    src_data,
                    repo_init_tracking,
                )
                .await?;
                Ok((b_oid, new_data))
            });

            while let Some(res) = blob_processing_pool.try_join_next() {
                let (b_oid, new_data) = res??;
                let new_id = dest.blob(&new_data[..])?;

                trace_print!("BlobTR: {b_oid} -> {new_id}");
                tr_table.push((b_oid, new_id));
                progress_reporting.register_progress(Some(1), None);
            }
        }

        // Now, clear out the rest.
        while let Some(res) = blob_processing_pool.join_next().await {
            let (b_oid, new_data) = res??;
            let new_id = dest.blob(&new_data[..])?;

            trace_print!("BlobTR: {b_oid} -> {new_id}");
            tr_table.push((b_oid, new_id));
            progress_reporting.register_progress(Some(1), None);
        }

        pft.finalize().await?;
        progress_reporting.finalize();

    Ok(tr_table)
} 


pub async fn migrate_repo(
    src_repo: impl AsRef<Path>,
    xet_repo: &GitXetRepo,
) -> Result<Vec<String>> {
    let repo_init_tracking = Arc::new(RepoInitTracking::default());

    // Open the source repo
    let src_repo = src_repo.as_ref().to_path_buf();
    let src = Arc::new(git2::Repository::discover(&src_repo)?);

    // Get the dest repo
    let dest = xet_repo.repo.clone();

    let mut tr_map = HashMap::<Oid,Oid>::new(); 
    
    // Converting general things.
    //
    // 
    // 
    // Converting Notes.
    //
    // The notes are stored specially as commits with a tree object tracking the notes and their attached objects.  These 
    // trees actually use the name field of a tree entry to track the Oids they are attached to, so they need to be 
    // translated separately.  Subtrees of this style are also possible. 
    // 
    // Now the notes can be attached to any object in the repo, including other notes or to a blob, so we need to 
    // make sure to track the conversion of the name fields properly.
    // 
    // The last steps did in fact convert all the known tree objects and the commit objects, so all of these would 
    // have already been converted, but we'll trust the aggressive gc step later to clean those up.  The only things 
    // not optimally migrated will be note blobs over the threshhold size for xet data migration, which will be 
    // stored once in xet and here again in the repo, as we can't work with note data through the filters. This just 
    // means that the data there will be stored in xet but will be useless.  This is fine. 
    //
    // Also, we'll skip all the notes in 
    // 
    // The algorithm has two passes: 
    // - Build a lookup table of note oids and their dependencies. 
    // - Traverse this, diving in to parents first.  


    let progress_reporting =
        DataProgressReporter::new("XET: Importing Objects", 0, None);

    if ENABLE_TRANSLATION_TRACING {
        progress_reporting.set_active(false);
    }
    
    let mut global_tr_map = HashMap::<Oid, Oid>::new();

    // Updating the logic.
    for converting_notes in [false, true] {

        let mut seed_oids = HashSet::new(); 

        // Build up the starting points from the given refenences. 
        for maybe_reference in src.references()? {
           let Ok(reference) = maybe_reference.map_err(|e| {
                tr_warn!("Error loading reference {e:?}, skipping.");
                e
            }) else {
                continue;
            };            
            
            let Some(reference_name) = reference.name() else { 
                tr_warn!("Skipping reference with non-UTF8 name."); 
                continue;
            };

            // Check if the reference is the correct type 
            if reference.is_note() != converting_notes {
                continue;
            }

            // Skip importing of xet notes; we assume that we're rebuilding things.
            if reference.is_note() && reference_name.starts_with("refs/notes/xet/") {
                continue;
            }

            // Only convert local references
            if reference.is_remote() {
                continue;
            }

            if let Some(oid) = reference.target() { 
                seed_oids.insert(oid); 
            }
        }

        // If it's not converting notes, also add in the tags.
        if !converting_notes {
            src.tag_foreach(|oid, _| seed_oids.push(oid))?;
        }

        //////////////////////////////////////////////////////////////////////////////////////////
        //
        //  Step 1: build the dependency graph.  We track what upstream objects must be converted before 
        //  the object itself must be converted. 
        //
        //  In addition, track a list of root oids that are referenced by commits 
        //  (except for note commits).

        let mut dependency_map = HashMap::<Oid, (Vec<Oid>, usize)>::new(); 
        let mut visited_oids = HashSet::new(); 
        let mut blobs_to_convert = HashSet::new();
        let mut root_tree_oids = HashSet::new(); 

        // Start us off with the seed oids from above.        
        let mut proc_queue = Vec::from_iter(seed_oids.into_iter());

        while let Some(oid) = proc_queue.pop() { 
            if visited_oids.contains(&oid) {
                continue; 
            }

            progress_reporting.update_target(Some(1), None);

            let Ok(obj) = src.find_object(oid, None).map_err(|e| {
                tr_warn!("Referenced Oid {oid} not found in src database, passing Oid through as is."); 
                e
            }) else {
                continue; 
            } ;

            let dependents = match obj.kind().unwrap_or(ObjectType::Any) { 
                ObjectType::Tree => {
                    if converting_notes {
                        get_note_tree_dependents(&src, oid, &tr_map)?
                    } else {
                        get_nonnote_tree_dependents(&src, oid)?
                    }
                }
                ObjectType::Commit => get_commit_dependents(&src, oid), 
                ObjectType::Blob => { blob_oids.insert(oid); vec![] },
                ObjectType::Tag => get_tag_dependents(&src, oid),
                _ => {
                    tr_warn!("Object Oid {oid} in notes not blob, commit, or tree, passing through.");
                }
            };

            for d_oid in dependents {
                dependency_map.entry(d_oid).or_default().0.push(oid); 
                proc_queue.push(d_oid); 
            }
            visited_oids.insert(oid);
        }
    }
        
    //////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Step 2: Blob conversion.
    // 
    //  All the blobs are imported at once as they should be done in parallel if there is imported 
    //  data to retrieve.
    
    // Now, if needed, translate all the blobs.
    let blob_tr_table = {
        if converting_notes {
            port_blobs_directly(&src, &dest, blobs_to_convert.into_iter().convert(), progress_reporting.clone())?
        } else {
            convert_all_blobs_with_import(&src, &xet_repo, blobs_to_convert.into_iter().convert(), progress_reporting.clone()).await?
        }
    };

    let mut l_tr_map = HashMap::with_capacity(visited_oids.len());
    tr_map.reserve(visited_oids.len());

    for (src_oid, dest_oid) in blob_tr_table {
        tr_map.insert(src_oid, dest_oid);  
        l_tr_map.insert(src_oid, dest_oid);
    }

    let mut l_tr_map = HashMap::from_iter(blob_tr_table.into_iter());
    tr_map.extend(l_tr_map.iter()); 


    //////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Step 3: Non-blob conversion.
    // 
    //  Using the dependency graph, we can run through things in order.  Due to the nature of git's 
    //  Oids, we are gauranteed to not have any cycles.
    //   

    // Track the processing queue here.   
    let mut processing_queue = vec![]; 

    for (&k, (dv, idx)) in dependency_map.iter() {
        if dv.is_empty() { 
            processing_queue.push(k);
        }
    }

        // Now, run through and process everything.
        // We don't actually skip anything here based on the tr_map, 
        // as it is theoretically possible that possible that a referenced commit 
        // will have been  split.   
        while let Some(oid) = processing_queue.pop() {

            let Ok(obj) = src.find_object(oid, None).map_err(|e| {
                tr_warn!("Referenced Oid {oid} not found in src database, passing Oid through as is."); 
                e
            }) else {
                tr_map.insert(oid, oid);
                l_tr_map.insert(oid, oid); 
                continue; 
            };

            let new_oid = match obj.kind().unwrap_or(ObjectType::Any) { 
                Some(ObjectType::Tree) => {
                    if converting_notes {
                        convert_note_tree(&src, &dest, obj, &l_tr_map, &tr_map)?
                    } else {
                        convert_nonnote_tree(&src, &dest, obj, root_tree_oids.contains(oid), &l_tr_map)?
                    }
                },
                Some(ObjectType::Commit) => convert_commit(&src, &dest, obj, &l_tr_map, &tr_map)?,
                Some(ObjectType::Blob) => { // Blobs should already all have been converted. 
                    *l_tr_map.get(&oid).unwrap_or_else(|| {
                        tr_panic!("Logic Error; blob {oid} not in translation map; passing through");
                        &oid
                    })
                },
                Some(ObjectType::Tag) => convert_tag(&src, &dest, obj, &tr_map)?,
                _ => {
                    tr_warn!("Entry {oid} has object type other than blob, commit, tag, or tree; skipping.");
                }
            };

            l_tr_map.insert(oid, new_oid);
            tr_map.insert(oid, new_oid);

            // Now, go through all the oids depending on this and queue them if they are ready. 
            if let Some((dependent_oids, _)) = upstream_map.get(&oid).cloned() { 
                for d_oid in dependent_oids {
                    if let Some((_, dependent_count)) = upstream_map.get_mut(&d_oid) {
                        debug_assert_ge!(*dependent_count, 0);
                        *dependent_count -= 1; 
                        if *dependent_count == 0 {
                            processing_queue.push(d_oid); 
                        }
                    } else {
                        tr_panic!("Bad logic.");
                    }
                }
            } else {
                tr_panic!("Bad logic.");
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Step 4: Add in all the references.  Their target oids should all have been properly converted already.  
    // 
    //  Using the dependency graph, we can run through things in order.  Due to the nature of git's 
    //  Oids, we are gauranteed to not have any cycles.

        










    // Go through the object database and enumerate all the types in it.
    let mut trees_to_convert: Vec<Oid> = Vec::new();
    let mut blobs_to_convert: Vec<Oid> = Vec::new();
    let mut commits_to_convert: Vec<Oid> = Vec::new();
    let mut tags_to_convert: Vec<Oid> = Vec::new();

    let mut anytype_oids: Vec<Oid> = Vec::new();

    let mut unknown_oids: Vec<Oid> = Vec::new();

    tr_print!("Scanning repository (working directory = {src_repo:?}).");

    let mut issues = OIDIssueTracker::default(); 

    // Go through and put everything in the odb into the destination repo.
    let odb = src.odb()?;
    odb.foreach(|&oid| {
        let Ok((_, o_type)) = odb.read_header(oid).map_err(|e| {
            tr_warn!("Error encountered reading info of oid {oid:?}: {e:?}");
            e
        }) else {
            unknown_oids.push(oid);
            return true;
        };

        match o_type {
            ObjectType::Any => {
                anytype_oids.push(oid);
            }
            ObjectType::Commit => {
                commits_to_convert.push(oid);
            }
            ObjectType::Tree => {
                trees_to_convert.push(oid);
            }
            ObjectType::Blob => {
                blobs_to_convert.push(oid);
            }
            ObjectType::Tag => {
                tags_to_convert.push(oid);
            }
        }

        true
    })?;

    // The translation map of old hash to new hash. 
    let mut blob_tr_map = HashMap::<Oid, Oid>::new();
    
   // Now, go through and convert all the tree objects.  
   // Blob conversion.
    {
        let progress_reporting =
            DataProgressReporter::new("XET: Importing files", Some(blobs_to_convert.len()), None);

        if ENABLE_TRANSLATION_TRACING {
            progress_reporting.set_active(false);
        }

        // Set up the pointer file translator.
        let pft = Arc::new(
            PointerFileTranslatorV2::from_config(&xet_repo.xet_config, xet_repo.repo_salt().await?)
                .await?,
        );

        // Now, we need to only convert blobs that are reachable by a tree.  Other blobs (e.g. notes)
        // should not actually be put through the filter processes but rather translated over directly.
        let mut filterable_blobs = HashSet::new();

        for t_oid in trees_to_convert.iter() {
            let tree = src.find_tree(*t_oid)?;

            for item in tree.iter() {
                if matches!(item.kind(), Some(ObjectType::Blob)) {
                    filterable_blobs.insert(item.id());
                }
            }
        }

        // Now, run the bulk of the blob processing in parallel as
        let blob_processing_permits = Arc::new(Semaphore::new(MAX_CONCURRENT_BLOB_PROCESSING));
        let mut blob_processing_pool = JoinSet::<Result<(Oid, Vec<u8>)>>::new();

        // Now, with everything there, first go through and convert all the blobs.
        for b_oid in blobs_to_convert {
            if filterable_blobs.contains(&b_oid) {
                // Add this to the filtering pool.
                let blob_processing_permits = blob_processing_permits.clone();
                let progress_reporting = progress_reporting.clone();
                let src = src.clone();
                let pft = pft.clone();
                let src_repo = src_repo.clone();
                let repo_init_tracking = repo_init_tracking.clone();

                blob_processing_pool.spawn(async move {
                    let _permit = blob_processing_permits.acquire_owned().await?;

                    let src_data = src.find_blob(b_oid)?.content().to_vec();

                    let new_data = translate_blob_contents(
                        &src_repo,
                        pft.clone(),
                        b_oid,
                        progress_reporting,
                        src_data,
                        repo_init_tracking,
                    )
                    .await?;
                    Ok((b_oid, new_data))
                });
            } else {
                let src_blob = src.find_blob(b_oid)?;
                let content = src_blob.content();
                let new_oid = dest.blob(content)?;

                trace_print!("BlobTR: {b_oid} -> {new_oid}");
                blob_tr_map.insert(b_oid, new_oid);
                progress_reporting.register_progress(Some(1), Some(content.len()));
            }

            while let Some(res) = blob_processing_pool.try_join_next() {
                let (b_oid, new_data) = res??;
                let new_id = dest.blob(&new_data[..])?;

                trace_print!("BlobTR: {b_oid} -> {new_id}");
                blob_tr_map.insert(b_oid, new_id);
                progress_reporting.register_progress(Some(1), None);
            }
        }

        // Now, clear out the rest.
        while let Some(res) = blob_processing_pool.join_next().await {
            let (b_oid, new_data) = res??;
            let new_id = dest.blob(&new_data[..])?;

            trace_print!("BlobTR: {b_oid} -> {new_id}");
            blob_tr_map.insert(b_oid, new_id);
            progress_reporting.register_progress(Some(1), None);
        }

        pft.finalize().await?;
        progress_reporting.finalize();
    }

    

    // Now, convert all the trees that are not part of the notes.  
    {
        let progress_reporting = DataProgressReporter::new(
            "XET: Importing directory structures",
            Some(trees_to_convert.len()),
            None,
        );
        if ENABLE_TRANSLATION_TRACING {
            progress_reporting.set_active(false);
        }

        // Build up a set of trees that are known to be root trees.
        // These we need to overwrite the gitattributes for.
        let mut root_trees = HashSet::new();
        let mut tree_parents = HashMap::<Oid, Vec<usize>>::new();

        // Populate the set of root trees so we know which trees are root trees and which are not.
        for c_oid in commits_to_convert.iter() {
            let commit = src.find_commit(*c_oid)?;
            root_trees.insert(commit.tree_id());
            tree_parents.insert(commit.tree_id(), vec![]);
        }

        // All the trees here, with the number of processing parts to utilize.
        let mut trees = Vec::<(Tree, usize)>::new();

        // Track the parents for bookkeeping

        let mut processing_queue = Vec::new();

        // Populate the above lists
        for t_oid in trees_to_convert {
            let tree = src.find_tree(t_oid)?;

            // Defer notes and commit trees til later
            if note_trees.contains(&t_oid) {
                tr_print!("Tree {t_oid} is a note tree, deferring.");
                continue;
            }

            let t_idx = trees.len();

            let mut n_subtrees = 0;
            for item in tree.iter() {
                match item.kind().unwrap_or(ObjectType::Any) {
                    ObjectType::Tree => {
                        tree_parents.entry(item.id()).or_default().push(t_idx);
                        n_subtrees += 1;
                    }
                    ObjectType::Blob => {}
                    ObjectType::Commit => {
                        if src.find_commit(item.id()).is_ok() {
                            // TODO: if this is a thing, then we should handle it, but
                            // I think this mainly happens with subrepos.
                            tr_warn!(
                                "Commit {} found in tree {t_oid:?}, passing through.",
                                item.id()
                            );
                        }
                    }
                    t => {
                        tr_warn!(
                            "Entry {:?} of type {t:?} encountered in tree {t_oid:?}, ignoring.",
                            item.id()
                        );
                    }
                }
            }

            trees.push((tree, n_subtrees));

            if n_subtrees == 0 {
                processing_queue.push(t_idx);
            }
        }

        // Now, go through the queue and run with all of these.
        while let Some(next_idx) = processing_queue.pop() {
            let (src_tree_id, new_tree_id, parents) = {
                // Contain the source tree.
                let src_tree = &trees[next_idx].0;
                debug_assert_eq!(trees[next_idx].1, 0);

                // Now, is it a root node?  If so, we need to be careful about .gitattributes.
                let Some(parents) = tree_parents.get(&src_tree.id()) else {
                    error!(
                        "Repo migration logic error: {:?} not in the parent map.",
                        &src_tree.id()
                    );
                    continue;
                };
                let is_base_dir_commit = root_trees.contains(&src_tree.id());

                // Create a TreeBuilder to construct the new Tree
                let mut tree_builder = dest.treebuilder(None)?;
                let t_oid = src_tree.id();

                // Iterate over each entry in the source Tree
                for entry in src_tree.iter() {
                    let &entry_oid = &entry.id();

                    let new_oid = {
                        if matches!(
                            entry.kind().unwrap_or(ObjectType::Any),
                            ObjectType::Blob | ObjectType::Tree
                        ) {
                            // Get the translated Oid using the provided function
                            match blob_tr_map.get(&entry_oid) {
                                Some(&new_oid) => new_oid,
                                None => {
                                    if let Ok(obj) = src.find_object(entry_oid, None) {
                                        traced_warn!("In {t_oid}, entry {entry_oid} of type {:?} exists in source but not in transation table yet; passing through.", obj.kind());
                                    }
                                    entry_oid
                                }
                            }
                        } else {
                            entry_oid
                        }
                    };

                    // If the old object exists in the old repo, make sure the new object exists in the new repo.
                    #[cfg(debug_assertions)]
                    {
                        if src.find_object(entry_oid, entry.kind()).is_ok() {
                            // Make sure this entry exists in the new repo
                            assert!(dest.find_object(new_oid, entry.kind()).is_ok());
                        }
                    }

                    if is_base_dir_commit {
                        if let Some(".gitattributes") = entry.name() {
                            continue;
                        }
                    }

                    // Add the translated Oid to the new Tree with the same name and filemode
                    tree_builder.insert(entry.name_bytes(), new_oid, entry.filemode_raw())?;
                }

                if is_base_dir_commit {
                    // Add in the .gitattributes entry explicitly, as this is a root commit.
                    tree_builder.insert(".gitattributes", gitattributes_oid, 0o100644)?;
                }
                let new_tree_id = tree_builder.write()?;

                (src_tree.id(), new_tree_id, parents)
            };

            // Now, update the processing queue for the parent nodes.
            for parent_idx in parents {
                let p_count = &mut trees[*parent_idx].1;
                assert_ge!(*p_count, 1);
                *p_count -= 1;
                if *p_count == 0 {
                    processing_queue.push(*parent_idx);
                }
            }

            // Add in the new tree.
            tr_print!("TreeTR: {src_tree_id} -> {new_tree_id}");
            blob_tr_map.insert(src_tree_id, new_tree_id);

            progress_reporting.register_progress(Some(1), None);
        }

        // Now, all the trees should be converted. Make sure this is the case.
        #[cfg(debug_assertions)]
        {
            for (tr, c) in trees {
                assert_eq!(c, 0);
                let Some(new_id) = blob_tr_map.get(&tr.id()) else {
                    panic!("{:?} not in tr_map.", tr.id());
                };
                assert!(dest.find_tree(*new_id).is_ok())
            }
        }
        progress_reporting.finalize();
    }

    // Now that all the trees have been converted, go through and convert all the commits.
    // Again, this has to be done in order, so build the network of parents
    {
        let progress_reporting =
            DataProgressReporter::new("Importing commits", Some(commits_to_convert.len()), None);

        if ENABLE_TRANSLATION_TRACING {
            progress_reporting.set_active(false);
        }

        let mut commit_children = HashMap::<Oid, Vec<usize>>::new();

        let mut commits = Vec::<(Commit, usize)>::new();

        let mut processing_queue = Vec::new();

        for c_oid in commits_to_convert.iter() {
            if note_commits.contains(c_oid) {
                // These are handled later, as the tree structures have to be translated first
                // and those are translated after the commits.
                continue;
            }

            let commit = src.find_commit(*c_oid)?;

            let c_idx = commits.len();

            for p in commit.parents() {
                commit_children.entry(p.id()).or_default().push(c_idx);
            }

            let parent_count = commit.parent_count();
            commits.push((commit, parent_count));

            if parent_count == 0 {
                processing_queue.push(c_idx);
            }
        }

        while let Some(c_idx) = processing_queue.pop() {
            let (old_commit_id, new_commit_id) = {
                // All parents have been converted.
                assert_eq!(commits[c_idx].1, 0);

                let src_commit = &commits[c_idx].0;

                let mut translated_parent_commits = vec![];

                for parent in src_commit.parents() {
                    let Some(translated_oid) = blob_tr_map.get(&parent.id()) else {
                        error!("Repo migration logic error: commit parent not translated.");
                        continue;
                    };

                    let new_commit = dest.find_commit(*translated_oid)?;

                    translated_parent_commits.push(new_commit);
                }

                let Some(new_tree_id) = blob_tr_map.get(&src_commit.tree_id()) else {
                    panic!("Logic Error: commit tree id not translated.");
                };

                let new_tree = dest.find_tree(*new_tree_id)?;

                // Create a new commit in the destination repository
                let new_commit_id = dest.commit(
                    None, // Do not update HEAD
                    &src_commit.author().to_owned(),
                    &src_commit.committer().to_owned(), // Preserves timestamp in this signature
                    unsafe { std::str::from_utf8_unchecked(src_commit.message_raw_bytes()) },
                    &new_tree, // Tree to attach to the new commit
                    &translated_parent_commits.iter().collect::<Vec<_>>()[..],
                )?;

                (src_commit.id(), new_commit_id)
            };

            // Now update the bookkeeping around these commits to queue up the next commits in line
            if let Some(children) = commit_children.get(&old_commit_id) {
                for c_idx in children {
                    let p_count = &mut commits[*c_idx].1;

                    assert_ge!(*p_count, 1);
                    *p_count -= 1;
                    if *p_count == 0 {
                        processing_queue.push(*c_idx);
                    }
                }
            }

            tr_print!("CommitTR: {old_commit_id} -> {new_commit_id}");
            blob_tr_map.insert(old_commit_id, new_commit_id);
            progress_reporting.register_progress(Some(1), None);
        }

        // Now, all commits should have been converted.  Make sure this is the case.
        #[cfg(debug_assertions)]
        {
            for (cm, c) in commits {
                assert_eq!(c, 0);
                let Some(new_id) = blob_tr_map.get(&cm.id()) else {
                    panic!("Logic Error: commit {:?} not in translation map.", cm.id());
                };
                assert!(dest.find_commit(*new_id).is_ok());
            }
        }
        progress_reporting.finalize();
    }


        


            for d_oid in ).0 { 


            }





                (src_commit.id(), new_commit_id)






                    let (new_commit_msg, _) = {

                     } else {
                    } 
                        
                        







                    }

                    // tree_id and any parents are the dependents.
                    dependents.push(commit.tree_id()); 

                    for parent in commit.parents() {
                        dependents.push(parent.id());
                    }
                }
                Some(ObjectType::Blob) => {}
                _ => {
                    tr_warn!("Object Oid {oid} in notes not blob, commit, or tree, passing through.");
                }
            }

            



        }





                    let translated_oid = match blob_tr_map.get(&parent.id()) {
                        Some(&oid) => oid,
                        None => {
                            if pass_untranslated_oids {
                                tr_warn!(
                                    "parent {} in commit {c_oid} not translated yet.",
                                    parent.id()
                                );
                                parent.id()
                            } else {
                                tr_print!("parent_commit_id not translated yet.");
                                note_commits.insert(c_oid);
                                continue;
                            }
                        }
                    };

                    let new_commit = dest.find_commit(translated_oid)?;

                    translated_parent_commits.push(new_commit);
                }




                        },
                        _ => {
                            continue; 
                        }





                note_commits.insert(commit.id());
                note_trees.insert(commit.tree_id());

                // Now, also have to iterate into subtrees here too. 
                let mut tree_oids = vec![commit.tree_id()]; 

                while let Some(t_oid) = tree_oids.pop() { 
                    let tree = src.find_tree(t_oid)?;
            
                        match item.kind().unwrap_or(ObjectType::Any) {
                            ObjectType::Tree => {
                                if note_trees.insert(t_oid) {
                                    tree_oids.push(t_oid); 
                                }
                            }
                            _ => {} 
                        }
                    }
                }

                tr_print!(
                    "Commit {} with tree {} is from notes, deferring.",
                    commit.id(),
                    commit.tree_id()
                );
            }
        }
    }




    {
        // Now, because notes are so flexible, we need to handle these cases gracefully.  We thus repeatedly
        // iterate through all of the notes, converting what we can and putting what isn't ready back on the 
        // queue.  It's not great, but it also 
        // 
        let mut pass_untranslated_oids = false;

        tr_print!(
            "Processing {} deferred trees with commits.",
            note_trees.len()
        );
            
        let mut tree_processing_queue = Vec::from_iter(note_trees.into_iter());
        let mut commit_processing_queue = Vec::from_iter(note_commits.into_iter());


        while !note_trees.is_empty() || !note_commits.is_empty() {



            // Now, all of the remaining trees that hold commits have to also be converted.
            let mut made_progress = false;

            for t_oid in tree_processing_queue {
                let Ok(src_tree) = src.find_tree(t_oid).map_err(|e| {
                    tr_warn!("Error finding referenced note tree {t_oid}, skipping: {e:?}");
                    e
                }) else {
                    continue;
                };
                let src_tree_oid = src_tree.id();
                
                // Now, attempt to convert this tree.

                // Create a TreeBuilder to construct the new Tree
                let mut tree_builder = dest.treebuilder(None)?;
                let t_oid = src_tree.id();

                // Iterate over each entry in the source Tree
                for entry in src_tree.iter() {
                    let &entry_oid = &entry.id();

                    let new_oid = {
                        match entry.kind().unwrap_or(ObjectType::Any) {
ObjectType::Blob => { 
    // Now, for notes, we actually need the attached object to be stored as-is, so make sure it translates through directly. 

    if dest.find_blob(entry_oid).is_ok() { 
        entry_oid
    } else {
        dest_oid
    }
},
ObjectType::Tree => { 
    // See if it's been translated yet. 



                    } else {
                        tr_warn!("Non-blob object {entry_oid} encountered in notes tree {t_oid}, passing through."); 

                    if matches!(
                        entry.kind().unwrap_or(ObjectType::Any),
                        ObjectType::Blob | ObjectType::Tree
                    ) {
                        // Get the translated Oid using the provided function
                        match tr_map.get(&entry_oid) {
                            Some(&new_oid) => new_oid,
                            None => {
                                if let Ok(obj) = src.find_object(entry_oid, None) {
                                    tr_warn!("In {t_oid}, entry {entry_oid} of type {:?} exists in source but not in transation table yet; passing through.", obj.kind());
                                }
                                entry_oid
                            }
                        }
                    } else {
                        entry_oid
                    }
                };

                // If the old object exists in the old repo, make sure the new object exists in the new repo.
                #[cfg(debug_assertions)]
                {
                    if src.find_object(entry_oid, entry.kind()).is_ok() {
                        // Make sure this entry exists in the new repo
                        assert!(dest.find_object(new_oid, entry.kind()).is_ok());
                    }
                }

                if is_note_tree {
                    // Note reference annotations are stored as the hexidecimal of the oid in the path field
                    if let Some(hex_oid) = entry.name().or_else(|| {
                        tr_warn!(
                            "UTF-8 Error unpacking path of entry {} on tree {t_oid}",
                            entry.id()
                        );
                        None
                    }) {
                        if let Ok(path_oid) = Oid::from_str(hex_oid).map_err(|e| {
                            tr_warn!(
                                "Error converting path {hex_oid} of entry {} on tree {t_oid}",
                                entry.id()
                            );
                            e
                        }) {
                            if let Some(new_path_oid) = tr_map.get(&path_oid) {
                                tree_builder.insert(
                                    format!("{new_path_oid}").as_bytes(),
                                    new_oid,
                                    entry.filemode_raw(),
                                )?;
                                continue;
                            } else {
                                tr_print!("Path {path_oid} of entry {} on tree {t_oid} not in translation map yet", entry.id());

                                if !allow_nontranslated_path_oids {
                    // If this returns None, then it means that some of the paths are actually oids that are commits but not in the tr_map yet.
                    // So put this back on the queue.
                    note_trees.insert(t_oid);
                    continue;
                }

                            }
                        }
                    }
                }

                // Add the translated Oid to the new Tree with the same name and filemode
                tree_builder.insert(entry.name_bytes(), new_oid, entry.filemode_raw())?;
            }

            let new_tree_id = tree_builder.write()?;
            new_tree_id
        };



                tr_print!("Tree of Commits TR: {src_tree_oid} -> {new_tree_id}");
                tr_map.insert(src_tree_oid, new_tree_id);
                made_progress = true;
            }

            // Now, convert all the commits
            for c_oid in commit_processing_queue {
                let src_commit = src.find_commit(c_oid)?;

                let mut translated_parent_commits = vec![];

                for parent in src_commit.parents() {
                    let translated_oid = match tr_map.get(&parent.id()) {
                        Some(&oid) => oid,
                        None => {
                            if pass_untranslated_oids {
                                tr_warn!(
                                    "parent {} in commit {c_oid} not translated yet.",
                                    parent.id()
                                );
                                parent.id()
                            } else {
                                tr_print!("parent_commit_id not translated yet.");
                                note_commits.insert(c_oid);
                                continue;
                            }
                        }
                    };

                    let new_commit = dest.find_commit(translated_oid)?;

                    translated_parent_commits.push(new_commit);
                }

                let src_tree_id = src_commit.tree_id();
                let new_tree_id = match tr_map.get(&src_tree_id) {
                    Some(&oid) => oid,
                    None => {
                        if pass_untranslated_oids {
                            tr_warn!("Passing tree ID {src_tree_id} through without translation.");
                            src_tree_id
                        } else {
                            tr_print!("tree_id not translated yet.");
                            note_commits.insert(c_oid);
                            continue;
                        }
                    }
                };

                let new_tree = dest.find_tree(new_tree_id)?;

                // Create a new commit in the destination repository
                let new_commit_id = dest.commit(
                    None, // Do not update HEAD
                    &src_commit.author().to_owned(),
                    &src_commit.committer().to_owned(), // Preserves timestamp in this signature
                    unsafe { std::str::from_utf8_unchecked(src_commit.message_raw_bytes()) },
                    &new_tree, // Tree to attach to the new commit
                    &translated_parent_commits.iter().collect::<Vec<_>>(),
                )?;

                tr_map.insert(c_oid, new_commit_id);
                made_progress = true;
            }

            if !made_progress {
                tr_print!("No more prgress made; forcing the rest of the OIDs through.");
                pass_untranslated_oids = true
            }
        }
    }
    // Convert all the tags
    {
        for tag_id in tags_to_convert {
            let tag = src.find_tag(tag_id)?;

            let Some(new_target) = tr_map.get(&tag.target_id()) else {
                tr_warn!(
                    "Tag {:?} references OID {:?} not in new repo; skipping.",
                    tag.name().unwrap_or("NONAME"),
                    tag.target_id(),
                );
                continue;
            };
            let new_target_obj = dest.find_object(*new_target, None)?;

            let new_tag_id = {
                if let (Some(message), Some(author)) = (tag.message_bytes(), tag.tagger()) {
                    dest.tag(
                        unsafe { std::str::from_utf8_unchecked(tag.name_bytes()) },
                        &new_target_obj,
                        &author.to_owned(),
                        unsafe { std::str::from_utf8_unchecked(message) },
                        true,
                    )?
                } else {
                    dest.tag_lightweight(
                        unsafe { std::str::from_utf8_unchecked(tag.name_bytes()) },
                        &new_target_obj,
                        true,
                    )?
                }
            };

            tr_print!("Tag TR: {tag_id} -> {new_tag_id}");
            tr_map.insert(tag_id, new_tag_id);
        }
    }

    // Now, import all the notes as well.
    {
        tr_print!("Converting notes.");

        let mut notes_to_convert = HashMap::<Option<String>, Vec<(Oid, Oid)>>::new();
        let mut seen_note_oids = HashSet::new();

        // First, add in all the notes having explicit references in the source repository.
        for reference in src.references()? {
            let reference = reference?;
            let reference_name = match reference.name() {
                Some(name) => name.to_string(),
                None => continue,
            };

            // Check if the reference is a notes reference
            if !reference.is_note() {
                continue;
            }

            // Skip importing of xet notes; we assume that we're rebuilding things.
            if reference_name.starts_with("refs/notes/xet/") {
                continue;
            }

            // Retrieve the notes from the source repository
            let Ok(notes) = src.notes(Some(&reference_name)).map_err(
                |e| {
                    tr_warn!("Error iterating through notes in source repo with reference {reference_name}); skipping."); 
                    e
            }) else { continue; };

            for (i, note) in notes.enumerate() {
                if let Ok((note_oid, annotation_oid)) = note.map_err(|e| {
                    tr_warn!("Error retrieving note #{i} in source repo with reference {reference_name}); skipping."); 
                    e
                }) {
                    notes_to_convert.entry(Some(reference_name.clone())).or_default().push( (note_oid, annotation_oid) );
                    seen_note_oids.insert(note_oid);
                }
            }
        }

        // Now translate all the notes.  Notes, however, may attach to other notes, which means that we need to actually
        // translate the Oids through them as well.
        for (reference_name, note_list) in notes_to_convert.into_iter() {
            for (src_note_oid, src_annotation_oid) in note_list {
                tr_print!(
                    "NOTES: Source_annotation_oid {src_annotation_oid} has type {:?}",
                    src.find_object(src_annotation_oid, None)
                        .ok()
                        .map(|obj| obj.kind())
                );
                // The unwrapping here handles the case of a note attaching to another note.
                // Note content is not translated, so the Oids don't change.
                let dest_annotation_oid = match tr_map.get(&src_annotation_oid) {
                    Some(&oid) => {
                        tr_print!("src_annotation_oid:{src_annotation_oid} -> {oid}");
                        oid
                    }
                    None => {
                        tr_print!("src_annotation_oid:{src_annotation_oid} not in table.");

                        // Now it gets a little tricky, as this may be something that hasn't yet been committed.
                        src_annotation_oid
                    }
                };

                let notes_ref = reference_name.as_deref();

                // Note: this actually finds notes by src_annotated oid, not the note oid. Unfortunately, this
                // really isn't right api as this returns only the first note, not multiple.  To make this robust,
                // we actually need to just pull the signature from this one,

                let (author, committer) = {
                    match src.find_note(notes_ref, src_annotation_oid) {
                        Ok(source_note) => {
                            if source_note.id() != src_note_oid {
                                tr_info!("Pulling in signature from note {:?}", source_note.id());
                            }
                            (
                                source_note.author().to_owned(),
                                source_note.committer().to_owned(),
                            )
                        }
                        Err(e) => {
                            tr_warn!("Error finding author/committer info for note {notes_ref:?} ({e:?}), using source repository config defaults.");
                            let Ok(src_sig) = src.signature() else {
                                tr_warn!("Error retrieving default author/committer info for {notes_ref:?}, skipping.");
                                continue;
                            };
                            (src_sig.clone(), src_sig)
                        }
                    }
                };

                let Ok(note_obj) = src.find_object(src_note_oid, None).map_err(|e| {
                    tr_warn!("Error retrieving note content from source repo; skipping {e:?}.");
                    e
                }) else {
                    continue;
                };

                let Ok(blob) = note_obj.peel_to_blob().map_err(|e| {
                    tr_warn!("Note object is not of blob type, skipping. {e:?}.");
                    e
                }) else {
                    continue;
                };

                let Ok(note_content) = std::str::from_utf8(blob.content()) else {
                    tr_warn!("Note content is not UTF8 compatible, skipping.");
                    continue;
                };

                let Ok(dest_note_oid) = dest.note(
                            &author,
                            &committer,
                            notes_ref,
                            dest_annotation_oid,
                            note_content,
                            true
                        ).map_err(|e| {
                            tr_warn!("Error inserting note with source oid {src_note_oid} into new repository; skipping."); 
                            e}) else {continue };

                assert!(dest.find_object(dest_annotation_oid, None).is_ok());
                assert!(dest.find_object(dest_note_oid, None).is_ok());

                tr_print!("Converted note {src_note_oid} on {src_annotation_oid:?} to {dest_note_oid} on {dest_annotation_oid:?}.");
                tr_map.insert(src_note_oid, dest_note_oid);
            }
        }
    }

    let mut branch_list = Vec::new();

    // Convert all the references.  Ignore any in xet (as this imports things in a new way).
    {
        // Add some logic to update HEAD at the end to one of these.
        let mut importing_master = false;

        // Later symbolic branches to put in
        let mut symbolic_refs = vec![];

        for maybe_reference in src.references()? {
            let Ok(reference) = maybe_reference.map_err(|e| {
                error!("Error loading reference {e:?}, skipping.");
                e
            }) else {
                continue;
            };

            // Delay the symbolic references.
            if matches!(reference.kind(), Some(git2::ReferenceType::Symbolic)) {
                symbolic_refs.push(reference);
                continue;
            }

            let Some(name) = reference.name() else {
                error!("Error getting name of reference, skipping.");
                continue;
            };

            // Now, it's a direct branch.
            if reference.is_branch() {
                // Create a branch.

                let Some(commit_id) = reference.target() else {
                    tr_warn!("Reference {name} is without target; skipping ");
                    continue;
                };

                let Some(new_commit_id) = tr_map.get(&commit_id) else {
                    tr_warn!("Reference {name} has commit not in translation table, skipping.");
                    continue;
                };

                let target_commit = dest.find_commit(*new_commit_id)?;

                let branch_name = reference.shorthand().unwrap_or(name);
                dest.branch(branch_name, &target_commit, true)?;

                if branch_name == "master" {
                    importing_master = true;
                }
                branch_list.push(branch_name.to_owned());

                tr_print!("Set up branch {branch_name}");
            } else if reference.is_note() {
                let Some(target_oid) = reference.target() else {
                    tr_warn!("Reference {name} is without target; skipping ");
                    continue;
                };

                let Some(&new_target_oid) = tr_map.get(&target_oid) else {
                    tr_warn!("Reference {name} has target not in translation table, skipping.");
                    continue;
                };

                let _ = dest.reference(
                    name,
                    new_target_oid,
                    true,
                    &format!("Importing reference {name}."),
                ).map_err(|e|
                    {
                        tr_warn!("Error setting notes reference {name} to {new_target_oid:?} in destination; skipping"); 
                        e
                    });
                tr_print!(
                    "Set up reference {name}, src oid = {target_oid}, dest oid = {new_target_oid}"
                );
            } else if reference.is_remote() {
                tr_info!("Skipping import of remote reference {name}.");
            } else if reference.is_tag() {
                let Some(tag_id) = reference.target() else {
                    tr_warn!("Reference {name} is without target; skipping ");
                    continue;
                };

                let Some(new_tag_id) = tr_map.get(&tag_id) else {
                    tr_warn!("Reference {name} has tag not in translation table, skipping.");
                    continue;
                };

                let _ = dest.reference(
                    name,
                    *new_tag_id,
                    true,
                    &format!("Imported reference {name}"),
                ).map_err(|e|
                    {
                        tr_warn!("Error setting tag reference {name} to {new_tag_id:?} in destination; skipping"); 
                        e
                    });
            }
        }

        // Now, if importing master, create a symbolic reference from main to master,
        if importing_master {
            // Set up a symbolic refenrence from main to master, so that main is an alias here.
            let _ = dest
                .reference_symbolic(
                    "main",
                    "refs/heads/master",
                    true,
                    "Add symbolic reference main to point to master.",
                )
                .map_err(|e| {
                    tr_warn!(
                        "Error setting main (xethub default) to resolve to imported branch master; skipping."
                    );
                    e
                });
        }

        let _ = dest.set_head("refs/heads/main").map_err(|e| {
            tr_warn!("Error setting HEAD to imported branch main: {e:?}");
            e
        });

        // Now, resolve all the symbolic references.
        for reference in symbolic_refs {
            let Some(name) = reference.name() else {
                tr_info!("Error getting name of reference in symbolic refs without UTF-8 name; skipping.");
                continue;
            };

            let Some(target) = reference.symbolic_target() else {
                tr_warn!("Symbolic reference {name} has no target; skipping import.");
                continue;
            };

            let _ = dest
                .reference_symbolic(name, target, true, &format!("Imported reference {name}"))
                .map_err(|e| {
                    tr_warn!(
                        "Error setting symbolic reference {name} to point to {target}; ignoring.",
                    );
                    e
                });
        }
    }

    Ok(branch_list)
}
