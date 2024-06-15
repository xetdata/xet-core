use crate::data::PointerFileTranslatorV2;
use crate::errors::Result;
use crate::git_integration::git_xet_repo::GITATTRIBUTES_CONTENT;
use crate::git_integration::GitXetRepo;
use git2::{Object, ObjectType, Oid, Repository, Signature};
use more_asserts::debug_assert_lt;
use progress_reporting::DataProgressReporter;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

use super::data_import::*;
use super::utils::*;

const MAX_CONCURRENT_BLOB_PROCESSING: usize = 64;

// A utility to help figure out logic errors.
//
// Change to true to enable tracing through what all is going on.  When true, conversions
// and other things are tracked and printed in a consistent format to make it easy to see what's
// going on and debug what's converted.  This shouldn't be needed unless there are specific bugs
// to track down or more development work here is needed, but in that case it's incredibly helpful.
//
// Keep set to false for all production use.
//
const ENABLE_TRANSLATION_TRACING: bool = true;

// These macros conditionally direct the printing based on the above flag.

macro_rules! tr_print {
    ($($arg:tt)*) => {
        if ENABLE_TRANSLATION_TRACING {
            eprintln!($($arg)*);
        }
    };
}

macro_rules! tr_warn {
    ($($arg:tt)*) => {
        if ENABLE_TRANSLATION_TRACING {
            eprintln!("WARNING: {}", format!($($arg)*));
        } else {
            warn!($($arg)*);
        }
    };
}

macro_rules! tr_info {
    ($($arg:tt)*) => {
        if ENABLE_TRANSLATION_TRACING {
            eprintln!($($arg)*);
        } else {
            info!($($arg)*);
        }
    };
}

macro_rules! tr_panic {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        {
            panic!($($arg)*);
        }
        #[cfg(not(debug_assertions))]
        {
            if ENABLE_TRANSLATION_TRACING {
                panic!($($arg)*);
            } else {
                Err(GitXetRepoError::Other(format!($($arg)*)))?;
                unreachable!();
            }
        }
    };
}

// Tree processsing functions.
fn get_nonnote_tree_dependents(obj: Object) -> Vec<Oid> {
    let mut dependents = vec![];
    let oid = obj.id();

    let Some(tree) = obj.as_tree() else {
        tr_warn!("Tree oid {oid} not actually accessible as tree, ignoring.");
        return vec![];
    };

    tr_print!("Dependences of Tree {oid}:");

    for entry in tree.iter() {
        tr_print!(" -> Dep: {}", entry.id());
        dependents.push(entry.id());
    }
    dependents
}

/// Converts a non-note tree
fn convert_nonnote_tree(
    dest: &Repository,
    obj: Object,
    is_base_dir_tree: bool,
    entry_tr_map: &HashMap<Oid, Oid>,
) -> Result<Oid> {
    let oid = obj.id();

    let Some(tree) = obj.as_tree() else {
        tr_warn!("Tree {oid} not actually a tree in source repo, passing through.");
        return Ok(oid);
    };
    tr_print!("Converting tree {oid}:");

    let mut tree_builder = dest.treebuilder(None)?;

    for entry in tree.iter() {
        let src_entry_oid = entry.id();

        let Some(&dest_entry_oid) = entry_tr_map.get(&src_entry_oid) else {
            tr_panic!("Logic error: ignoring {src_entry_oid} in tree {oid}.");
        };

        if is_base_dir_tree {
            if let Some(".gitattributes") = entry.name() {
                continue;
            }
        }

        tree_builder.insert(entry.name_bytes(), dest_entry_oid, entry.filemode_raw())?;
        tr_print!(
            " -> Entry {}: {} -> {}",
            entry.name().unwrap_or("NON UTF8"),
            src_entry_oid,
            dest_entry_oid
        );
    }

    if is_base_dir_tree {
        let gitattributes_oid = dest.blob(GITATTRIBUTES_CONTENT.as_bytes())?;

        tr_print!("  Base dir tree; Adding .gitattributes with {gitattributes_oid}");

        // Add in the .gitattributes entry explicitly, as this is a root commit.
        tree_builder.insert(".gitattributes", gitattributes_oid, 0o100644)?;
    }

    let new_oid = tree_builder.write()?;
    tr_print!("Converted Tree: {} -> {}", oid, new_oid);
    Ok(new_oid)
}

/// Extracts the name oids from a note tree entry path.
fn extract_name_oid(t_oid: Oid, entry: &git2::TreeEntry) -> Option<Oid> {
    let Some(hex_oid) = entry.name().or_else(|| {
        tr_warn!(
            "UTF-8 Error unpacking path of entry {} on tree {t_oid}",
            entry.id()
        );
        None
    }) else {
        return None;
    };

    if hex_oid.len() != 40 {
        return None;
    }

    let Ok(path_oid) = Oid::from_str(hex_oid).map_err(|e| {
        tr_warn!(
            "Error converting path {hex_oid} of entry {} on note tree {t_oid}, passing through.",
            entry.id()
        );
        e
    }) else {
        return None;
    };
    Some(path_oid)
}

/// Getting all the dependent OIDs in a tree that is used for notes.
fn get_note_tree_dependents(
    src: &Repository,
    obj: Object,
    full_tr_map: &HashMap<Oid, Oid>,
) -> Vec<Oid> {
    let mut dependents = vec![];

    let oid = obj.id();

    let Some(tree) = obj.as_tree() else {
        tr_warn!("Tree Oid {oid} not actually accessible as tree, ignoring.");
        return vec![];
    };
    tr_print!("Dependences of Note Tree {oid}:");

    for entry in tree.iter() {
        tr_print!(" -> Dep: {}", entry.id());
        dependents.push(entry.id());

        // Possibly the names are oids (in the case of notes), and possibly
        // these are attached to note objects.  Convert these correctly.
        if let Some(attached_oid) = extract_name_oid(oid, &entry) {
            if !full_tr_map.contains_key(&attached_oid) {
                if src.find_object(attached_oid, None).is_ok() {
                    tr_print!(" -> Path Dep: {attached_oid}, unknown but valid target.");
                    dependents.push(attached_oid);
                } else {
                    tr_print!(" -> Path Dep: {attached_oid} rejected, target not in source.");
                }
            } else {
                tr_print!(" -> Path Dep: {attached_oid} rejected, target already converted.");
            }
        } else {
            tr_print!(
                " -> Path Dep: '{}' rejected, not OID.",
                entry.name().unwrap_or("NON UTF8")
            );
        }
    }
    dependents
}

/// Converting a tree that is used for notes.
fn convert_note_tree(
    src: &Repository,
    dest: &Repository,
    obj: Object,
    entry_tr_map: &HashMap<Oid, Oid>,
    full_tr_map: &HashMap<Oid, Oid>,
) -> Result<Oid> {
    let oid = obj.id();

    let Some(tree) = obj.as_tree() else {
        tr_warn!("Tree {oid} not actually a tree in source repo, passing through.");
        return Ok(oid);
    };

    tr_print!("Converting Note Tree: {oid}");

    let mut tree_builder = dest.treebuilder(None)?;

    for entry in tree.iter() {
        let src_entry_oid = entry.id();

        let Some(&dest_entry_oid) = entry_tr_map.get(&src_entry_oid) else {
            tr_panic!("Logic error: ignoring {src_entry_oid} in tree {oid}.");
        };

        if let Some(src_attached_oid) = extract_name_oid(oid, &entry) {
            if let Some(dest_attached_oid) = full_tr_map.get(&src_attached_oid) {
                tree_builder.insert(
                    dest_attached_oid.to_string().as_bytes(),
                    dest_entry_oid,
                    entry.filemode_raw(),
                )?;
                tr_print!(" -> Entry: {src_entry_oid} -> {dest_entry_oid}");
                tr_print!("    -> Attached: {src_attached_oid} -> {dest_attached_oid}");
            } else {
                if src.find_object(src_attached_oid, None).is_ok() {
                    tr_panic!("Logic error: ignoring {src_attached_oid} in tree {oid}.");
                }
                tree_builder.insert(entry.name_bytes(), dest_entry_oid, entry.filemode_raw())?;

                tr_print!(" -> Entry: {src_entry_oid} -> {dest_entry_oid}, attached OID {src_attached_oid} not in source repo, passing through.");
            }
        } else {
            tree_builder.insert(entry.name_bytes(), dest_entry_oid, entry.filemode_raw())?;
            tr_print!(
                " -> Entry: {} -> {}, no attached ID.",
                src_entry_oid,
                dest_entry_oid
            );
        }
    }

    let new_oid = tree_builder.write()?;
    tr_print!("Converted Note Tree: {oid} -> {new_oid}");
    Ok(new_oid)
}

/// Get all the dependents of a commit.
fn get_commit_dependents(src: &Repository, obj: Object) -> (Vec<Oid>, Oid) {
    let oid = obj.id();
    let mut dependents = vec![];
    let Some(commit) = obj.as_commit() else {
        tr_warn!("Commit Oid {oid} not actually accessible as commit, ignoring.");
        return (vec![], Oid::zero());
    };

    tr_print!(
        "Dependencies of Commit {oid}, \"{}\": ",
        commit.summary().unwrap_or("NOT UTF8")
    );

    // The tree_id is a dependent.
    dependents.push(commit.tree_id());
    tr_print!(" -> Tree: {}", commit.tree_id());

    // All parents are dependents of course.
    for parent in commit.parents() {
        tr_print!(" -> Parent: {}", parent.id());
        dependents.push(parent.id());
    }

    // Commit messages (e.g. merges) may reference other commits as Oids.
    if let Some(msg) = commit.message() {
        for named_oid in extract_str_oids(&src, msg) {
            tr_print!(" -> Named: {named_oid}");
            dependents.push(named_oid);
        }
    }
    (dependents, commit.tree_id())
}

/// Convert a regular command
fn convert_commit(
    src: &Repository,
    dest: &Repository,
    obj: Object,
    tr_map: &HashMap<Oid, Oid>,
    msg_tr_map: &HashMap<Oid, Oid>,
) -> Result<Oid> {
    let oid = obj.id();

    let Some(src_commit) = obj.as_commit() else {
        tr_warn!("Commit Oid {oid} not actually accessible as commit, ignoring.");
        return Ok(oid);
    };

    tr_print!(
        "Converting Commit {oid}: {}",
        src_commit.summary().unwrap_or("NOT UTF8")
    );

    let src_tree_oid = src_commit.tree_id();

    // All the referenced commits here should be to locally translated objects tracked by
    // the above dependencies.
    let Some(&new_tree_id) = tr_map.get(&src_tree_oid) else {
        tr_panic!("Logic error; passing {src_tree_oid} through in commit {oid}.");
    };

    let mut new_parents = Vec::with_capacity(src_commit.parent_count());

    for parent in src_commit.parents() {
        let src_parent_oid = parent.id();

        let Some(&new_parent_id) = tr_map.get(&src_parent_oid) else {
            tr_panic!("Logic error; ignoring parent {src_parent_oid} in commit {oid}.");
        };

        let Ok(new_commit) = dest.find_commit(new_parent_id) else {
            tr_panic!(
                "Converted parent of commit {oid}: {src_parent_oid} -> {new_parent_id} not in dest as commit."
            );
        };
        tr_print!(" -> Converting parent: {src_parent_oid} -> {new_parent_id}");

        new_parents.push(new_commit);
    }

    let new_commit_msg = {
        if let Some(msg) = src_commit.message() {
            let new_msg = replace_oids(&src, msg, &msg_tr_map);
            new_msg
        } else {
            unsafe { std::str::from_utf8_unchecked(src_commit.message_raw_bytes()).to_owned() }
        }
    };

    let Ok(new_tree) = dest.find_tree(new_tree_id) else {
        tr_panic!(
            "Logic error; converted tree id {new_tree_id} of commit {oid} not found in dest."
        );
    };

    // Create a new commit in the destination repository
    let new_commit_id = dest.commit(
        None, // Do not update HEAD
        &src_commit.author().to_owned(),
        &src_commit.committer().to_owned(), // Preserves timestamp in this signature
        &new_commit_msg,
        &new_tree, // Tree to attach to the new commit
        &new_parents.iter().collect::<Vec<_>>()[..],
    )?;

    tr_print!("Commit converted: {oid} -> {new_commit_id}");

    Ok(new_commit_id)
}

/// Gets the dependent of a tag.
fn get_tag_dependents(obj: Object) -> Vec<Oid> {
    let oid = obj.id();
    let Some(tag) = obj.as_tag() else {
        tr_warn!("Tag {oid} not actually accessible as tag, ignoring.",);
        return vec![];
    };

    tr_print!("Tag dependent: {oid} -> {}", tag.target_id());

    vec![tag.target_id()]
}

/// Converts a tag
fn convert_tag(
    src: &Repository,
    dest: &Repository,
    obj: Object,
    tr_map: &HashMap<Oid, Oid>,
) -> Result<Oid> {
    let oid = obj.id();

    let Some(tag) = obj.as_tag() else {
        tr_warn!("Tag Oid {oid} not actually a tag, skipping.");
        return Ok(oid);
    };

    let old_target_id = tag.target_id();
    let Some(&new_target_id) = tr_map.get(&old_target_id) else {
        tr_panic!("Logic Error; target_id {old_target_id} untranslated, skipping");
    };

    let Ok(new_target_obj) = dest.find_object(new_target_id, None).map_err(|e| {
        if src.find_object(old_target_id, None).is_ok() {
            tr_panic!("Logic Error; target_id {old_target_id} untranslated to dest object: {e}");
        } else {
            tr_warn!(
                "Tag target {old_target_id} for tag {oid} was not valid, skipping import. ({e})"
            );
        }
    }) else {
        return Ok(oid);
    };

    let signature = tag
        .tagger()
        .unwrap_or(Signature::now("unset", "unset@unset.com").unwrap());

    let new_tag_oid = dest.tag(
        unsafe { std::str::from_utf8_unchecked(tag.name_bytes()) },
        &new_target_obj,
        &signature,
        unsafe { std::str::from_utf8_unchecked(tag.message_bytes().unwrap_or_default()) },
        true,
    )?;

    Ok(new_tag_oid)
}

fn port_blobs_directly(
    src: &Repository,
    dest: &Repository,
    blobs: Vec<Oid>,
    progress_reporting: Arc<DataProgressReporter>,
) -> Result<Vec<(Oid, Oid)>> {
    let mut tr_table = Vec::with_capacity(blobs.len());

    for b_oid in blobs {
        if dest.find_blob(b_oid).is_ok() {
            tr_table.push((b_oid, b_oid));
            continue;
        }

        let Ok(src_blob) = src.find_blob(b_oid).map_err(|e| {
            tr_warn!("Referenced blob {b_oid} not valid in source repo; ignoring.");
            e
        }) else {
            continue;
        };

        let content = src_blob.content();
        let new_oid = dest.blob(content)?;

        tr_print!("BlobTR: {b_oid} -> {new_oid}");
        debug_assert_eq!(b_oid, new_oid);
        tr_table.push((b_oid, new_oid));
        progress_reporting.register_progress(Some(1), Some(content.len()));
    }

    Ok(tr_table)
}

async fn convert_all_blobs_with_import(
    src: Arc<Repository>,
    xet_repo: &GitXetRepo,
    blobs: Vec<Oid>,
    progress_reporting: Arc<DataProgressReporter>,
) -> Result<Vec<(Oid, Oid)>> {
    let mut tr_table = Vec::with_capacity(blobs.len());
    let dest = xet_repo.repo.clone();

    let repo_init_tracking = Arc::new(RepoInitTracking::default());

    // Now, go through and convert all the tree objects.
    // Blob conversion.

    // Set up the pointer file translator.
    let pft = Arc::new(
        PointerFileTranslatorV2::from_config(&xet_repo.xet_config, xet_repo.repo_salt().await?)
            .await?,
    );

    // Now, run the bulk of the blob processing in parallel as
    let blob_processing_permits = Arc::new(Semaphore::new(MAX_CONCURRENT_BLOB_PROCESSING));
    let mut blob_processing_pool = JoinSet::<Result<(Oid, Vec<u8>)>>::new();

    // Now, with everything there, first go through and convert all the blobs.
    for b_oid in blobs {
        {
            // Add this to the filtering pool.
            let blob_processing_permits = blob_processing_permits.clone();
            let progress_reporting = progress_reporting.clone();
            let src = src.clone();
            let pft = pft.clone();
            let src_repo = src.path().to_path_buf();
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
        }
        while let Some(res) = blob_processing_pool.try_join_next() {
            let (b_oid, new_data) = res??;
            let new_id = dest.blob(&new_data[..])?;

            tr_print!("BlobTR: {b_oid} -> {new_id}");
            tr_table.push((b_oid, new_id));
            progress_reporting.register_progress(Some(1), None);
        }
    }

    // Now, clear out the rest.
    while let Some(res) = blob_processing_pool.join_next().await {
        let (b_oid, new_data) = res??;
        let new_id = dest.blob(&new_data[..])?;

        tr_print!("BlobTR: {b_oid} -> {new_id}");
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
    // Open the source repo
    let src_repo = src_repo.as_ref().to_path_buf();
    let src = Arc::new(git2::Repository::discover(&src_repo)?);

    // Get the dest repo
    let dest = xet_repo.repo.clone();

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

    let progress_reporting = DataProgressReporter::new("XET: Importing Objects", Some(0), None);

    if ENABLE_TRANSLATION_TRACING {
        progress_reporting.set_active(false);
    }

    let mut full_tr_map = HashMap::<Oid, Oid>::new();

    // Updating the logic.
    for converting_notes in [false, true] {
        let mut seed_oids = HashSet::new();

        tr_print!("+++++++++++++++++++++++++++++++++++++++");
        tr_print!("Converting Notes: {converting_notes}.");

        // Build up the starting points from the given refenences.
        for maybe_reference in src.references()? {
            let Ok(reference) = maybe_reference.map_err(|e| {
                tr_warn!("Error loading reference {e:?}, skipping.");
                e
            }) else {
                continue;
            };

            let reference_name = String::from_utf8_lossy(reference.name_bytes());

            tr_print!("Considering reference {reference_name}.");

            // Check if the reference is the correct type
            if reference_name.starts_with("refs/notes/") != converting_notes {
                tr_print!("Note mode wrong, rejecting.");
                continue;
            }

            // Skip importing of xet notes; we assume that we're rebuilding things.
            if reference.is_note() && reference_name.starts_with("refs/notes/xet/") {
                tr_print!("Xet Note, rejecting.");
                continue;
            }

            // Only convert local references
            if reference.is_remote() {
                tr_print!("Xet Note, rejecting.");
                continue;
            }

            if let Some(oid) = reference.target() {
                tr_print!("Importing reference {reference_name} with oid {oid}");
                seed_oids.insert(oid);
            }
        }

        // If it's not converting notes, also add in the tags.
        if !converting_notes {
            src.tag_foreach(|oid, _| seed_oids.insert(oid))?;
        }

        //////////////////////////////////////////////////////////////////////////////////////////
        //
        //  Step 1: build the dependency graph.  We track what upstream objects must be converted before
        //  the object itself must be converted.
        //
        //  In addition, track a list of root oids that are referenced by commits
        //  (except for note commits).

        // After conversion, these dependent oids are closer to being ready.
        let mut downstream_oids = HashMap::<Oid, Vec<Oid>>::new();

        // The number of dependencies that have to be ready before we can convert this one.
        let mut dependency_count = HashMap::<Oid, usize>::new();
        let mut blobs = HashSet::new();
        let mut root_tree_oids = HashSet::new();

        // Start us off with the seed oids from above.
        let mut proc_queue = Vec::from_iter(seed_oids.into_iter());

        while let Some(oid) = proc_queue.pop() {
            if dependency_count.contains_key(&oid) {
                continue;
            }
            // Make sure this entry has a spot, even if there are no dependents.
            downstream_oids.entry(oid).or_default();

            progress_reporting.update_target(Some(1), None);

            let Ok(obj) = src.find_object(oid, None).map_err(|e| {
                tr_warn!(
                    "Referenced Oid {oid} not found in src database, passing Oid through as is."
                );
                e
            }) else {
                dependency_count.insert(oid, 0);
                continue;
            };

            let dependents = match obj.kind().unwrap_or(ObjectType::Any) {
                ObjectType::Tree => {
                    if converting_notes {
                        get_note_tree_dependents(&src, obj, &full_tr_map)
                    } else {
                        get_nonnote_tree_dependents(obj)
                    }
                }
                ObjectType::Commit => {
                    let (dependents, tree_oid) = get_commit_dependents(&src, obj);
                    if !converting_notes {
                        // Commits reference the root trees.
                        root_tree_oids.insert(tree_oid);
                    }
                    dependents
                }

                ObjectType::Blob => {
                    blobs.insert(oid);
                    vec![]
                }
                ObjectType::Tag => get_tag_dependents(obj),
                _ => {
                    tr_warn!("Oid {oid} not blob, commit, or tree, passing through.");
                    vec![]
                }
            };

            dependency_count.insert(oid, dependents.len());
            for d_oid in dependents {
                downstream_oids.entry(d_oid).or_default().push(oid);
                proc_queue.push(d_oid);
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
                port_blobs_directly(
                    &src,
                    &dest,
                    blobs.into_iter().collect(),
                    progress_reporting.clone(),
                )?
            } else {
                convert_all_blobs_with_import(
                    src.clone(),
                    &xet_repo,
                    blobs.into_iter().collect(),
                    progress_reporting.clone(),
                )
                .await?
            }
        };

        let mut op_tr_map = HashMap::with_capacity(dependency_count.len());
        full_tr_map.reserve(dependency_count.len());

        for (src_oid, dest_oid) in blob_tr_table {
            full_tr_map.insert(src_oid, dest_oid);
            op_tr_map.insert(src_oid, dest_oid);
        }

        //////////////////////////////////////////////////////////////////////////////////////////
        //
        //  Step 3: Non-blob conversion.
        //
        //  Using the dependency graph, we can run through things in order.  Due to the nature of git's
        //  Oids, we are gauranteed to not have any cycles.
        //

        // Track the processing queue here.
        let mut processing_queue = vec![];

        for (&k, &dep_count) in dependency_count.iter() {
            if dep_count == 0 {
                processing_queue.push(k);
            }
        }

        // Now, run through and process everything.
        // We don't actually skip anything here based on the tr_map,
        // as it is theoretically possible that possible that a referenced commit
        // will have been  split.
        while let Some(oid) = processing_queue.pop() {
            let Ok(obj) = src.find_object(oid, None).map_err(|e| {
                tr_warn!(
                    "Referenced Oid {oid} not found in src database, passing Oid through as is."
                );
                e
            }) else {
                full_tr_map.insert(oid, oid);
                op_tr_map.insert(oid, oid);
                continue;
            };

            let new_oid = match obj.kind().unwrap_or(ObjectType::Any) {
                ObjectType::Tree => {
                    if converting_notes {
                        convert_note_tree(&src, &dest, obj, &op_tr_map, &full_tr_map)?
                    } else {
                        convert_nonnote_tree(&dest, obj, root_tree_oids.contains(&oid), &op_tr_map)?
                    }
                }
                ObjectType::Commit => convert_commit(&src, &dest, obj, &op_tr_map, &full_tr_map)?,
                ObjectType::Blob => {
                    // Blobs should already all have been converted.
                    *op_tr_map.get(&oid).unwrap_or_else(|| {
                        tr_panic!(
                            "Logic Error; blob {oid} not in translation map; passing through"
                        );
                    })
                }
                ObjectType::Tag => convert_tag(&src, &dest, obj, &full_tr_map)?,
                _ => {
                    tr_warn!("Entry {oid} has object type other than blob, commit, tag, or tree; skipping.");
                    oid
                }
            };

            op_tr_map.insert(oid, new_oid);
            full_tr_map.insert(oid, new_oid);

            // Now, go through all the oids depending on this and queue them if they are ready.
            if let Some(dependent_oids) = downstream_oids.get(&oid) {
                for &d_oid in dependent_oids {
                    if let Some(dep_remaining_count) = dependency_count.get_mut(&d_oid) {
                        debug_assert_lt!(0, *dep_remaining_count);
                        *dep_remaining_count -= 1;
                        if *dep_remaining_count == 0 {
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

            let name = String::from_utf8_lossy(reference.name_bytes());

            // Now, it's a direct branch.
            if reference.is_branch() {
                // Create a branch.

                let Some(commit_id) = reference.target() else {
                    tr_warn!("Reference {name} is without target; skipping ");
                    continue;
                };

                let Some(new_commit_id) = full_tr_map.get(&commit_id) else {
                    tr_warn!("Reference {name} has commit not in translation table, skipping.");
                    continue;
                };

                let target_commit = dest.find_commit(*new_commit_id)?;

                let branch_name = reference.shorthand().unwrap_or(&name);
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

                let Some(&new_target_oid) = full_tr_map.get(&target_oid) else {
                    tr_warn!("Reference {name} has target not in translation table, skipping.");
                    continue;
                };

                let _ = dest.reference(
                    &name,
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

                let Some(new_tag_id) = full_tr_map.get(&tag_id) else {
                    tr_warn!("Reference {name} has tag not in translation table, skipping.");
                    continue;
                };

                let _ = dest.reference(
                    &name,
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
            let name = String::from_utf8_lossy(reference.name_bytes());

            let Some(target) = reference.symbolic_target() else {
                tr_warn!("Symbolic reference {name} has no target; skipping import.");
                continue;
            };

            let _ = dest
                .reference_symbolic(&name, target, true, &format!("Imported reference {name}"))
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
