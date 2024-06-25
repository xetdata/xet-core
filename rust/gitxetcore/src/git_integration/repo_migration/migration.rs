use crate::data::PointerFileTranslatorV2;
use crate::errors::Result;
use crate::git_integration::git_xet_repo::GITATTRIBUTES_CONTENT;
use crate::git_integration::GitXetRepo;
use git2::{Object, ObjectType, Oid, Repository, Signature};
use progress_reporting::DataProgressReporter;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::warn;

#[allow(unused)]
use crate::errors::GitXetRepoError;

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
const ENABLE_TRANSLATION_TRACING: bool = false;

// These macros conditionally direct the printing based on the above flag.

macro_rules! mg_trace {
    ($($arg:tt)*) => {
        if ENABLE_TRANSLATION_TRACING {
            eprintln!($($arg)*);
        }
    };
}

macro_rules! mg_warn {
    ($($arg:tt)*) => {
        if ENABLE_TRANSLATION_TRACING {
            eprintln!("WARNING: {}", format!($($arg)*));
        } else {
            warn!($($arg)*);
        }
    };
}

macro_rules! mg_fatal {
    ($($arg:tt)*) => {
        if ENABLE_TRANSLATION_TRACING {
            panic!($($arg)*);
        }
        Err(GitXetRepoError::Other(format!($($arg)*)))?;
        unreachable!();
    };
}

// Tree processsing functions.
fn get_nonnote_tree_dependents(obj: Object) -> Vec<Oid> {
    let mut dependents = vec![];
    let oid = obj.id();

    let Some(tree) = obj.as_tree() else {
        mg_warn!("Tree oid {oid} not actually accessible as tree, ignoring.");
        return vec![];
    };

    mg_trace!("Dependences of Tree {oid}:");

    for entry in tree.iter() {
        mg_trace!(" -> Dep: {}", entry.id());
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
        mg_warn!("Tree {oid} not actually a tree in source repo, passing through.");
        return Ok(oid);
    };
    mg_trace!("Converting tree {oid}:");

    let mut tree_builder = dest.treebuilder(None)?;

    for entry in tree.iter() {
        let src_entry_oid = entry.id();

        let Some(&dest_entry_oid) = entry_tr_map.get(&src_entry_oid) else {
            mg_fatal!("Logic error: ignoring {src_entry_oid} in tree {oid}.");
        };

        if is_base_dir_tree {
            if let Some(".gitattributes") = entry.name() {
                continue;
            }
        }

        // Sometimes this fails...
        'tree_builder_done: {
            let mut filemode = entry.filemode_raw();
            if let Err(e) = tree_builder.insert(entry.name_bytes(), dest_entry_oid, filemode) {
                if format!("{e}").contains("invalid filemode") {
                    mg_warn!(
                        "Warning: correcting invalid filemode on {oid}:{} to 0o100644",
                        entry.name().unwrap_or("NON-UTF8")
                    );
                    filemode = 0o100644;
                } else {
                    break 'tree_builder_done;
                }
            } else {
                break 'tree_builder_done;
            }

            tree_builder.insert(entry.name_bytes(), dest_entry_oid, filemode)?;
        }

        mg_trace!(
            " -> Entry {}: {} -> {}",
            entry.name().unwrap_or("NON UTF8"),
            src_entry_oid,
            dest_entry_oid
        );
    }

    if is_base_dir_tree {
        let gitattributes_oid = dest.blob(GITATTRIBUTES_CONTENT.as_bytes())?;

        mg_trace!("  Base dir tree; Adding .gitattributes with {gitattributes_oid}");

        // Add in the .gitattributes entry explicitly, as this is a root commit.
        tree_builder.insert(".gitattributes", gitattributes_oid, 0o100644)?;
    }

    let new_oid = tree_builder.write()?;
    mg_trace!("Converted Tree: {} -> {}", oid, new_oid);
    Ok(new_oid)
}

/// Extracts the name oids from a note tree entry path.
fn extract_name_oid(t_oid: Oid, entry: &git2::TreeEntry) -> Option<Oid> {
    let hex_oid = entry.name().or_else(|| {
        mg_warn!(
            "UTF-8 Error unpacking path of entry {} on tree {t_oid}",
            entry.id()
        );
        None
    })?;

    if hex_oid.len() != 40 {
        return None;
    }

    let Ok(path_oid) = Oid::from_str(hex_oid) else {
        return None;
    };
    Some(path_oid)
}

/// Getting all the dependent OIDs in a tree that is used for notes.
fn get_note_tree_dependents(
    src: &Repository,
    obj: Object,
    full_tr_map: &HashMap<Oid, Oid>,
) -> (Vec<Oid>, Vec<Oid>) {
    let mut nonnote_dependents = vec![];
    let mut note_dependents = vec![];

    let oid = obj.id();

    let Some(tree) = obj.as_tree() else {
        mg_warn!("Tree Oid {oid} not actually accessible as tree, ignoring.");
        return (vec![], vec![]);
    };
    mg_trace!("Dependences of Note Tree {oid}:");

    for entry in tree.iter() {
        mg_trace!(" -> Dep: {}", entry.id());
        note_dependents.push(entry.id());

        // Possibly the names are oids (in the case of notes), and possibly
        // these are attached to note objects.  Convert these correctly.
        if let Some(attached_oid) = extract_name_oid(oid, &entry) {
            if !full_tr_map.contains_key(&attached_oid) {
                if src.find_object(attached_oid, None).is_ok() {
                    mg_trace!(" -> Path Dep: {attached_oid}, unknown but valid target.");
                    nonnote_dependents.push(attached_oid);
                } else {
                    mg_trace!(" -> Path Dep: {attached_oid} rejected, target not in source.");
                }
            } else {
                mg_trace!(" -> Path Dep: {attached_oid} rejected, target already converted.");
            }
        } else {
            mg_warn!(
                " -> Path Dep: '{}' rejected, not OID.",
                entry.name().unwrap_or("NON UTF8")
            );
        }
    }
    (nonnote_dependents, note_dependents)
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
        mg_warn!("Tree {oid} not actually a tree in source repo, passing through.");
        return Ok(oid);
    };

    mg_trace!("Converting Note Tree: {oid}");

    let mut tree_builder = dest.treebuilder(None)?;

    for entry in tree.iter() {
        let src_entry_oid = entry.id();

        let Some(&dest_entry_oid) = entry_tr_map.get(&src_entry_oid) else {
            mg_fatal!("Logic error: ignoring {src_entry_oid} in tree {oid}.");
        };

        if let Some(src_attached_oid) = extract_name_oid(oid, &entry) {
            if let Some(dest_attached_oid) = full_tr_map.get(&src_attached_oid) {
                tree_builder.insert(
                    dest_attached_oid.to_string().as_bytes(),
                    dest_entry_oid,
                    entry.filemode_raw(),
                )?;
                mg_trace!(" -> Entry: {src_entry_oid} -> {dest_entry_oid}");
                mg_trace!("    -> Attached: {src_attached_oid} -> {dest_attached_oid}");
            } else {
                if src.find_object(src_attached_oid, None).is_ok() {
                    mg_fatal!("Logic error: ignoring {src_attached_oid} in tree {oid}.");
                }
                tree_builder.insert(entry.name_bytes(), dest_entry_oid, entry.filemode_raw())?;

                mg_trace!(" -> Entry: {src_entry_oid} -> {dest_entry_oid}, attached OID {src_attached_oid} not in source repo, passing through.");
            }
        } else {
            tree_builder.insert(entry.name_bytes(), dest_entry_oid, entry.filemode_raw())?;
            mg_trace!(
                " -> Entry: {} -> {}, no attached ID.",
                src_entry_oid,
                dest_entry_oid
            );
        }
    }

    let new_oid = tree_builder.write()?;
    mg_trace!("Converted Note Tree: {oid} -> {new_oid}");
    Ok(new_oid)
}

/// Get all the dependents of a commit.
fn get_commit_dependents(
    src: &Repository,
    obj: Object,
    disable_commit_message_tracking: bool,
) -> (Vec<Oid>, Oid) {
    let oid = obj.id();
    let mut dependents = vec![];
    let Some(commit) = obj.as_commit() else {
        mg_warn!("Commit Oid {oid} not actually accessible as commit, ignoring.");
        return (vec![], Oid::zero());
    };

    mg_trace!(
        "Dependencies of Commit {oid}, \"{}\": ",
        commit.summary().unwrap_or("NOT UTF8")
    );

    // The tree_id is a dependent.
    dependents.push(commit.tree_id());
    mg_trace!(" -> Tree: {}", commit.tree_id());

    // All parents are dependents of course.
    for parent in commit.parents() {
        mg_trace!(" -> Parent: {}", parent.id());
        dependents.push(parent.id());
    }

    // Commit messages (e.g. merges) may reference other commits as Oids.
    if !disable_commit_message_tracking {
        if let Some(msg) = commit.message() {
            for named_oid in extract_str_oids(src, msg) {
                mg_trace!(" -> Named: {named_oid}");
                dependents.push(named_oid);
            }
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
        mg_warn!("Commit Oid {oid} not actually accessible as commit, ignoring.");
        return Ok(oid);
    };

    mg_trace!(
        "Converting Commit {oid}: {}",
        src_commit.summary().unwrap_or("NOT UTF8")
    );

    let src_tree_oid = src_commit.tree_id();

    // All the referenced commits here should be to locally translated objects tracked by
    // the above dependencies.
    let Some(&new_tree_id) = tr_map.get(&src_tree_oid) else {
        mg_fatal!("Logic error; passing {src_tree_oid} through in commit {oid}.");
    };

    let mut new_parents = Vec::with_capacity(src_commit.parent_count());

    for parent in src_commit.parents() {
        let src_parent_oid = parent.id();

        let Some(&new_parent_id) = tr_map.get(&src_parent_oid) else {
            mg_fatal!("Logic error; ignoring parent {src_parent_oid} in commit {oid}.");
        };

        let Ok(new_commit) = dest.find_commit(new_parent_id) else {
            mg_fatal!(
                "Converted parent of commit {oid}: {src_parent_oid} -> {new_parent_id} not in dest as commit."
            );
        };
        mg_trace!(" -> Converting parent: {src_parent_oid} -> {new_parent_id}");

        new_parents.push(new_commit);
    }

    let new_commit_msg = {
        if let Some(msg) = src_commit.message() {
            replace_oids(src, msg, msg_tr_map)
        } else {
            unsafe { std::str::from_utf8_unchecked(src_commit.message_raw_bytes()).to_owned() }
        }
    };

    let Ok(new_tree) = dest.find_tree(new_tree_id) else {
        mg_fatal!(
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

    mg_trace!("Commit converted: {oid} -> {new_commit_id}");

    Ok(new_commit_id)
}

/// Gets the dependent of a tag.
fn get_tag_dependents(obj: Object) -> Vec<Oid> {
    let oid = obj.id();
    let Some(tag) = obj.as_tag() else {
        mg_warn!("Tag {oid} not actually accessible as tag, ignoring.",);
        return vec![];
    };

    mg_trace!("Tag dependent: {oid} -> {}", tag.target_id());

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
        mg_warn!("Tag Oid {oid} not actually a tag, skipping.");
        return Ok(oid);
    };

    let old_target_id = tag.target_id();
    let Some(&new_target_id) = tr_map.get(&old_target_id) else {
        mg_fatal!("Logic Error; target_id {old_target_id} untranslated, skipping");
    };

    let new_target_obj = match dest.find_object(new_target_id, None) {
        Ok(t_obj) => t_obj,
        Err(e) => {
            if src.find_object(old_target_id, None).is_ok() {
                mg_fatal!(
                    "Logic Error; target_id {old_target_id} untranslated to dest object: {e}"
                );
            } else {
                mg_warn!(
                "Tag target {old_target_id} for tag {oid} was not valid, skipping import. ({e})"
            );
                return Ok(oid);
            }
        }
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

async fn convert_blobs(
    src: Arc<Repository>,
    xet_repo: &GitXetRepo,
    blobs: Vec<(Oid, bool)>,
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
    for (b_oid, enable_filtering) in blobs {
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

                let new_data = {
                    if enable_filtering {
                        translate_blob_contents(
                            &src_repo,
                            pft.clone(),
                            b_oid,
                            progress_reporting,
                            src_data,
                            repo_init_tracking,
                        )
                        .await?
                    } else {
                        src_data
                    }
                };
                Ok((b_oid, new_data))
            });
        }
        while let Some(res) = blob_processing_pool.try_join_next() {
            let (b_oid, new_data) = res??;
            let new_id = dest.blob(&new_data[..])?;

            mg_trace!("BlobTR: {b_oid} -> {new_id}");
            tr_table.push((b_oid, new_id));
            progress_reporting.register_progress(Some(1), None);
        }
    }

    // Now, clear out the rest.
    while let Some(res) = blob_processing_pool.join_next().await {
        let (b_oid, new_data) = res??;
        let new_id = dest.blob(&new_data[..])?;

        mg_trace!("BlobTR: {b_oid} -> {new_id}");
        tr_table.push((b_oid, new_id));
        progress_reporting.register_progress(Some(1), None);
    }

    pft.finalize().await?;

    Ok(tr_table)
}

pub fn get_oids_by_note_refs(src: &Repository) -> Result<HashSet<Oid>> {
    let mut seed_oids = HashSet::new();

    // Build up the starting points from the given refenences.
    for maybe_reference in src.references()? {
        let Ok(reference) = maybe_reference.map_err(|e| {
            mg_warn!("Error loading reference {e:?}, skipping.");
            e
        }) else {
            continue;
        };

        let name = String::from_utf8_lossy(reference.name_bytes());

        mg_trace!("Considering reference {name}.");

        // Only convert local references

        if reference.is_note() {
            if name.contains("/notes/xet") {
                mg_trace!("  -> Xet Note, rejecting.");
                continue;
            }
            if name.contains("xet") {
                mg_warn!("Note {name} still contains xet keyword...");
                continue;
            }

            let Some(target_oid) = reference.target() else {
                mg_warn!("Note reference {name} is without target; skipping ");
                continue;
            };

            mg_trace!("  -> Reference is Note; OID = {target_oid}");

            seed_oids.insert(target_oid);
        }
    }

    Ok(seed_oids)
}

pub fn get_oids_by_nonnote_refs(src: &Repository) -> Result<HashSet<Oid>> {
    let mut seed_oids = HashSet::new();

    // Build up the starting points from the given refenences.
    for maybe_reference in src.references()? {
        let Ok(reference) = maybe_reference.map_err(|e| {
            mg_warn!("Error loading reference {e:?}, skipping.");
            e
        }) else {
            continue;
        };

        let name = String::from_utf8_lossy(reference.name_bytes());

        mg_trace!("Considering reference {name}.");

        // Only convert local references

        if reference.is_note() {
            mg_trace!("  -> Note a note; rejecting.");
            continue;
        }

        if reference.is_branch() {
            let Some(target_oid) = reference.target() else {
                mg_warn!("Branch reference {name} is without target; skipping ");
                continue;
            };

            mg_trace!("  -> Reference is branch; OID = {target_oid}");

            seed_oids.insert(target_oid);
        } else if reference.is_remote() {
            mg_trace!("  -> Reference is to a remote; rejecting.");
            continue;
        } else if reference.is_tag() {
            let Some(tag_id) = reference.target() else {
                mg_warn!("Tag reference {name} is without target; skipping ");
                continue;
            };

            mg_trace!("  -> Reference is tag; OID = {tag_id}");
            seed_oids.insert(tag_id);
        } else {
            mg_trace!(
                "  -> Reference {name} not note, branch, remote, or tag; checking for target present."
            );

            let Some(target_oid) = reference.target() else {
                mg_warn!("Reference {name} is without target; skipping ");
                continue;
            };
            mg_trace!("  -> Reference has target; OID = {target_oid}");
            seed_oids.insert(target_oid);
        }
    }

    Ok(seed_oids)
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

    eprintln!("Xet Migrate: Scanning repository structure...");

    let progress_reporting =
        DataProgressReporter::new("Xet Migrate: Importing Objects", Some(0), None);

    if ENABLE_TRANSLATION_TRACING {
        progress_reporting.set_active(false);
    }

    let mut full_tr_map = HashMap::<Oid, Oid>::new();

    // We try to convert the commit message hashes in things like commit messages, as this
    // will track merges etc. properly.  However, these sometimes use smaller hashes, which
    // have a possibility of conflict.  When this happens, we detect a cycle and restart.
    //

    // Updating the logic.
    for (
        // Are we in the note conversion stage?  If so, then only choose note references at the stop
        in_note_conversion_stage,
        // Commit messages end up
        disable_commit_message_dep_tracking,
        // Note blobs should not get filtered, so in theory a single source blob
        reprocess_previous_oids,
    ) in [
        (false, false, false),
        (false, true, false),
        (true, true, true),
    ] {
        let seed_oids = {
            if in_note_conversion_stage {
                get_oids_by_note_refs(&src)?
            } else {
                get_oids_by_nonnote_refs(&src)?
            }
        };

        let seed_oids: HashSet<Oid> = seed_oids
            .into_iter()
            .filter(|oid| !full_tr_map.contains_key(oid))
            .collect();

        mg_trace!("+++++++++++++++++++++++++++++++++++++++");
        mg_trace!("Converting Notes: {in_note_conversion_stage}.");

        //////////////////////////////////////////////////////////////////////////////////////////
        //
        //  Step 1: build the dependency graph.  We track what upstream objects must be converted before
        //  the object itself must be converted.
        //
        //  In addition, track a list of root oids that are referenced by commits
        //  (except for note commits).

        // Processing downstream -- oids that have to be converted first are more upstream than the others.

        // The oids that may be ready once this one is ready.
        let mut dependencies = HashMap::<Oid, Vec<Oid>>::new();

        let mut blobs = HashSet::new();
        let mut note_blobs = HashSet::new();
        let mut commit_tree_oids = HashSet::new();
        let mut note_oids = HashSet::new();

        // Start us off with the seed oids from above.
        let mut proc_queue = Vec::from_iter(
            seed_oids
                .into_iter()
                .map(|oid| (oid, in_note_conversion_stage)),
        );

        mg_trace!(
            "Initial dependency tracking queue for notes mode {in_note_conversion_stage} has {} entries.",
            proc_queue.len()
        );

        while let Some((oid, is_note_oid)) = proc_queue.pop() {
            if dependencies.contains_key(&oid) {
                continue;
            }

            if !reprocess_previous_oids && full_tr_map.contains_key(&oid) {
                continue;
            }

            if is_note_oid {
                note_oids.insert(oid);
            }

            mg_trace!("Considering dependents of OID {oid}:");

            let (nonnote_dependents, note_dependents) = 'have_deps: {
                let Ok(obj) = src.find_object(oid, None).map_err(|e| {
                    mg_warn!(
                    "Referenced Oid {oid} not found in src database, passing Oid through as is."
                );
                    e
                }) else {
                    break 'have_deps (vec![], vec![]);
                };

                match obj.kind().unwrap_or(ObjectType::Any) {
                    ObjectType::Tree => {
                        if in_note_conversion_stage && is_note_oid {
                            get_note_tree_dependents(&src, obj, &full_tr_map)
                        } else {
                            (get_nonnote_tree_dependents(obj), vec![])
                        }
                    }
                    ObjectType::Commit => {
                        let (dependents, tree_oid) =
                            get_commit_dependents(&src, obj, disable_commit_message_dep_tracking);
                        commit_tree_oids.insert(tree_oid);

                        // Note trees have to be directly referenced from a note commit, but other commits
                        // may be downstream of these notes.
                        // Propegate referenced commits and other things that are known to not be
                        // note trees.
                        if is_note_oid {
                            (vec![], dependents)
                        } else {
                            (dependents, vec![])
                        }
                    }

                    ObjectType::Blob => {
                        if is_note_oid {
                            note_blobs.insert(oid);
                        } else {
                            blobs.insert(oid);
                        }
                        (vec![], vec![])
                    }
                    ObjectType::Tag => (get_tag_dependents(obj), vec![]),
                    _ => {
                        mg_warn!("Oid {oid} not blob, commit, or tree, passing through.");
                        (vec![], vec![])
                    }
                }
            };

            for &d_oid in nonnote_dependents.iter() {
                proc_queue.push((d_oid, false));
            }

            for &d_oid in note_dependents.iter() {
                proc_queue.push((d_oid, true));
            }

            dependencies.insert(
                oid,
                nonnote_dependents
                    .into_iter()
                    .chain(note_dependents.into_iter())
                    .collect(),
            );
            progress_reporting.update_target(Some(1), None);
        }

        // First, seed the processing queue for part two below, then

        //////////////////////////////////////////////////////////////////////////////////////////
        //
        //  Step 2: Blob conversion.
        //
        //  All the blobs are imported at once as they should be done in parallel if there is imported
        //  data to retrieve.

        // Now, if needed, translate all the blobs.

        let mut op_tr_map = HashMap::with_capacity(dependencies.len());
        full_tr_map.reserve(dependencies.len());

        let blob_tr_table = convert_blobs(
            src.clone(),
            xet_repo,
            blobs
                .into_iter()
                .map(|oid| (oid, true))
                .chain(note_blobs.into_iter().map(|oid| (oid, false)))
                .collect(),
            progress_reporting.clone(),
        )
        .await?;

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

        // Now, from the dependency table, reverse the graph so we know which ones have to be processed.
        let mut downstream_oids = HashMap::<Oid, HashSet<Oid>>::new();
        let mut unprocessed_dependencies = HashMap::<Oid, HashSet<Oid>>::new();

        for (oid, dep_oids) in dependencies {
            let mut unprocessed_dependents = HashSet::new();
            for &d_oid in dep_oids.iter() {
                if !op_tr_map.contains_key(&d_oid) {
                    downstream_oids.entry(d_oid).or_default().insert(oid);
                    unprocessed_dependents.insert(d_oid);
                }
            }
            // No need to add the blobs to the queue.
            if op_tr_map.contains_key(&oid) {
                if !unprocessed_dependents.is_empty() {
                    mg_fatal!("Registered Blob {oid} has dependents.");
                }
            } else {
                unprocessed_dependencies.insert(oid, unprocessed_dependents);
            }
        }

        // Track the processing queue here.
        let mut processing_queue = vec![];

        for (&oid, unprocessed_deps) in unprocessed_dependencies.iter() {
            let remaining_dep_count = unprocessed_deps.len();
            if remaining_dep_count == 0 {
                mg_trace!(
                    "{oid} has {remaining_dep_count} upstream OIDs; adding to processing queue."
                );
                processing_queue.push(oid);
            } else {
                mg_trace!("{oid} has {remaining_dep_count} upstream OIDs.");
            }
        }

        mg_trace!(
            "Initial processing queue for notes mode {in_note_conversion_stage} has {} entries.",
            processing_queue.len()
        );

        // Now, run through and process everything.
        // We don't actually skip anything here based on the tr_map,
        // as it is theoretically possible that possible that a referenced commit
        // will have been  split.
        while let Some(oid) = processing_queue.pop() {
            // Only convert the ones that have not been converted.
            if op_tr_map.contains_key(&oid) {
                if ENABLE_TRANSLATION_TRACING {
                    if let Some(local_downstream_oids) = downstream_oids.get(&oid) {
                        for d_oid in local_downstream_oids {
                            let Some(unproc_dependents) = unprocessed_dependencies.get(d_oid)
                            else {
                                mg_fatal!("Downstream Oid {d_oid} has no registered upstream log.");
                            };

                            if unproc_dependents.contains(&oid) {
                                mg_fatal!("Processed oid {oid} not removed from upstream tracking of {d_oid}.");
                            }
                        }
                    }
                }
                mg_trace!("OID {oid} already translated.");
                continue;
            }

            if !reprocess_previous_oids && full_tr_map.contains_key(&oid) {
                continue;
            }

            mg_trace!("Converting {oid}.");

            let new_oid = 'get_obj_oid: {
                let Ok(obj) = src.find_object(oid, None).map_err(|e| {
                    mg_warn!(
                        "Referenced Oid {oid} not found in src database, passing Oid through as is."
                    );
                    e
                }) else {
                    break 'get_obj_oid oid;
                };

                match obj.kind().unwrap_or(ObjectType::Any) {
                    ObjectType::Tree => {
                        if in_note_conversion_stage && note_oids.contains(&oid) {
                            convert_note_tree(&src, &dest, obj, &op_tr_map, &full_tr_map)?
                        } else {
                            convert_nonnote_tree(
                                &dest,
                                obj,
                                commit_tree_oids.contains(&oid),
                                &op_tr_map,
                            )?
                        }
                    }
                    ObjectType::Commit => {
                        convert_commit(&src, &dest, obj, &op_tr_map, &full_tr_map)?
                    }
                    ObjectType::Blob => {
                        // Blobs should already all have been converted.
                        mg_fatal!(
                            "Logic Error; blob {oid} not in translation map; passing through"
                        );
                    }
                    ObjectType::Tag => convert_tag(&src, &dest, obj, &full_tr_map)?,
                    _ => {
                        mg_warn!("Entry {oid} has object type other than blob, commit, tag, or tree; skipping.");
                        oid
                    }
                }
            };

            op_tr_map.insert(oid, new_oid);
            full_tr_map.insert(oid, new_oid);

            // Now, register the progress with all the downstream oids
            if let Some(local_downstream_oids) = downstream_oids.get(&oid) {
                for &d_oid in local_downstream_oids {
                    let Some(local_upstream_oids) = unprocessed_dependencies.get_mut(&d_oid) else {
                        mg_fatal!("Logic error: upstream oid in list not found.");
                    };

                    local_upstream_oids.remove(&oid);

                    if local_upstream_oids.is_empty() {
                        processing_queue.push(d_oid);
                    }
                }
            }

            progress_reporting.register_progress(Some(1), None);
        }

        if ENABLE_TRANSLATION_TRACING {
            let mut bad_oids = 0;
            for (oid, local_upstream_oids) in unprocessed_dependencies.iter() {
                if !local_upstream_oids.is_empty() {
                    mg_trace!(
                        "OID {oid} has {} unprocessed upstream oids",
                        local_upstream_oids.len()
                    );
                    for &u_oid in local_upstream_oids {
                        if let Ok(obj) = src.find_object(u_oid, None) {
                            mg_trace!(
                                "  -> {u_oid}: {obj:?} (in tr map? {}/{})",
                                op_tr_map.contains_key(&u_oid),
                                full_tr_map.contains_key(&u_oid)
                            );
                        } else {
                            mg_trace!("  -> {u_oid} (not known)");
                        }
                    }
                    bad_oids += 1;
                }
            }
            if bad_oids != 0 {
                let (root_oids, has_cycle) =
                    find_roots_and_detect_cycles(&unprocessed_dependencies);

                mg_trace!("----> Root OIDs: {}", root_oids.len());

                let mut downstream_set = HashSet::new();

                for u_oid in root_oids {
                    if let Some(downstream) = downstream_oids.get(&u_oid) {
                        for &d_oid in downstream {
                            downstream_set.insert(d_oid);
                        }
                    }

                    if let Ok(obj) = src.find_object(u_oid, None) {
                        mg_trace!(
                            "  -> {u_oid}: {obj:?} (in tr map? {}/{})",
                            op_tr_map.contains_key(&u_oid),
                            full_tr_map.contains_key(&u_oid)
                        );
                    } else {
                        mg_trace!(
                            "  -> {u_oid}: not known, (in tr map? {}/{})",
                            op_tr_map.contains_key(&u_oid),
                            full_tr_map.contains_key(&u_oid)
                        );
                    }
                }

                mg_trace!(
                    "----> Downstream set of root OIDs: {}",
                    downstream_set.len()
                );

                for u_oid in downstream_set {
                    if let Ok(obj) = src.find_object(u_oid, None) {
                        mg_trace!(
                            "  -> {u_oid}: {obj:?} (in tr map? {}/{})",
                            op_tr_map.contains_key(&u_oid),
                            full_tr_map.contains_key(&u_oid)
                        );
                    } else {
                        mg_trace!(
                            "  -> {u_oid}: not known, (in tr map? {}/{})",
                            op_tr_map.contains_key(&u_oid),
                            full_tr_map.contains_key(&u_oid)
                        );
                    }
                }

                mg_trace!("----> Cycle: {has_cycle:?}");

                mg_fatal!("Sage has has {bad_oids} unprocessed entries.");
            }
        }
    }

    progress_reporting.finalize();

    eprintln!("Xet Migrate: Setting up references.");

    //////////////////////////////////////////////////////////////////////////////////////////
    //
    //  Step 4: Add in all the references.  Their target oids should all have been properly converted already.
    //
    //  Using the dependency graph, we can run through things in order.  Due to the nature of git's
    //  Oids, we are gauranteed to not have any cycles.

    let mut ref_list: Vec<String> = Vec::new();

    // Convert all the references.  Ignore any in xet (as this imports things in a new way).
    {
        // Add some logic to update HEAD at the end to one of these.
        let mut importing_master = false;

        // Later symbolic branches to put in
        let mut symbolic_refs = vec![];

        for maybe_reference in src.references()? {
            let Ok(reference) = maybe_reference.map_err(|e| {
                mg_warn!("Error loading reference {e:?}, skipping.");
                e
            }) else {
                continue;
            };

            // Delay the symbolic references.
            if matches!(reference.kind(), Some(git2::ReferenceType::Symbolic)) {
                symbolic_refs.push(reference);
                continue;
            }

            let name: String = String::from_utf8_lossy(reference.name_bytes()).into();

            if reference.is_remote() {
                mg_trace!("Skipping import of remote reference {name}.");
                continue;
            }

            // Reject note references.
            if reference.is_note() && name.contains("/notes/xet") {
                mg_trace!("  -> Xet Note, rejecting.");
                continue;
            }

            // Exclude other types of references (e.g. pull requests).
            if reference.is_branch() || reference.is_note() || reference.is_tag() {
                // If it's the master branch, then change it to main and add refs/notes/master as
                // a symbolic reference to that.
                let new_ref_name = {
                    if name == "refs/heads/master" && src.find_reference("refs/heads/main").is_err()
                    {
                        importing_master = true;
                        "refs/heads/main"
                    } else {
                        &name
                    }
                };

                let Some(target_oid) = reference.target() else {
                    mg_warn!("Reference {name} is without target; skipping ");
                    continue;
                };

                let Some(&new_target_oid) = full_tr_map.get(&target_oid) else {
                    mg_warn!(
                    "Reference {name} has target {target_oid} not in translation table, skipping."
                );
                    continue;
                };

                // Otherwise, import everything else as a converted reference.
                let ref_added= dest.reference(
                    new_ref_name,
                    new_target_oid,
                    true,
                    &format!("Imported reference {name}"),
                ).map_err(|e|
                    {
                        mg_warn!("Error setting reference {name} to {new_target_oid:?} in destination; skipping"); 
                        e
                    }).is_ok();

                if ref_added {
                    ref_list.push(new_ref_name.to_owned());
                }
            }
        }

        // Now, if importing master, create a symbolic reference from main to master,
        if importing_master {
            // Set up a symbolic refenrence from main to master, so that main is an alias here.
            let ref_added = dest
                .reference_symbolic(
                    "refs/heads/master",
                    "refs/heads/maain",
                    true,
                    "Add symbolic reference master to point to main.",
                )
                .map_err(|e| {
                    mg_warn!(
                        "Error setting main (xethub default) to resolve to imported branch master; skipping."
                    );
                    e
                }).is_ok();

            if ref_added {
                ref_list.push("refs/heads/master".to_owned());
            }
        }

        let _ = dest.set_head("refs/heads/main").map_err(|e| {
            mg_warn!("Error setting HEAD to imported branch main: {e:?}");
            e
        });

        // Now, resolve all the symbolic references.
        for reference in symbolic_refs {
            let name = String::from_utf8_lossy(reference.name_bytes());

            let Some(target) = reference.symbolic_target() else {
                mg_warn!("Symbolic reference {name} has no target; skipping import.");
                continue;
            };

            let is_ok = dest
                .reference_symbolic(&name, target, true, &format!("Imported reference {name}"))
                .map_err(|e| {
                    mg_warn!(
                        "Error setting symbolic reference {name} to point to {target}; ignoring.",
                    );
                    e
                })
                .is_ok();

            if is_ok {
                ref_list.push(name.into());
            }
        }
    }

    Ok(ref_list)
}
