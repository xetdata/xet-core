use crate::data::{is_xet_pointer_file, PointerFileTranslatorV2};
use crate::errors::Result;
use crate::git_integration::git_xet_repo::GITATTRIBUTES_CONTENT;
use crate::git_integration::GitXetRepo;
use crate::stream::stdout_process_stream::AsyncStdoutDataIterator;
use git2::{Commit, ObjectType, Oid, Tree};
use more_asserts::assert_ge;
use progress_reporting::DataProgressReporter;
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

use super::run_git_captured;

const MAX_CONCURRENT_BLOB_PROCESSING: usize = 64;
const GIT_SMUDGE_DATA_READ_BUFFER_SIZE: usize = 64 * 1024 * 1024;

// Tracks what initialization still needs to happen.
#[derive(Default)]
struct RepoInitTracking {
    lfs_is_initialized: AtomicBool,
    git_lfs_init_lock: Mutex<()>,
}

pub async fn migrate_repo(src_repo: impl AsRef<Path>, xet_repo: &GitXetRepo) -> Result<()> {
    let repo_init_tracking = Arc::new(RepoInitTracking::default());

    // Open the source repo
    let src_repo = src_repo.as_ref().to_path_buf();
    let src = Arc::new(git2::Repository::discover(&src_repo)?);

    // Get the dest repo
    let dest = xet_repo.repo.clone();

    // Go through the object database and enumerate all the types in it.
    let mut trees_to_convert: Vec<Oid> = Vec::new();
    let mut blobs_to_convert: Vec<Oid> = Vec::new();
    let mut commits_to_convert: Vec<Oid> = Vec::new();
    let mut tags_to_convert: Vec<Oid> = Vec::new();

    let mut anytype_oids: Vec<Oid> = Vec::new();

    let mut unknown_oids: Vec<Oid> = Vec::new();

    eprintln!("Scanning repository (working directory = {src_repo:?}).");

    // Go through and put everything in the odb into the destination repo.
    let odb = src.odb()?;
    odb.foreach(|&oid| {
        let Ok((_, o_type)) = odb.read_header(oid).map_err(|e| {
            warn!("Error encountered reading info of oid {oid:?}: {e:?}");
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

    // The translation map
    let mut tr_map = HashMap::<Oid, Oid>::new();

    // Blob conversion.
    {
        let progress_reporting =
            DataProgressReporter::new("XET: Importing files", Some(blobs_to_convert.len()), None);

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
                tr_map.insert(b_oid, new_oid);
                progress_reporting.register_progress(Some(1), Some(content.len()));
            }

            while let Some(res) = blob_processing_pool.try_join_next() {
                let (b_oid, new_data) = res??;
                let new_id = dest.blob(&new_data[..])?;

                tr_map.insert(b_oid, new_id);
                progress_reporting.register_progress(Some(1), None);
            }
        }

        // Now, clear out the rest.
        while let Some(res) = blob_processing_pool.join_next().await {
            let (b_oid, new_data) = res??;
            let new_id = dest.blob(&new_data[..])?;

            tr_map.insert(b_oid, new_id);
            progress_reporting.register_progress(Some(1), None);
        }

        pft.finalize().await?;
        progress_reporting.finalize();
    }

    // Create a gitattributes blob object, if it's not there already.
    let gitattributes_oid = dest.blob(GITATTRIBUTES_CONTENT.as_bytes())?;

    // Now, go through and convert all the tree objects.
    {
        let progress_reporting = DataProgressReporter::new(
            "XET: Importing directory structures",
            Some(trees_to_convert.len()),
            None,
        );

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
            let t_idx = trees.len();

            let tree = src.find_tree(t_oid)?;

            let mut n_subtrees = 0;
            for item in tree.iter() {
                match item.kind().unwrap_or(ObjectType::Any) {
                    ObjectType::Tree => {
                        tree_parents.entry(item.id()).or_default().push(t_idx);
                        n_subtrees += 1;
                    }
                    ObjectType::Blob => {}
                    ObjectType::Commit => {
                        info!(
                            "Entry of type Commit encountered in tree {t_oid:?}, commit oid ={:?}.  Assuming submodule reference; passing OID through directly.",
                            item.id()
                        );
                    }
                    t => {
                        info!("Entry of type {t:?} encountered in tree {t_oid:?}, ignoring.");
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

                // Iterate over each entry in the source Tree
                for entry in src_tree.iter() {
                    let entry_oid = &entry.id();

                    let new_oid = {
                        if matches!(
                            entry.kind().unwrap_or(ObjectType::Any),
                            ObjectType::Blob | ObjectType::Tree
                        ) {
                            // Get the translated Oid using the provided function
                            let Some(new_oid) = tr_map.get(entry_oid) else {
                                error!(
                                    "Repo migration logic error: {entry_oid:?} not in the translation map."
                                );
                                continue;
                            };
                            new_oid
                        } else {
                            entry_oid
                        }
                    };

                    // If the old object exists in the old repo, make sure the new object exists in the new repo.
                    #[cfg(test)]
                    {
                        if src.find_object(*entry_oid, entry.kind()).is_ok() {
                            // Make sure this entry exists in the new repo
                            assert!(dest.find_object(*new_oid, entry.kind()).is_ok());
                        }
                    }

                    if is_base_dir_commit {
                        if let Some(".gitattributes") = entry.name() {
                            continue;
                        }
                    }

                    // Add the translated Oid to the new Tree with the same name and filemode
                    tree_builder.insert(entry.name_bytes(), *new_oid, entry.filemode_raw())?;
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
            tr_map.insert(src_tree_id, new_tree_id);

            progress_reporting.register_progress(Some(1), None);
        }

        // Now, all the trees should be converted. Make sure this is the case.
        #[cfg(test)]
        {
            for (tr, c) in trees {
                assert_eq!(c, 0);
                let Some(new_id) = tr_map.get(&tr.id()) else {
                    panic!("{:?} not in tr_map.", tr.id());
                };
                assert!(dest.find_tree(*new_id).is_ok())
            }
        }
        progress_reporting.finalize();
    }

    // Now that all the trees have been converted, go through  and convert all the commits.
    // Again, this has to be done in order, so build the network of parents
    {
        let progress_reporting =
            DataProgressReporter::new("Importing commits", Some(commits_to_convert.len()), None);

        let mut commit_children = HashMap::<Oid, Vec<usize>>::new();

        let mut commits = Vec::<(Commit, usize)>::new();

        let mut processing_queue = Vec::new();

        for c_oid in commits_to_convert.iter() {
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
                    let Some(translated_oid) = tr_map.get(&parent.id()) else {
                        error!("Repo migration logic error: commit parent not translated.");
                        continue;
                    };

                    let new_commit = dest.find_commit(*translated_oid)?;

                    translated_parent_commits.push(new_commit);
                }

                let Some(new_tree_id) = tr_map.get(&src_commit.tree_id()) else {
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

            tr_map.insert(old_commit_id, new_commit_id);
            progress_reporting.register_progress(Some(1), None);
        }

        // Now, all commits should have been converted.  Make sure this is the case.
        #[cfg(test)]
        {
            for (cm, c) in commits {
                assert_eq!(c, 0);
                let Some(new_id) = tr_map.get(&cm.id()) else {
                    panic!("Logic Error: commit {:?} not in translation map.", cm.id());
                };
                assert!(dest.find_commit(*new_id).is_ok());
            }
        }
        progress_reporting.finalize();
    }

    // Convert all the tags
    {
        for tag_id in tags_to_convert {
            let tag = src.find_tag(tag_id)?;

            let Some(new_target) = tr_map.get(&tag.target_id()) else {
                warn!(
                    "Tag {:?} references OID {:?} not in new repo; skipping.",
                    tag.name().unwrap_or("NONAME"),
                    tag.target_id()
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

            tr_map.insert(tag_id, new_tag_id);
        }
    }

    // Convert all the references.  Ignore any in xet (as this imports things in a new way).
    {
        // Add some logic to update HEAD at the end to one of these.
        let mut importing_main = false;
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
                    error!("Reference {name} is without target; skipping ");
                    continue;
                };

                let Some(new_commit_id) = tr_map.get(&commit_id) else {
                    error!("Reference {name} has commit not in translation table, skipping.");
                    continue;
                };

                let target_commit = dest.find_commit(*new_commit_id)?;

                let branch_name = reference.shorthand().unwrap_or(name);
                dest.branch(branch_name, &target_commit, true)?;

                if branch_name == "main" {
                    importing_main = true;
                }
                if branch_name == "master" {
                    importing_master = true;
                }

                eprintln!("Set up branch {branch_name}");
            } else if reference.is_note() {
                warn!("Skipping import of note reference {name}.");
            } else if reference.is_remote() {
                warn!("Skipping import of remote reference {name}.");
            } else if reference.is_tag() {
                let Some(tag_id) = reference.target() else {
                    error!("Reference {name} is without target; skipping ");
                    continue;
                };

                let Some(new_tag_id) = tr_map.get(&tag_id) else {
                    error!("Reference {name} has tag not in translation table, skipping.");
                    continue;
                };

                dest.reference(
                    name,
                    *new_tag_id,
                    true,
                    &format!("Imported reference {name}"),
                )?;
            }
        }

        // Now, set head to main or master.
        if importing_main {
            let _ = dest.set_head("main").map_err(|e| {
                warn!("Error setting HEAD to imported branch main.");
                e
            });
        } else if importing_master {
            let _ = dest.set_head("master").map_err(|e| {
                warn!("Error setting HEAD to imported branch master.");
                e
            });

            // Set up a symbolic refenrence
            let _ = dest
                .reference_symbolic(
                    "main",
                    "master",
                    true,
                    "Add symbolic reference main to point to master.",
                )
                .map_err(|e| {
                    warn!(
                        "Error setting main (xethub default) to resolve to imported branch master; skipping."
                    );
                    e
                });
        }

        // Now, resolve all the symbolic references.
        for reference in symbolic_refs {
            let Some(name) = reference.name() else {
                warn!("Error getting name of reference, skipping.");
                continue;
            };

            let Some(target) = reference.symbolic_target() else {
                warn!("Symbolic reference {name} has no target; skipping import.");
                continue;
            };

            let _ = dest
                .reference_symbolic(name, target, true, &format!("Imported reference {name}"))
                .map_err(|e| {
                    warn!(
                        "Error setting symbolic reference {name} to point to {target}; ignoring."
                    );
                    e
                });
        }
    }

    Ok(())
}

/// Translate old blob contents into new blob contents.
async fn translate_blob_contents(
    src_repo_dir: &Path,
    pft: Arc<PointerFileTranslatorV2>,
    blob_oid: Oid,
    progress_reporting: Arc<DataProgressReporter>,
    src_data: Vec<u8>,
    repo_init_tracking: Arc<RepoInitTracking>,
) -> Result<Vec<u8>> {
    // Identify the process needed to pull the data out.
    let name = format!("BLOB:{blob_oid:?}");
    // Is it a git lfs pointer?  If so, then run git lfs to get the git lfs data.
    if is_git_lfs_pointer(&src_data[..]) {
        info!("Source blob ID {blob_oid:?} is git lfs pointer file; smudging through git-lfs.");
        ensure_git_lfs_is_initialized(src_repo_dir, &repo_init_tracking).await?;

        let git_lfs_reader = smudge_git_lfs_pointer(src_repo_dir, src_data.clone()).await?;
        let ret_data
        = pft.clean_file_and_report_progress(
            &PathBuf::from_str(&name).unwrap(),
            git_lfs_reader,
            &Some(progress_reporting),
        )
        .await.map_err(|e| {
            warn!("Error filtering git-lfs blob {name}: {e:?}, contents = \"{}\".  Importing as is.",
            std::str::from_utf8(&src_data[..]).unwrap_or("<Binary Data>"));
            e
        }).unwrap_or(src_data);

        Ok(ret_data)
    } else if is_xet_pointer_file(&src_data[..]) {
        info!("Source blob ID {blob_oid:?} is git xet pointer file; smudging through git-xet.");
        let git_xet_pointer = smudge_git_xet_pointer(src_repo_dir, src_data.clone()).await?;
        let ret_data = pft.clean_file_and_report_progress(
            &PathBuf::from_str(&name).unwrap(),
            git_xet_pointer,
            &Some(progress_reporting),
        )
        .await.map_err(|e| {
            warn!("Error filtering Xet pointer in {name}: {e:?}, contents = \"{}\".  Importing as is.",
            std::str::from_utf8(&src_data[..]).unwrap_or("<Binary Data>"));
            e
        }).unwrap_or(src_data);

        Ok(ret_data)
    } else {
        debug!("Cleaning blob {blob_oid:?} of size {}", src_data.len());
        // Return the filtered data
        pft.clean_file_and_report_progress(
            &PathBuf::from_str(&name).unwrap(),
            src_data,
            &Some(progress_reporting),
        )
        .await
    }
}

async fn ensure_git_lfs_is_initialized(
    src_repo_dir: &Path,
    init_locks: &Arc<RepoInitTracking>,
) -> Result<()> {
    if !init_locks
        .lfs_is_initialized
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        let _lg = init_locks.git_lfs_init_lock.lock().await;

        if init_locks
            .lfs_is_initialized
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Ok(());
        }

        info!("Running git lfs install in {src_repo_dir:?}");

        run_git_captured(Some(&src_repo_dir.to_path_buf()), "lfs", &["install"], true, None).map_err(|e| {
            error!("Error running `git lfs install` on repository with lfs pointers: {e:?}.  Please ensure git lfs is installed correctly.");
            e
        })?;

        init_locks
            .lfs_is_initialized
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    Ok(())
}

pub fn is_git_lfs_pointer(data: &[u8]) -> bool {
    if data.len() >= 1024 {
        return false;
    }

    // Convert &[u8] to a &str to parse as text.
    let Ok(text) = std::str::from_utf8(data) else {
        return false;
    };

    let text = text.trim_start();

    text.starts_with("version https://git-lfs.github.com/spec/v1") && text.contains("oid sha256:")
}

pub async fn smudge_git_lfs_pointer(
    repo_dir: impl AsRef<Path> + Debug,
    data: Vec<u8>,
) -> Result<AsyncStdoutDataIterator> {
    // Spawn a command with the necessary setup
    let mut command = tokio::process::Command::new("git");

    command.current_dir(repo_dir).arg("lfs").arg("smudge");

    AsyncStdoutDataIterator::from_command(command, &data[..], GIT_SMUDGE_DATA_READ_BUFFER_SIZE)
        .await
}

pub async fn smudge_git_xet_pointer(
    repo_dir: impl AsRef<Path> + Debug,
    data: Vec<u8>,
) -> Result<AsyncStdoutDataIterator> {
    // Spawn a command with the necessary setup
    let mut command = tokio::process::Command::new("git");

    command.current_dir(repo_dir).arg("xet").arg("smudge");

    AsyncStdoutDataIterator::from_command(command, &data[..], GIT_SMUDGE_DATA_READ_BUFFER_SIZE)
        .await
}