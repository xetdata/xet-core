use crate::merkledb_plumb::find_cas_nodes_for_blob;
use clap::Args;
use merkledb::prelude_v2::*;
use merkledb::*;
use merklehash::*;
use pointer_file::PointerFile;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::error;

#[derive(Args, Debug)]
pub struct RepoSizeArgs {
    /// A git commit reference to get repository size statistics.
    #[clap(default_value = "HEAD")]
    reference: String,

    /// If set, do not read the repository size statistics in git notes
    #[clap(long)]
    no_cache_read: bool,

    /// If set, only do not write the repository size statistics in git notes
    #[clap(long)]
    no_cache_write: bool,

    /// Print more detailed statistics
    #[clap(short, long)]
    detailed: bool,
}

use crate::config::XetConfig;
use crate::{
    constants::POINTER_FILE_LIMIT,
    errors::{self, GitXetRepoError},
    git_integration::{GitRepo, GitXetRepo},
};

/// Return type of `git_blob_to_blob_size`
#[derive(Default, Debug)]
pub struct BlobSize {
    /// If the blob was a pointer file
    is_pointer: bool,
    /// The smudged size of the blob. If its a pointer, this is the size
    /// marked in the pointer file. If its not a pointer, this is the raw
    /// size of the blob.
    size: u64,
}

/// Returns the effective blob size of a blob inspecting pointer files.
pub fn git_blob_to_blob_size(blob: &git2::Blob) -> anyhow::Result<BlobSize> {
    let blob_size = blob.size();
    if blob_size <= POINTER_FILE_LIMIT {
        let content = blob.content();
        if let Ok(content_str) = std::str::from_utf8(content) {
            let pointer_file = PointerFile::init_from_string(content_str, "");
            if pointer_file.is_valid() {
                // return the size encoded in the pointer file
                let size = pointer_file.filesize();
                return Ok(BlobSize {
                    is_pointer: true,
                    size,
                });
            }
        }
    }
    Ok(BlobSize {
        is_pointer: false,
        size: blob_size as u64,
    })
}

pub async fn get_repo_size_at_reference(
    config: &XetConfig,
    reference: &str,
    no_cache_read: bool,
    no_cache_write: bool,
) -> errors::Result<()> {
    let repo = GitRepo::open(Some(config.repo_path()?))?;
    let notes_ref = "refs/notes/xet/repo-size";
    let oid = repo
        .read()
        .revparse_single(reference)
        .map_err(|_| anyhow::anyhow!("Unable to resolve reference {}", reference))?
        .id();

    // if no cache_read is false, and it is in git notes for the current commit, return that
    if let (false, Ok(possible_note)) = (no_cache_read, repo.read().find_note(Some(notes_ref), oid))
    {
        let message = possible_note.message().ok_or_else(|| {
            GitXetRepoError::Other("Failed to get message from git note".to_string())
        })?;
        println!("{message}");
    } else {
        let repo_lg = repo.read();
        let commit = repo_lg.find_commit(oid)?;
        // Find the current tree
        let tree = commit.tree()?;
        let mut sum: u64 = 0;
        tree.walk(git2::TreeWalkMode::PreOrder, |_, entry| {
            if let Some(git2::ObjectType::Blob) = entry.kind() {
                if let Some(blob) = entry
                    .to_object(&repo_lg)
                    .ok()
                    .and_then(|x| x.peel_to_blob().ok())
                {
                    if let Ok(BlobSize {
                        is_pointer: _,
                        size,
                    }) = git_blob_to_blob_size(&blob)
                    {
                        sum += size;
                    }
                }
            }
            git2::TreeWalkResult::Ok
        })?;

        // cache the result in git notes
        if !no_cache_write {
            let sig = repo.read().signature()?;
            let note = sum.to_string();
            repo.write()
                .note(&sig, &sig, Some(notes_ref), oid, &note, false)?;
        }
        println!("{sum}");
    }
    Ok(())
}

#[derive(Default, Debug)]
struct CommitSizeData {
    /// The effective repo size in bytes if you smudge everything (git_stored_size + cas_stored_size)
    repo_size: u64,
    /// Sum of all the bytes stored by git (excludes pointer files)
    git_stored_size: u64,
    /// Sum of all the bytes stored as pointer files
    cas_stored_size: u64,
    /// List of all the CAS entries
    cas_entries: HashSet<MerkleHash>,
}

/// Computes the commit size data for a given tree
fn get_commit_size_data(
    repo: &Arc<GitRepo>,
    tree: &git2::Tree,
    mdb: &MerkleMemDB,
) -> Result<CommitSizeData, anyhow::Error> {
    let mut ret = CommitSizeData::default();

    let repo_lg = repo.read();

    tree.walk(git2::TreeWalkMode::PreOrder, |_, entry| {
        if let Some(git2::ObjectType::Blob) = entry.kind() {
            if let Some(blob) = entry
                .to_object(&repo_lg)
                .ok()
                .and_then(|x| x.peel_to_blob().ok())
            {
                if let Ok(BlobSize { is_pointer, size }) = git_blob_to_blob_size(&blob) {
                    ret.repo_size += size;
                    if is_pointer {
                        ret.cas_stored_size += size;
                    } else {
                        ret.git_stored_size += size;
                    }
                }
                if let Ok((casuse, _)) = find_cas_nodes_for_blob(mdb, &blob) {
                    for h in casuse.keys() {
                        ret.cas_entries.insert(*h);
                    }
                }
            }
        }
        git2::TreeWalkResult::Ok
    })?;
    Ok(ret)
}

type DetailedSize = HashMap<String, u64>;
pub fn compute_detailed_repo_size(
    repo: &Arc<GitRepo>,
    mdb: &MerkleMemDB,
    commit: &git2::Commit,
) -> anyhow::Result<DetailedSize> {
    let mut ret: DetailedSize = DetailedSize::default();
    // Find the current tree and read out the repository information for
    // the current commit
    let tree = commit.tree()?;
    let current_commit_data = get_commit_size_data(repo, &tree, mdb)?;

    ret.insert("repo_size".to_string(), current_commit_data.repo_size);
    ret.insert(
        "git_stored_size".to_string(),
        current_commit_data.git_stored_size,
    );
    ret.insert(
        "cas_stored_size".to_string(),
        current_commit_data.cas_stored_size,
    );
    // this is a merge commit. We cannot compute dedupe information
    // for merge commits. Early return.
    if commit.parents().len() > 1 {
        return Ok(ret);
    }

    // Find the first previous tree if any
    let prevtree = if commit.parents().len() == 0 {
        None
    } else {
        Some(commit.parent(0)?.tree()?)
    };
    // find the list of cas entries used by the previous tree (prevtree_nodes)
    let prev_commits_cas_entries = if let Some(tree) = &prevtree {
        let prev_commit_data = get_commit_size_data(repo, tree, mdb)?;
        prev_commit_data.cas_entries
    } else {
        HashSet::new()
    };
    let cas_diff = &current_commit_data.cas_entries - &prev_commits_cas_entries;

    let rg = repo.read();
    // Ask git for the tree diff
    let treediff = rg
        .diff_tree_to_tree(
            prevtree.as_ref(),
            Some(&tree),
            Some(
                git2::DiffOptions::new()
                    .ignore_filemode(true)
                    .ignore_submodules(true)
                    .force_binary(true), // we just want to know what files changed
            ),
        )
        .map_err(|_| anyhow::anyhow!("Unable to compute diff"))?;

    // for each changed file
    // count the number of bytes used by the new cas entries
    let mut total_modified_file_size: u64 = 0;
    for delta in treediff.deltas() {
        let delta_new_file = delta.new_file();
        let curfile = delta_new_file.id();
        if let Ok(blob) = repo.read().find_blob(curfile) {
            if let Ok(BlobSize { is_pointer, size }) = git_blob_to_blob_size(&blob) {
                if is_pointer {
                    total_modified_file_size += size;
                }
            }
        }
    }
    // calculate the total cas size
    let new_cas_bytes: u64 = cas_diff
        .iter()
        .filter_map(|x| mdb.find_node(x))
        .map(|x| x.len() as u64)
        .sum();

    ret.insert("new_cas_bytes".to_string(), new_cas_bytes);
    ret.insert(
        "total_bytes_of_modified_files_stored_in_cas".to_string(),
        total_modified_file_size,
    );
    ret.insert(
        "deduped_bytes_at_commit".to_string(),
        total_modified_file_size - new_cas_bytes,
    );

    Ok(ret)
}
pub async fn get_detailed_repo_size_at_reference(
    config: &XetConfig,
    reference: &str,
    no_cache_read: bool,
    no_cache_write: bool,
) -> errors::Result<()> {
    let xet_repo = GitXetRepo::open(config.clone())?;
    let gitrepo = xet_repo.git_repo().clone();

    let notes_ref = "refs/notes/xet/detailed-repo-size";
    let oid = gitrepo
        .read()
        .revparse_single(reference)
        .map_err(|_| anyhow::anyhow!("Unable to resolve reference {}", reference))?
        .id();

    // if no cache is false, and it is in git notes for the current commit, return that
    if let (false, Ok(possible_note)) = (
        no_cache_read,
        gitrepo.read().find_note(Some(notes_ref), oid),
    ) {
        let message = possible_note.message().ok_or_else(|| {
            GitXetRepoError::Other("Failed to get message from git note".to_string())
        })?;
        println!("{message}");
    } else {
        // we need to recompute the stats
        // load the merkledb
        let _ = xet_repo.sync_notes_to_dbs().await;
        let mdb = MerkleMemDB::open(&config.merkledb).map_err(|e| {
            error!("Unable to open {:?}: {e:?}", &config.merkledb);
            e
        })?;
        let repo_lg = gitrepo.read();
        let commit = repo_lg.find_commit(oid)?;
        let detailed_size = compute_detailed_repo_size(&gitrepo, &mdb, &commit)?;
        let content_str = serde_json::to_string_pretty(&detailed_size).map_err(|_| {
            GitXetRepoError::Other("Failed to serialize detailed repo size to JSON".to_string())
        })?;

        // cache the result in git notes
        if !no_cache_write {
            let sig = gitrepo.read().signature()?;
            gitrepo
                .write()
                .note(&sig, &sig, Some(notes_ref), oid, &content_str, true)?;
        }
        println!("{content_str}");
    }
    Ok(())
}

pub async fn repo_size_command(config: XetConfig, args: &RepoSizeArgs) -> errors::Result<()> {
    if args.detailed {
        get_detailed_repo_size_at_reference(
            &config,
            &args.reference,
            args.no_cache_read,
            args.no_cache_write,
        )
        .await
    } else {
        get_repo_size_at_reference(
            &config,
            &args.reference,
            args.no_cache_read,
            args.no_cache_write,
        )
        .await
    }
}
