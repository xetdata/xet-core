use crate::errors::{GitXetRepoError, Result};
use chrono::{DateTime, TimeZone, Utc};
use git2;
use serde::Serialize;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tracing::{error, info, span, Level, Span};

// Describes a set of actions to perform on a git repo
#[derive(Debug, Clone)]
pub enum ManifestEntry {
    Create {
        file: PathBuf,
        modeexec: bool,
        content: Vec<u8>,
        githash_content: Option<git2::Oid>,
    },
    Update {
        file: PathBuf,
        content: Vec<u8>,
        githash_content: Option<git2::Oid>,
    },
    Upsert {
        file: PathBuf,
        modeexec: bool,
        content: Vec<u8>,
        githash_content: Option<git2::Oid>,
    },
    Move {
        file: PathBuf,
        prev_path: PathBuf,
        content: Option<Vec<u8>>,
    },
    Delete {
        file: PathBuf,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct AtomicCommitOutput {
    pub id: String,
    pub short_id: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub author_name: Option<String>,
    #[serde(default)]
    pub author_email: Option<String>,
    #[serde(default)]
    pub committer_name: Option<String>,
    #[serde(default)]
    pub committer_email: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    pub parent_ids: Vec<String>,
    pub committed_date: DateTime<Utc>,
}

fn _get_file_mode(tree_entry: &git2::TreeEntry) -> Result<git2::FileMode> {
    let raw_mode = tree_entry.filemode();
    if raw_mode == i32::from(git2::FileMode::BlobExecutable) {
        Ok(git2::FileMode::BlobExecutable)
    } else if raw_mode == i32::from(git2::FileMode::Blob) {
        Ok(git2::FileMode::Blob)
    } else if raw_mode == i32::from(git2::FileMode::Tree) {
        Ok(git2::FileMode::Tree)
    } else if raw_mode == i32::from(git2::FileMode::Link) {
        Ok(git2::FileMode::Link)
    } else {
        Err(GitXetRepoError::InvalidOperation(format!(
            "File has unsupported mode {raw_mode}"
        )))
    }
}

// only return Ok(Tree) or Ok(Blob) or Err
fn _validate_oid_is_tree_or_blob(
    repo: &Arc<git2::Repository>,
    file: &std::path::Path,
    oid: git2::Oid,
) -> Result<git2::ObjectType> {
    match repo.find_object(oid, None) {
        Ok(obj) => {
            // must be a tree or a blob
            if matches!(obj.kind(), Some(git2::ObjectType::Tree)) {
                Ok(git2::ObjectType::Tree)
            } else if matches!(obj.kind(), Some(git2::ObjectType::Blob)) {
                Ok(git2::ObjectType::Blob)
            } else {
                Err(GitXetRepoError::InvalidOperation(
                    "Can only copy files or directories".to_owned(),
                ))
            }
        }
        Err(_) => Err(GitXetRepoError::InvalidOperation(format!(
            "Trying to create {file:?} from non-existant Oid {oid:?}"
        ))),
    }
}
// We wait for at most 10s to acquire the lock
const MAX_RETRY_TIME_MS: u64 = 1000;

/// Performs an atomic commit of a manifest into a git repository.
/// refname can be a branch name, or can be any other ref.
///
/// If create_ref is set, the ref will be created if it does not already exist.
/// refname must start with "refs/"
#[tracing::instrument(skip_all, name = "atomic_commit", 
    fields(manifest.counts, repo_path,
           author = author, refname = refname, author_email = author_email))]
pub fn atomic_commit_impl<'a>(
    repo: &'a Arc<git2::Repository>,
    manifest_entries: Vec<ManifestEntry>,
    refname: &str,
    commit_message: &str,
    author: &str,
    author_email: &str,
    create_ref: bool,
) -> Result<(String, git2::Commit<'a>)> {
    Span::current().record("repo_path", format!("{:?}", repo.path()));

    let init_span = span!(Level::INFO, "Initialization");
    let init_span_lg = init_span.enter();

    let mut manifest_modes: HashMap<usize, git2::FileMode> = HashMap::new();
    let mut manifest_oids: HashMap<usize, git2::Oid> = HashMap::new();

    // if we find refs/heads/{refname} and it is a branch we use it.
    // Otherwise, we stick with the original refname.
    let refname = {
        if refname.starts_with("refs/") {
            refname.to_owned()
        } else {
            format!("refs/heads/{refname}")
        }
    };

    drop(init_span_lg);

    let mut m_create_count = 0usize;
    let mut m_upsert_count = 0usize;
    let mut m_move_count = 0usize;
    let mut m_update_count = 0usize;
    let mut m_delete_count = 0usize;

    {
        let verification_span = span!(Level::INFO, "Verification");
        let _lg = verification_span.enter();

        // early terminate sanity check on the ref name
        let reference = repo.find_reference(&refname).ok().or_else(|| {
            if create_ref {
                repo.head().ok()
            } else {
                None
            }
        });
        let commit = reference.and_then(|x| x.peel_to_commit().ok());
        let tree = commit.and_then(|x| x.tree().ok());
        // early terminate in case of validation errors
        for (i, manifest_entry) in manifest_entries.iter().enumerate() {
            match manifest_entry {
                ManifestEntry::Create {
                    file,
                    modeexec,
                    content: _,
                    githash_content,
                } => {
                    if tree.as_ref().and_then(|x| x.get_path(file).ok()).is_some() {
                        error!("Trying to create {file:?} which already exists.");
                        return Err(GitXetRepoError::InvalidOperation(format!(
                            "Trying to create {file:?} which already exists"
                        )));
                    }
                    if let Some(ref oid) = githash_content {
                        if _validate_oid_is_tree_or_blob(repo, file, *oid)?
                            == git2::ObjectType::Tree
                        {
                            // if this is a tree, the mode must be tree
                            // if the blob case we fall through
                            manifest_modes.insert(i, git2::FileMode::Tree);
                            continue;
                        }
                    }
                    if *modeexec {
                        manifest_modes.insert(i, git2::FileMode::BlobExecutable)
                    } else {
                        manifest_modes.insert(i, git2::FileMode::Blob)
                    };
                    m_create_count += 1;
                }
                ManifestEntry::Upsert {
                    file,
                    modeexec,
                    content: _,
                    githash_content,
                } => {
                    if let Some(ref oid) = githash_content {
                        if _validate_oid_is_tree_or_blob(repo, file, *oid)?
                            == git2::ObjectType::Tree
                        {
                            // if this is a tree, the mode must be tree
                            // if the blob case we fall through
                            manifest_modes.insert(i, git2::FileMode::Tree);
                            continue;
                        }
                    }
                    if *modeexec {
                        manifest_modes.insert(i, git2::FileMode::BlobExecutable)
                    } else {
                        manifest_modes.insert(i, git2::FileMode::Blob)
                    };
                    m_upsert_count += 1;
                }
                ManifestEntry::Delete { file } => {
                    if tree.as_ref().and_then(|x| x.get_path(file).ok()).is_none() {
                        error!("Trying to delete {file:?} which does not exist.");
                        return Err(GitXetRepoError::InvalidOperation(format!(
                            "Trying to delete {file:?} which does not exist"
                        )));
                    }
                    m_delete_count += 1;
                }
                ManifestEntry::Move {
                    file: _,
                    prev_path,
                    content: _,
                } => {
                    if let Some(ref tree) = tree {
                        match tree.get_path(prev_path) {
                            Ok(tree_entry) => {
                                manifest_modes.insert(i, _get_file_mode(&tree_entry)?);
                                let object = tree_entry.to_object(repo);
                                match object {
                                    Ok(object) => {
                                        manifest_oids.insert(i, object.id());
                                    }
                                    Err(_) => {
                                        error!("Trying to move {prev_path:?} which does not have a valid object id.");
                                        return Err(GitXetRepoError::InvalidOperation(format!(
                                    "Trying to move {prev_path:?} which does not have a valid object id",
                                )));
                                    }
                                }
                            }
                            Err(_) => {
                                error!("Trying to move {prev_path:?} which does not exist.");
                                return Err(GitXetRepoError::InvalidOperation(format!(
                                    "Trying to move {prev_path:?} which does not exist",
                                )));
                            }
                        }
                    } else {
                        return Err(GitXetRepoError::InvalidOperation(format!(
                            "Trying to move {prev_path:?} which does not exist",
                        )));
                    }
                    m_move_count += 1;
                }
                ManifestEntry::Update {
                    file,
                    content: _,
                    githash_content,
                } => {
                    if let Some(ref oid) = githash_content {
                        if _validate_oid_is_tree_or_blob(repo, file, *oid)?
                            == git2::ObjectType::Tree
                        {
                            // if this is a tree, the mode must be tree
                            // if the blob case we fall through
                            manifest_modes.insert(i, git2::FileMode::Tree);
                            continue;
                        }
                    }
                    if let Some(ref tree) = tree {
                        match tree.get_path(file) {
                            Ok(tree_entry) => {
                                manifest_modes.insert(i, _get_file_mode(&tree_entry)?);
                            }
                            Err(_) => {
                                error!("Trying to update {file:?} which does not exist");
                                return Err(GitXetRepoError::InvalidOperation(format!(
                                    "Trying to update {file:?} which does not exist",
                                )));
                            }
                        }
                    } else {
                        error!("Trying to update {file:?} which does not exist");
                        return Err(GitXetRepoError::InvalidOperation(format!(
                            "Trying to update {file:?} which does not exist"
                        )));
                    }
                    m_update_count += 1;
                }
            }
            // do not put code here. The match above has "continue" cases
        }
    }

    Span::current().record("manifest_counts", 
        format!("Total: {} (Create: {m_create_count}, Upserts: {m_upsert_count}, Deletes: {m_delete_count}, Moves: {m_move_count}, Updates: {m_update_count})", manifest_entries.len()));

    info!("manifest_counts: Sanity check completed.");

    let preparation_span = span!(Level::INFO, "Preparation");
    let preparation_span_lg = preparation_span.enter();

    // build the commit signature first to early check for correctness
    let signature = git2::Signature::now(author, author_email)?;

    // convert objects in the manifest to an oid
    for (i, manifest_entry) in manifest_entries.iter().enumerate() {
        match manifest_entry {
            ManifestEntry::Create {
                file: _,
                modeexec: _,
                content,
                githash_content,
            }
            | ManifestEntry::Upsert {
                file: _,
                modeexec: _,
                content,
                githash_content,
            }
            | ManifestEntry::Update {
                file: _,
                content,
                githash_content,
            } => {
                if let Some(ref oid) = githash_content {
                    manifest_oids.insert(i, *oid);
                } else {
                    let oid = repo.blob(content)?;
                    manifest_oids.insert(i, oid);
                }
            }
            ManifestEntry::Move {
                file: _,
                prev_path: _,
                content: Some(content),
            } => {
                // Move actions that do not specify content preserve the existing file content,
                // and any other value of content overwrites the file content.
                let oid = repo.blob(content)?;
                manifest_oids.insert(i, oid);
            }
            _ => {}
        }
    }

    // to build up all the intermediate trees can actually be a little bit
    // tricky... except libgit2 provides this very nice TreeUpdateBuilder!
    let mut builder = git2::build::TreeUpdateBuilder::new();
    for (i, manifest_entry) in manifest_entries.into_iter().enumerate() {
        match manifest_entry {
            ManifestEntry::Create {
                file,
                modeexec: _,
                content: _,
                githash_content: _,
            }
            | ManifestEntry::Upsert {
                file,
                modeexec: _,
                content: _,
                githash_content: _,
            }
            | ManifestEntry::Update {
                file,
                content: _,
                githash_content: _,
            } => {
                let oid = manifest_oids[&i];
                let mode = manifest_modes[&i];
                builder.upsert(&file, oid, mode);
            }
            ManifestEntry::Delete { file } => {
                builder.remove(&file);
            }
            ManifestEntry::Move {
                file,
                prev_path,
                content: _,
            } => {
                let oid = manifest_oids[&i];
                let mode = manifest_modes[&i];
                builder.remove(&prev_path);
                builder.upsert(&file, oid, mode);
            }
        }
    }

    drop(preparation_span_lg);

    let lock_acquisition_span = span!(Level::INFO, "Transaction Lock Acquisition");
    let lock_acquisition_span_lg: span::Entered<'_> = lock_acquisition_span.enter();

    // ok. Now we need to construct the commit
    // lock the ref first
    let mut reflock = repo.transaction()?;
    let mut retry_time_ms = 100;
    loop {
        match reflock.lock_ref(&refname) {
            Ok(_) => {
                break;
            }
            Err(e) => {
                if e.code() == git2::ErrorCode::Locked {
                    info!("Locked. Sleeping for {retry_time_ms} ms and retry");
                    let sleep_time = std::time::Duration::from_millis(retry_time_ms);
                    std::thread::sleep(sleep_time);
                    // multiplicative backoff. But no longer than MAX_RETRY_TIME_MS
                    retry_time_ms = std::cmp::min(retry_time_ms * 2, MAX_RETRY_TIME_MS);
                    continue;
                } else {
                    return Err(e.into());
                }
            }
        }
    }
    drop(lock_acquisition_span_lg);

    let commit_span = span!(Level::INFO, "Committing");
    let commit_span_lg: span::Entered<'_> = commit_span.enter();
    let prev_reference = repo.find_reference(&refname).ok().or_else(|| {
        if create_ref {
            repo.head().ok()
        } else {
            None
        }
    });

    let commit = if let Some(reference) = prev_reference {
        // find the tree at the current ref
        let previous_commit = reference.peel_to_commit()?;
        let tree = previous_commit.tree()?;
        // create an updated tree
        let result_tree_oid = builder.create_updated(repo, &tree)?;
        let result_tree = repo.find_tree(result_tree_oid)?;
        // commit the changes
        let commit_oid = repo.commit(
            None,
            &signature,
            &signature,
            commit_message,
            &result_tree,
            &[&previous_commit],
        )?;
        let commit = repo.find_commit(commit_oid)?;
        // update the ref
        reflock.set_target(&refname, commit_oid, Some(&signature), commit_message)?;
        reflock.commit()?;
        commit
    } else if create_ref {
        // we creating the ref from scratch. so we need to work backwards a bit
        // start by making an empty tree
        let empty_treebuilder = repo.treebuilder(None)?;
        let empty_tree_oid = empty_treebuilder.write()?;
        let empty_tree = repo.find_tree(empty_tree_oid)?;
        // create the result tree
        let result_tree_oid = builder.create_updated(repo, &empty_tree)?;
        let result_tree = repo.find_tree(result_tree_oid)?;
        // create a commit with no parent
        let commit_oid = repo.commit(
            None,
            &signature,
            &signature,
            commit_message,
            &result_tree,
            &[],
        )?;
        let commit = repo.find_commit(commit_oid)?;
        // then create the ref
        reflock.set_target(&refname, commit_oid, Some(&signature), commit_message)?;
        reflock.commit()?;
        commit
    } else {
        return Err(GitXetRepoError::InvalidOperation(format!(
            "Unable to find reference {refname:?}"
        )));
    };

    drop(commit_span_lg);

    Ok((refname, commit))
}

pub fn atomic_commit(
    repo: &Arc<git2::Repository>,
    manifest_entries: Vec<ManifestEntry>,
    refname: &str,
    commit_message: &str,
    author: &str,
    author_email: &str,
    create_ref: bool,
) -> Result<AtomicCommitOutput> {
    let (_, commit) = atomic_commit_impl(
        repo,
        manifest_entries,
        refname,
        commit_message,
        author,
        author_email,
        create_ref,
    )?;

    let id = commit.id().to_string();
    let mut short_id = id.clone();
    short_id.truncate(8);
    let author = commit.author();
    let committer = commit.committer();
    let committed_date: DateTime<Utc> = Utc
        .timestamp_millis_opt(commit.time().seconds() * 1000)
        .single()
        .ok_or(GitXetRepoError::Other(
            "Unable to create DateTime".to_string(),
        ))?;
    let parent_ids = commit
        .parent_ids()
        .map(|parent_id| parent_id.to_string())
        .collect::<Vec<String>>();

    let ret = AtomicCommitOutput {
        id,
        short_id,
        title: commit.summary().map(str::to_string),
        author_name: author.name().map(str::to_string),
        author_email: author.email().map(str::to_string),
        committer_name: committer.name().map(str::to_string),
        committer_email: committer.email().map(str::to_string),
        message: commit.message().map(str::to_string),
        committed_date,
        parent_ids,
    };

    Ok(ret)
}
