use super::git_notes_wrapper::{GitNotesContentIterator, GitNotesIteratorWrapper};
use super::git_process_wrapping;
use crate::errors::{GitXetRepoError, Result};
use crate::git_integration::bare_repo_commits::atomic_commit_impl;
use crate::git_integration::bare_repo_commits::ManifestEntry;
use crate::git_integration::git_notes_wrapper::GitNotesNameIterator;
use git2::ObjectType;
use git2::Oid;
use git2::Repository;
use git2::TreeWalkMode;
use git2::TreeWalkResult;
use std::sync::RwLock;
use std::{
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::error;
use tracing::{debug, info};

// A collection of encapsulating classes and other traits that
//

pub struct Git2WrapperImpl {
    repo: Repository,
}

unsafe impl Send for Git2WrapperImpl {}
unsafe impl Sync for Git2WrapperImpl {}

impl Deref for Git2WrapperImpl {
    type Target = Repository;

    fn deref(&self) -> &Self::Target {
        &self.repo
    }
}

impl DerefMut for Git2WrapperImpl {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.repo
    }
}

impl AsRef<Repository> for Git2WrapperImpl {
    fn as_ref(&self) -> &Repository {
        &self.repo
    }
}

impl AsMut<Repository> for Git2WrapperImpl {
    fn as_mut(&mut self) -> &mut Repository {
        &mut self.repo
    }
}

pub struct Git2RepositoryReadGuard<'a> {
    read_guard: std::sync::RwLockReadGuard<'a, Git2WrapperImpl>,
}

impl<'a> Deref for Git2RepositoryReadGuard<'a> {
    type Target = Repository;

    fn deref<'b>(&'b self) -> &'b Self::Target {
        &self.read_guard.repo
    }
}

impl<'a> AsRef<Repository> for Git2RepositoryReadGuard<'a> {
    fn as_ref<'b>(&'b self) -> &'b Repository {
        &self.read_guard.repo
    }
}

pub struct Git2RepositoryWriteGuard<'a> {
    write_guard: std::sync::RwLockWriteGuard<'a, Git2WrapperImpl>,
}

impl<'a> Deref for Git2RepositoryWriteGuard<'a> {
    type Target = Repository;

    fn deref(&self) -> &Self::Target {
        &self.write_guard.repo
    }
}

impl<'a> AsRef<Repository> for Git2RepositoryWriteGuard<'a> {
    fn as_ref(&self) -> &Repository {
        &self.write_guard.repo
    }
}
impl<'a> DerefMut for Git2RepositoryWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.write_guard.repo
    }
}

impl<'a> AsMut<Repository> for Git2RepositoryWriteGuard<'a> {
    fn as_mut(&mut self) -> &mut Repository {
        &mut self.write_guard.repo
    }
}

pub struct GitRepo {
    repo: RwLock<Git2WrapperImpl>,
    git_dir: PathBuf,
    work_dir: Option<PathBuf>,
}

impl GitRepo {
    pub fn open(repo_path: Option<&Path>) -> Result<Arc<Self>> {
        let repo = match repo_path {
            Some(path) => {
                if path == PathBuf::default() {
                    git2::Repository::open_from_env()?
                } else {
                    git2::Repository::discover(path)?
                }
            }
            None => git2::Repository::open_from_env()?,
        };

        let git_dir = repo.path().to_path_buf();
        let work_dir = repo.workdir().map(|f| f.to_path_buf());

        Ok(Arc::new(Self {
            repo: RwLock::new(Git2WrapperImpl { repo }),
            git_dir,
            work_dir,
        }))
    }

    pub fn git_dir(&self) -> &Path {
        &self.git_dir
    }

    pub fn work_dir(&self) -> Option<&Path> {
        self.work_dir.as_ref().map(PathBuf::as_path)
    }

    pub fn repo_dir(&self) -> &Path {
        self.work_dir().unwrap_or(&self.git_dir)
    }

    pub fn read<'a>(&'a self) -> Git2RepositoryReadGuard<'a> {
        Git2RepositoryReadGuard {
            read_guard: self
                .repo
                .read()
                .expect("Locking error due to panic in other thread."),
        }
    }

    pub fn write<'a>(&'a self) -> Git2RepositoryWriteGuard<'a> {
        Git2RepositoryWriteGuard {
            write_guard: self
                .repo
                .write()
                .expect("Locking error due to panic in other thread."),
        }
    }

    pub fn ref_to_oid(&self, refname: &str) -> Result<Option<Oid>> {
        let oid = self.read().refname_to_id(refname);

        match oid {
            Ok(oid) => Ok(Some(oid)),
            Err(e) => {
                if e.code() == git2::ErrorCode::NotFound {
                    Ok(None)
                } else {
                    Err(GitXetRepoError::from(e))
                }
            }
        }
    }

    /// Working with Xet Notes.
    ///
    /// This provides storage of arbitrary blobs
    /// into notes.
    ///
    /// A note is a mapping from An "Annotated Oid" ==> "Blob Oid"
    ///
    /// 1) Typically this "Annotated Oid" is some other object / commit id.
    ///
    /// 2) The Blob contents, while theoretically can be any arbitrary
    /// byte string, the libgit2 C API *requires* this string to be a C string,
    /// and the rust API then requires it to be utf-8. (Interestingly, the raw git
    /// commandline actually does not require this).
    ///
    /// (1) can be a little limiting as it may be hard to always find an Annotated
    /// ID to use. We could in the post-commit hook for instance, find the HEAD
    /// commit id. But this may not be resilient to other stranger use cases.
    ///
    /// we take advantage of the fact that this "Annotated Oid" does not
    /// actually have to exist. i.e. we can just put really any random number into
    /// it. To have this work consistently , we will actually use a hash of the
    /// *contents* of the blob as the annotated Oid. The advantage here is that
    /// we do not have to worry about overwriting existing blobs; i.e. if you have
    /// to overwrite, it is an identical blob.
    ///
    /// For (2), unfortunately this means I actually need to force convert
    /// a &[u8] into something utf-8-ish unless we fork and patch:
    ///  -libgit
    ///  -libgit2-rs
    ///
    /// Originally I considered the option of storing the blob seperately, and
    /// just writing a reference to the blob in the note. But this means the actual
    /// blob is not in the tree and doesn't get pushed.
    ///
    ///
    /// So the result is in pseudo-code
    ///
    /// add_note(bytes) {
    ///    blob_oid = git_add_blob(escape(bytes))
    ///    create note mapping blob_oid -> escape(blob_oid)
    /// }
    ///
    ///
    /// iterator() {
    ///    for each note (annotated_id, note_oid) {
    ///        bytes = unescape(read annotated_oid)
    ///        yield bytes
    ///    }
    /// }

    pub fn xet_notes_content_iterator<'a>(
        &'a self,
        notes_ref: &str,
    ) -> Result<GitNotesContentIterator<'a>> {
        let read_guard = self.read();

        match read_guard.notes(Some(&notes_ref)) {
            Err(e) => {
                info!("Error attempting to open notes {notes_ref}, ignoring content.",);
                if e.code() == git2::ErrorCode::NotFound {
                    Ok(GitNotesContentIterator { notes: None })
                } else {
                    Err(GitXetRepoError::from(e))
                }
            }
            Ok(note_iter) => Ok(GitNotesContentIterator {
                notes: Some(GitNotesIteratorWrapper {
                    read_guard,
                    note_iter,
                }),
            }),
        }
    }

    pub fn xet_notes_name_iterator<'a>(
        &'a self,
        notes_ref: &str,
    ) -> Result<GitNotesNameIterator<'a>> {
        let read_guard = self.read();

        match read_guard.notes(Some(&notes_ref)) {
            Err(e) => {
                info!("Error attempting to open notes {notes_ref}, ignoring content.",);
                if e.code() == git2::ErrorCode::NotFound {
                    Ok(GitNotesNameIterator { notes: None })
                } else {
                    Err(GitXetRepoError::from(e))
                }
            }
            Ok(note_iter) => Ok(GitNotesNameIterator {
                notes: Some(GitNotesIteratorWrapper {
                    read_guard,
                    note_iter,
                }),
            }),
        }
    }

    /// Return the content of a note with a give name.
    #[allow(clippy::type_complexity)]
    pub fn xet_notes_name_to_content(&self, name: &str) -> Result<Vec<u8>> {
        let oid = git2::Oid::from_str(name)?;
        let repo_ro = self.read();
        let blob = repo_ro.find_blob(oid)?;

        if let Ok(ret) = base64::decode(blob.content()) {
            Ok(ret)
        } else {
            error!("Unable to decode blob {:?}", blob.id());
            Err(GitXetRepoError::GitRepoError(git2::Error::new(
                git2::ErrorCode::Invalid,
                git2::ErrorClass::Object,
                "Unable to decode blob as base64",
            )))
        }
    }

    /// Adds some content into the repository. This can be read out later
    /// via notes_content_iterator()
    pub fn add_xet_note<T: AsRef<[u8]>>(&self, notes_ref: &str, content: T) -> Result<()> {
        let repo_w = self.write();
        let sig = repo_w.signature()?;
        let content_str = base64::encode(content.as_ref());
        let blob_oid = repo_w.blob(content_str.as_bytes())?;
        let note_ok = repo_w.note(&sig, &sig, Some(notes_ref), blob_oid, &content_str, false);
        match note_ok {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.code() == git2::ErrorCode::Exists {
                    Ok(())
                } else {
                    Err(GitXetRepoError::GitRepoError(e))
                }
            }
        }
    }

    /// Checks if a certain note already exists.
    pub fn xet_note_with_exists<T: AsRef<[u8]>>(
        &self,
        notes_ref: &str,
        content: T,
    ) -> Result<bool> {
        let content_str = base64::encode(content.as_ref());
        let blob_oid = git2::Oid::hash_object(git2::ObjectType::Blob, content_str.as_bytes())?;

        Ok(self.read().find_note(Some(notes_ref), blob_oid).is_ok())
    }

    pub fn list_remotes(&self) -> Result<Vec<(String, String)>> {
        info!("XET: Listing git remotes");

        let repo = self.read();

        // first get the list of remotes
        let remotes = match repo.remotes() {
            Err(e) => {
                error!("Error: Unable to list remotes : {:?}", &e);
                return Ok(vec![]);
            }
            Ok(r) => r,
        };

        if remotes.is_empty() {
            return Ok(vec![]);
        }

        let mut result = Vec::new();
        // get the remote object and extract the URLs
        let mut i = remotes.iter();
        while let Some(Some(remote)) = i.next() {
            if let Some(info) = repo.find_remote(remote)?.url() {
                result.push((remote.to_string(), info.to_string()));
            }
        }
        Ok(result)
    }

    pub fn remote_urls<'a>(&'a self) -> Result<Vec<String>> {
        info!("XET: Listing git remotes");

        let repo = self.read();

        // first get the list of remotes
        let remotes = match repo.remotes() {
            Err(e) => {
                error!("Error: Unable to list remotes : {:?}", &e);
                return Ok(vec![]);
            }
            Ok(r) => r,
        };

        if remotes.is_empty() {
            return Ok(vec![]);
        }

        Ok(remotes
            .iter()
            .flatten()
            .filter_map(|r| {
                repo.find_remote(r)
                    .ok()
                    .and_then(|rr| rr.url().map(<_>::to_owned))
            })
            .collect())
    }

    pub fn remote_names<'a>(&'a self) -> Result<Vec<String>> {
        let repo = self.read();

        // first get the list of remotes
        match repo.remotes() {
            Err(e) => {
                error!("Error: Unable to list remotes : {:?}", &e);
                return Ok(vec![]);
            }
            Ok(r) => Ok(r.into_iter().flatten().map(<_>::to_owned).collect()),
        }
    }

    /// Add files to a repo by directly going to the index.  Works on regular or bare repos.  Will not change checked-out
    /// files.
    ///
    /// If the branch_name is given, the commit will be added to that branch.  If branch_name is None, than HEAD will be used.
    /// If main_branch_name_if_empty_repo is given, then a branch will be created containing only this commit if there are no
    /// branches in the repo.
    pub fn create_commit(
        &self,
        branch_name: Option<&str>,
        commit_message: &str,
        files: &[(&str, &[u8])],
        main_branch_name_if_empty_repo: Option<&str>, // If given, make sure the repo has at least one branch
    ) -> Result<()> {
        let repo = self.write();

        let default_branch = main_branch_name_if_empty_repo.unwrap_or("main");

        let branch_name = branch_name.unwrap_or("HEAD");

        let mut _head = None;

        // Create the commit
        let (refname, mut set_head) = {
            if branch_name != "HEAD" {
                (branch_name, false)
            } else if !repo.branches(None)?.any(|_| true) {
                info!(
                    "git_wrap:create_commit: Setting HEAD to point to new branch {default_branch}."
                );
                (default_branch, true)
            } else {
                _head = repo.head().ok();
                (
                    _head
                        .as_ref()
                        .and_then(|r| r.name())
                        .unwrap_or(default_branch),
                    false,
                )
            }
        };

        // If the reference doesn't exist, create a new branch with that.
        if !refname.starts_with("refs/") {
            // See if it's a branch
            if repo.find_branch(refname, git2::BranchType::Local).is_err() {
                // The branch does not exist, create it from HEAD if it exists.  Otherwise, set head later
                if let Ok(commit) = repo.head().and_then(|r| r.peel_to_commit()) {
                    repo.branch(refname, &commit, false)?;
                } else {
                    set_head = true;
                }
            }
        }

        debug!("git_wrap:create_commit: update_ref = {refname:?}");

        let config = git2::Config::open_default()?;

        // Retrieve the user's name and email
        let user_name = config.get_string("user.name")?;
        let user_email = config.get_string("user.email")?;

        let (refname, _) = atomic_commit_impl(
            &repo,
            files
                .iter()
                .map(|(name, data)| ManifestEntry::Upsert {
                    file: name.into(),
                    modeexec: false,
                    content: (*data).into(),
                    githash_content: None,
                })
                .collect(),
            refname,
            commit_message,
            &user_name,
            &user_email,
            true,
        )?;

        if set_head {
            repo.set_head(&refname)?;
        }

        Ok(())
    }

    /// Read a file from the repo directly from the index.  If the branch is not given, then HEAD is used.  
    pub fn read_file_from_repo<'a>(
        &'a self,
        file_path: &str,
        branch: Option<&str>,
    ) -> Result<Option<Vec<u8>>> {
        let repo = self.read();

        // Resolve HEAD or the specified branch to the corresponding commit
        let (_reference_name, commit) = match branch {
            Some(branch_name) => {
                let reference_name = format!("refs/heads/{}", branch_name);
                let reference = repo.find_reference(&reference_name)?;
                (reference_name, reference.peel_to_commit()?)
            }
            None => {
                let Ok(head) = repo.head() else {
                    return Ok(None);
                };
                ("HEAD".to_owned(), head.peel_to_commit()?)
            }
        };
        {
            if let Some(ret) = 'a: {
                if let Ok(tree) = commit.tree() {
                    if let Ok(entry) = tree.get_path(std::path::Path::new(file_path)) {
                        if entry.kind() == Some(git2::ObjectType::Blob) {
                            if let Ok(blob) = repo.find_blob(entry.id()) {
                                break 'a Some(Vec::<u8>::from(blob.content()));
                            }
                        }
                    }
                }

                None
            } {
                #[cfg(debug_assertions)]
                {
                    let git_out = git_process_wrapping::run_git_captured_raw(
                        Some(&repo.path().to_path_buf()),
                        "show",
                        &[&format!("{_reference_name}:{file_path}")],
                        false,
                        None,
                    )?;
                    debug_assert_eq!(git_out.status.code(), Some(0));

                    if git_out.stdout != ret {
                        let content = std::str::from_utf8(&ret[..]).unwrap_or("<Binary Data>");
                        let res_stdout =
                            std::str::from_utf8(&git_out.stdout[..]).unwrap_or("<Binary Data>");
                        let res_stderr =
                            std::str::from_utf8(&git_out.stderr[..]).unwrap_or("<Binary Data>");

                        panic!("Not equal: (retrieved) '{content:?}' != '{res_stdout:?}', error={res_stderr:?}");
                    }
                    debug_assert_eq!(git_out.status.code(), Some(0));
                }

                Ok(Some(ret))
            } else {
                #[cfg(debug_assertions)]
                {
                    let git_out = git_process_wrapping::run_git_captured_raw(
                        Some(&repo.path().to_path_buf()),
                        "show",
                        &[&format!("{_reference_name}:{file_path}")],
                        false,
                        None,
                    )?;
                    debug_assert!(git_out.stdout.is_empty());
                    debug_assert_ne!(git_out.status.code(), Some(0));
                }

                Ok(None)
            }
        }
    }

    /// List files from a repo below a root path.
    /// If root path is "." list files under the repo root.
    /// Return a list of file paths relative to the repo root.
    /// Return empty list if the root path is not found under the specified branch.
    pub fn list_files_from_repo(
        &self,
        root_path: &str,
        branch: Option<&str>,
        recursive: bool,
    ) -> Result<Vec<String>> {
        let repo = self.read();

        const REPO_ROOT_PATH: &str = ".";

        // Resolve HEAD or the specified branch to the corresponding commit
        let commit = match branch {
            Some(branch_name) => {
                let reference = repo.find_reference(&format!("refs/heads/{}", branch_name))?;
                reference.peel_to_commit()?
            }
            None => {
                let Ok(head) = repo.head() else {
                    return Ok(vec![]);
                };
                head.peel_to_commit()?
            }
        };

        // find the tree entry with name equal to root_path
        let tree = commit.tree()?;

        let mut root_path_entry_kind = None;
        let mut root_path_entry_id = Oid::zero();
        if let Ok(entry) = tree.get_path(std::path::Path::new(root_path)) {
            root_path_entry_kind = entry.kind();
            root_path_entry_id = entry.id();
        }

        match root_path_entry_kind {
            None => {
                // entry for path not found and if path is not special case "."
                if root_path != REPO_ROOT_PATH {
                    return Ok(vec![]);
                }
            }
            Some(ObjectType::Blob) => return Ok(vec![root_path.to_owned()]), // this is a file, directly return it
            Some(ObjectType::Tree) => (), // continue to list the subtree
            _ => return Ok(vec![]),       // unrecognized kind, return empty list
        }

        // now root_path is a directory
        // special case, root_path is "." and will not be found by get_path
        let (subtree, ancestor) = if root_path == REPO_ROOT_PATH {
            (tree, "")
        } else {
            (repo.find_tree(root_path_entry_id)?, root_path)
        };

        let mut list = vec![];
        if !recursive {
            // only the direct children of this root_path
            for entry in subtree.iter() {
                if let Some(git2::ObjectType::Blob) = entry.kind() {
                    let file_name = entry.name().unwrap().to_owned();
                    let file_path = Path::new(ancestor)
                        .join(file_name)
                        .to_str()
                        .unwrap_or_default()
                        .to_owned();
                    list.push(file_path);
                }
            }
        } else {
            // recursively list all children under this root_path
            subtree.walk(TreeWalkMode::PreOrder, |parent, entry| {
                if let Some(git2::ObjectType::Blob) = entry.kind() {
                    let file_name = entry.name().unwrap().to_owned();
                    let file_path = Path::new(ancestor)
                        .join(parent)
                        .join(file_name)
                        .to_str()
                        .unwrap_or_default()
                        .to_owned();
                    list.push(file_path);
                }

                TreeWalkResult::Ok
            })?;
        }

        Ok(list)
    }
}

#[cfg(test)]
mod git_repo_tests {
    use super::*;
    use crate::git_integration::git_process_wrapping::run_git_captured;
    use tempfile::TempDir;

    #[test]
    fn test_direct_repo_read_write_branches() -> anyhow::Result<()> {
        // Create a temporary directory
        let tmp_repo = TempDir::new().unwrap();
        let tmp_repo_path = tmp_repo.path().to_path_buf();

        let _ = run_git_captured(Some(&tmp_repo_path), "init", &["--bare"], true, None)?;

        let repo = GitRepo::open(Some(&tmp_repo_path))?;

        let file_1 = "Random Content 1".as_bytes();
        let file_2 = "Random Content 2".as_bytes();
        let file_2b = "Random Content 2b".as_bytes();
        let file_2c = "Random Content 2c".as_bytes();
        let file_3 = "Random Content 3".as_bytes();

        repo.create_commit(
            None,
            "Test commit",
            &[("file_1.txt", file_1), ("file_2.txt", file_2)],
            None,
        )?;

        // Make sure that we can get those back
        let file_1_read = repo.read_file_from_repo("file_1.txt", None)?.unwrap();
        let file_2_read = repo.read_file_from_repo("file_2.txt", None)?.unwrap();

        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2, file_2_read);

        // Make sure we can overwrite things correctly.
        repo.create_commit(
            None,
            "Test commit updated",
            &[("file_2.txt", file_2b)],
            None,
        )?;
        let file_1_read = repo.read_file_from_repo("file_1.txt", None)?.unwrap();
        let file_2_read = repo.read_file_from_repo("file_2.txt", None)?.unwrap();

        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2b, file_2_read);

        // Now write to a specified branch.  This branch doesn't exist, so it should create a new branch off of main
        repo.create_commit(
            Some("my_branch"),
            "Test commit",
            &[("file_3.txt", file_3)],
            None,
        )?;

        // Read this off of HEAD
        let file_1_read = repo.read_file_from_repo("file_1.txt", None)?.unwrap();
        let file_2_read = repo.read_file_from_repo("file_2.txt", None)?.unwrap();
        let file_3_read = repo
            .read_file_from_repo("file_3.txt", Some("my_branch"))?
            .unwrap();
        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2b, file_2_read);
        assert_eq!(file_3, file_3_read);

        // Read this off of the branch name
        let file_1_read = repo
            .read_file_from_repo("file_1.txt", Some("my_branch"))?
            .unwrap();
        let file_2_read = repo
            .read_file_from_repo("file_2.txt", Some("my_branch"))?
            .unwrap();
        let file_3_read = repo
            .read_file_from_repo("file_3.txt", Some("my_branch"))?
            .unwrap();
        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2b, file_2_read);
        assert_eq!(file_3, file_3_read);

        // Make sure main doesn't change
        let file_1_read = repo
            .read_file_from_repo("file_1.txt", Some("main"))?
            .unwrap();
        let file_2_read = repo
            .read_file_from_repo("file_2.txt", Some("main"))?
            .unwrap();
        let file_3_query = repo.read_file_from_repo("file_3.txt", Some("main"))?;
        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2b, file_2_read);
        assert!(file_3_query.is_none());

        // Write to main, overwrite
        repo.create_commit(
            Some("main"),
            "Test commit",
            &[("file_2.txt", file_2c)],
            None,
        )?;
        let file_1_read = repo
            .read_file_from_repo("file_1.txt", Some("main"))?
            .unwrap();
        let file_2_read = repo
            .read_file_from_repo("file_2.txt", Some("main"))?
            .unwrap();
        let file_3_query = repo.read_file_from_repo("file_3.txt", Some("main"))?;
        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2c, file_2_read);
        assert!(file_3_query.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_repo_respect_delete() -> anyhow::Result<()> {
        // Create a temporary directory
        let tmp_repo = TempDir::new().unwrap();
        let tmp_repo_path = tmp_repo.path().to_path_buf();
        let tmp_repo_1_path = tmp_repo.path().join("repo_1");
        std::fs::create_dir_all(&tmp_repo_1_path)?;

        let _ = run_git_captured(Some(&tmp_repo_1_path), "init", &["--bare"], true, None)?;

        let repo_1 = GitRepo::open(Some(&tmp_repo_path))?;

        let file_1 = "Random Content 1".as_bytes();
        let file_2 = "Random Content 2".as_bytes();
        let file_2b = "Random Content 2b".as_bytes();
        let file_3 = "Random Content 3".as_bytes();
        let file_4 = "Random Content 4".as_bytes();

        repo_1.create_commit(
            None,
            "Test commit",
            &[
                ("file_1.txt", file_1),
                ("file_2.txt", file_2),
                ("file_3.txt", file_3),
            ],
            None,
        )?;
        let file_1_read = repo_1.read_file_from_repo("file_1.txt", None)?.unwrap();
        assert_eq!(file_1, file_1_read);

        // Now clone and check the new version works on mirrored repos
        let _ = run_git_captured(
            Some(&tmp_repo_path),
            "clone",
            &["repo_1", "repo_2"],
            true,
            None,
        )?;
        let tmp_repo_2_path = tmp_repo.path().join("repo_2");

        // Now, add a file, change a file, delete one of the files, commit, and push the change back.
        let _ = run_git_captured(Some(&tmp_repo_2_path), "rm", &["file_1.txt"], true, None)?;
        std::fs::write(tmp_repo_2_path.join("file_2.txt"), file_2b)?;
        let _ = run_git_captured(Some(&tmp_repo_2_path), "add", &["file_2.txt"], true, None)?;
        std::fs::write(tmp_repo_2_path.join("file_4.txt"), file_4)?;
        let _ = run_git_captured(Some(&tmp_repo_2_path), "add", &["file_4.txt"], true, None)?;
        let _ = run_git_captured(
            Some(&tmp_repo_2_path),
            "commit",
            &["-m", "Update."],
            true,
            None,
        )?;
        let _ = run_git_captured(
            Some(&tmp_repo_2_path),
            "push",
            &["origin", "main"],
            true,
            None,
        )?;

        // Now verify all the original things there.
        assert!(repo_1.read_file_from_repo("file_1.txt", None)?.is_none());

        let file_2_read = repo_1.read_file_from_repo("file_2.txt", None)?.unwrap();
        assert_eq!(file_2b, file_2_read);

        let file_3_read = repo_1.read_file_from_repo("file_3.txt", None)?.unwrap();
        assert_eq!(file_3, file_3_read);

        let file_4_read = repo_1.read_file_from_repo("file_4.txt", None)?.unwrap();
        assert_eq!(file_4, file_4_read);
        Ok(())
    }

    #[tokio::test]
    async fn test_repo_read_write_through_mirror_push() -> anyhow::Result<()> {
        // Create a temporary directory
        let tmp_repo = TempDir::new().unwrap();
        let tmp_repo_path = tmp_repo.path().to_path_buf();
        let tmp_repo_1_path = tmp_repo.path().join("repo_1");
        std::fs::create_dir_all(&tmp_repo_1_path)?;

        let _ = run_git_captured(Some(&tmp_repo_1_path), "init", &["--bare"], true, None)?;

        let repo_1 = GitRepo::open(Some(&tmp_repo_1_path))?;

        let file_1 = "Random Content 1".as_bytes();
        let file_2 = "Random Content 2".as_bytes();

        repo_1.create_commit(None, "Test commit", &[("file_1.txt", file_1)], None)?;
        let file_1_read = repo_1.read_file_from_repo("file_1.txt", None)?.unwrap();
        assert_eq!(file_1, file_1_read);

        // Now clone and check the new version works on bare clones
        let _ = run_git_captured(
            Some(&tmp_repo_path),
            "clone",
            &["--bare", "repo_1", "repo_2"],
            true,
            None,
        )?;
        let tmp_repo_2_path = tmp_repo.path().join("repo_2");

        let repo_2 = GitRepo::open(Some(&tmp_repo_2_path))?;

        repo_2.create_commit(None, "Test commit 2", &[("file_2.txt", file_2)], None)?;

        // Make sure that we can get those back (doesn't have to be bare here)
        let file_1_read = repo_2.read_file_from_repo("file_1.txt", None)?.unwrap();
        let file_2_read = repo_2.read_file_from_repo("file_2.txt", None)?.unwrap();

        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2, file_2_read);

        // Now, can we push back?
        let _ = run_git_captured(
            Some(&tmp_repo_2_path),
            "push",
            &["--force", "origin", "main"],
            true,
            None,
        )?;

        // Make sure that all the files are still there after the push.
        let file_1_read = repo_1.read_file_from_repo("file_1.txt", None)?.unwrap();
        let file_2_read = repo_1.read_file_from_repo("file_2.txt", None)?.unwrap();

        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2, file_2_read);

        Ok(())
    }

    #[tokio::test]
    async fn test_list_repo_files() -> anyhow::Result<()> {
        // Create a temporary directory
        let tmp_repo = TempDir::new().unwrap();
        let tmp_repo_path = tmp_repo.path().to_path_buf();

        let _ = run_git_captured(Some(&tmp_repo_path), "init", &["--bare"], true, None)?;

        let repo = GitRepo::open(Some(&tmp_repo_path))?;

        let (path_1, file_1) = ("1.txt", "Random Content 1".as_bytes());
        let (path_2, file_2) = ("2.csv", "Random Content 2".as_bytes());
        let (path_3, file_3) = ("data/3.dat", "Random Content 3".as_bytes());
        let (path_4, file_4) = ("data/4.mp3", "Random Content 4".as_bytes());
        let (path_5, file_5) = ("data/imgs/5.png", "Random Content 5".as_bytes());
        let (path_6, file_6) = ("data/mov/6.mov", "Random Content 6".as_bytes());

        let path_and_files = vec![
            (path_1, file_1),
            (path_2, file_2),
            (path_3, file_3),
            (path_4, file_4),
            (path_5, file_5),
            (path_6, file_6),
        ];
        repo.create_commit(None, "Test commit", &path_and_files, None)?;

        // list single file
        let root_path = "data/imgs/5.png";
        let recursive = false;
        let files = repo.list_files_from_repo(root_path, None, recursive)?;
        let expected = ["data/imgs/5.png"];
        assert_eq!(&files, &expected);

        // list single file recursive
        let root_path = "data/mov/6.mov";
        let recursive = true;
        let files = repo.list_files_from_repo(root_path, None, recursive)?;
        let expected = ["data/mov/6.mov"];
        assert_eq!(&files, &expected);

        // list directory
        let root_path = "data";
        let recursive = false;
        let mut files = repo.list_files_from_repo(root_path, None, recursive)?;
        let mut expected = ["data/3.dat", "data/4.mp3"];
        files.sort();
        expected.sort();
        assert_eq!(&files, &expected);

        // list directory recursive
        let root_path = "data";
        let recursive = true;
        let mut files = repo.list_files_from_repo(root_path, None, recursive)?;
        let mut expected = [
            "data/3.dat",
            "data/4.mp3",
            "data/imgs/5.png",
            "data/mov/6.mov",
        ];
        files.sort();
        expected.sort();
        assert_eq!(&files, &expected);

        // list root
        let root_path: &str = ".";
        let recursive = false;
        let mut files = repo.list_files_from_repo(root_path, None, recursive)?;
        let mut expected = ["1.txt", "2.csv"];
        files.sort();
        expected.sort();
        assert_eq!(&files, &expected);

        // list root recursive
        let root_path: &str = ".";
        let recursive = true;
        let mut files = repo.list_files_from_repo(root_path, None, recursive)?;
        let mut expected = [
            "1.txt",
            "2.csv",
            "data/3.dat",
            "data/4.mp3",
            "data/imgs/5.png",
            "data/mov/6.mov",
        ];
        files.sort();
        expected.sort();
        assert_eq!(&files, &expected);

        // list invalid path
        let root_path: &str = "xx";
        let recursive = false;
        let files = repo.list_files_from_repo(root_path, None, recursive)?;
        let expected = Vec::<String>::new();
        assert_eq!(&files, &expected);

        Ok(())
    }
}
