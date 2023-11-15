use super::git_process_wrapping;
use crate::errors::Result;
use crate::git_integration::git_commits::atomic_commit_impl;
use crate::git_integration::git_commits::ManifestEntry;
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

    fn deref(&self) -> &'a Self::Target {
        &self.read_guard.repo
    }
}

impl<'a> AsRef<Repository> for Git2RepositoryReadGuard<'a> {
    fn as_ref(&self) -> &'a Repository {
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

pub struct Git2Wrapper {
    repo: RwLock<Git2WrapperImpl>,
    pub git_dir: PathBuf,
    pub repo_dir: PathBuf,
}

impl Git2Wrapper {
    pub fn open(repo_path: Option<impl AsRef<Path>>) -> Result<Arc<Self>> {
        let repo = match repo_path {
            Some(path) => {
                if path.as_ref() == PathBuf::default() {
                    git2::Repository::open_from_env()?
                } else {
                    git2::Repository::discover(path)?
                }
            }
            None => git2::Repository::open_from_env()?,
        };

        let git_dir = repo.path().to_path_buf();
        let repo_dir = git_dir.clone();

        Ok(Arc::new(Self {
            repo: RwLock::new(Git2WrapperImpl { repo }),
            git_dir,
            repo_dir,
        }))
    }

    pub async fn read<'a>(&'a self) -> Git2RepositoryReadGuard {
        Git2RepositoryReadGuard {
            read_guard: self.repo.read(),
        }
    }

    pub async fn write<'a>(&'a self) -> Git2RepositoryWriteGuard {
        Git2RepositoryWriteGuard {
            write_guard: self.repo.write().await,
        }
    }

    pub async fn repo_dir(&self) -> PathBuf {
        let repo = self.read().await;

        match repo.workdir() {
            Some(p) => p,
            None => repo.path(), // When it's a bare directory
        }
        .to_path_buf()
    }

    pub async fn list_remotes(&self) -> Result<Vec<(String, String)>> {
        info!("XET: Listing git remotes");

        let repo = self.read().await;

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

    /// Add files to a repo by directly going to the index.  Works on regular or bare repos.  Will not change checked-out
    /// files.
    ///
    /// If the branch_name is given, the commit will be added to that branch.  If branch_name is None, than HEAD will be used.
    /// If main_branch_name_if_empty_repo is given, then a branch will be created containing only this commit if there are no
    /// branches in the repo.
    pub async fn create_commit(
        &self,
        branch_name: Option<&str>,
        commit_message: &str,
        files: &[(&str, &[u8])],
        main_branch_name_if_empty_repo: Option<&str>, // If given, make sure the repo has at least one branch
    ) -> Result<()> {
        let repo = self.write().await;

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
    pub async fn read_file_from_repo<'a>(
        &'a self,
        file_path: &str,
        branch: Option<&str>,
    ) -> Result<Option<Vec<u8>>> {
        let repo = self.read().await;

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
    pub async fn list_files_from_repo(
        &self,
        root_path: &str,
        branch: Option<&str>,
        recursive: bool,
    ) -> Result<Vec<String>> {
        let repo = self.read().await;

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

    /// Filter out file paths that are not in the repo index (untracked files).
    pub async fn filter_files_from_index(&self, files: &[PathBuf]) -> Result<Vec<PathBuf>> {
        let index = self.read().await.index()?;

        let mut ret = vec![];

        for f in files {
            // This enum value is taken from https://github.com/libgit2/libgit2/blob/45fd9ed7ae1a9b74b957ef4f337bc3c8b3df01b5/include/git2/index.h#L157
            // Follow the exam in https://libgit2.org/libgit2/ex/HEAD/ls-files.html
            /** A normal staged file in the index. */
            const GIT_INDEX_STAGE_NORMAL: i32 = 0;

            let entry_opt = index.get_path(f, GIT_INDEX_STAGE_NORMAL);
            if let Some(_entry) = entry_opt {
                // In debug mode, verifies the file path found exactly matches the query
                #[cfg(debug_assertions)]
                assert_eq!(bytes2path(&_entry.path), f);
                ret.push(f.to_owned());
            }
        }

        Ok(ret)
    }
}

// From git2::util
#[cfg(unix)]
#[cfg(debug_assertions)]
fn bytes2path(b: &[u8]) -> &Path {
    use std::{ffi::OsStr, os::unix::prelude::*};
    Path::new(OsStr::from_bytes(b))
}
#[cfg(windows)]
#[cfg(debug_assertions)]
fn bytes2path(b: &[u8]) -> &Path {
    use std::str;
    Path::new(str::from_utf8(b).unwrap())
}

#[cfg(test)]
mod git_repo_tests {
    use super::*;
    use crate::git_integration::git_process_wrapping::run_git_captured;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_direct_repo_read_write_branches() -> anyhow::Result<()> {
        // Create a temporary directory
        let tmp_repo = TempDir::new().unwrap();
        let tmp_repo_path = tmp_repo.path().to_path_buf();

        let _ = run_git_captured(Some(&tmp_repo_path), "init", &["--bare"], true, None)?;

        let repo = Git2Wrapper::open(Some(&tmp_repo_path))?;

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
        )
        .await?;

        // Make sure that we can get those back
        let file_1_read = repo.read_file_from_repo("file_1.txt", None).await?.unwrap();
        let file_2_read = repo.read_file_from_repo("file_2.txt", None).await?.unwrap();

        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2, file_2_read);

        // Make sure we can overwrite things correctly.
        repo.create_commit(
            None,
            "Test commit updated",
            &[("file_2.txt", file_2b)],
            None,
        )
        .await?;
        let file_1_read = repo.read_file_from_repo("file_1.txt", None).await?.unwrap();
        let file_2_read = repo.read_file_from_repo("file_2.txt", None).await?.unwrap();

        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2b, file_2_read);

        // Now write to a specified branch.  This branch doesn't exist, so it should create a new branch off of main
        repo.create_commit(
            Some("my_branch"),
            "Test commit",
            &[("file_3.txt", file_3)],
            None,
        )
        .await?;

        // Read this off of HEAD
        let file_1_read = repo.read_file_from_repo("file_1.txt", None).await?.unwrap();
        let file_2_read = repo.read_file_from_repo("file_2.txt", None).await?.unwrap();
        let file_3_read = repo
            .read_file_from_repo("file_3.txt", Some("my_branch"))
            .await?
            .unwrap();
        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2b, file_2_read);
        assert_eq!(file_3, file_3_read);

        // Read this off of the branch name
        let file_1_read = repo
            .read_file_from_repo("file_1.txt", Some("my_branch"))
            .await?
            .unwrap();
        let file_2_read = repo
            .read_file_from_repo("file_2.txt", Some("my_branch"))
            .await?
            .unwrap();
        let file_3_read = repo
            .read_file_from_repo("file_3.txt", Some("my_branch"))
            .await?
            .unwrap();
        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2b, file_2_read);
        assert_eq!(file_3, file_3_read);

        // Make sure main doesn't change
        let file_1_read = repo
            .read_file_from_repo("file_1.txt", Some("main"))
            .await?
            .unwrap();
        let file_2_read = repo
            .read_file_from_repo("file_2.txt", Some("main"))
            .await?
            .unwrap();
        let file_3_query = repo.read_file_from_repo("file_3.txt", Some("main")).await?;
        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2b, file_2_read);
        assert!(file_3_query.is_none());

        // Write to main, overwrite
        repo.create_commit(
            Some("main"),
            "Test commit",
            &[("file_2.txt", file_2c)],
            None,
        )
        .await?;
        let file_1_read = repo
            .read_file_from_repo("file_1.txt", Some("main"))
            .await?
            .unwrap();
        let file_2_read = repo
            .read_file_from_repo("file_2.txt", Some("main"))
            .await?
            .unwrap();
        let file_3_query = repo.read_file_from_repo("file_3.txt", Some("main")).await?;
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

        let repo_1 = Git2Wrapper::open(Some(&tmp_repo_path))?;

        let file_1 = "Random Content 1".as_bytes();
        let file_2 = "Random Content 2".as_bytes();
        let file_2b = "Random Content 2b".as_bytes();
        let file_3 = "Random Content 3".as_bytes();
        let file_4 = "Random Content 4".as_bytes();

        repo_1
            .create_commit(
                None,
                "Test commit",
                &[
                    ("file_1.txt", file_1),
                    ("file_2.txt", file_2),
                    ("file_3.txt", file_3),
                ],
                None,
            )
            .await?;
        let file_1_read = repo_1
            .read_file_from_repo("file_1.txt", None)
            .await?
            .unwrap();
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
        assert!(repo_1
            .read_file_from_repo("file_1.txt", None)
            .await?
            .is_none());

        let file_2_read = repo_1
            .read_file_from_repo("file_2.txt", None)
            .await?
            .unwrap();
        assert_eq!(file_2b, file_2_read);

        let file_3_read = repo_1
            .read_file_from_repo("file_3.txt", None)
            .await?
            .unwrap();
        assert_eq!(file_3, file_3_read);

        let file_4_read = repo_1
            .read_file_from_repo("file_4.txt", None)
            .await?
            .unwrap();
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

        let repo_1 = Git2Wrapper::open(Some(&tmp_repo_1_path))?;

        let file_1 = "Random Content 1".as_bytes();
        let file_2 = "Random Content 2".as_bytes();

        repo_1
            .create_commit(None, "Test commit", &[("file_1.txt", file_1)], None)
            .await?;
        let file_1_read = repo_1
            .read_file_from_repo("file_1.txt", None)
            .await?
            .unwrap();
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

        let repo_2 = Git2Wrapper::open(Some(&tmp_repo_2_path))?;

        repo_2
            .create_commit(None, "Test commit 2", &[("file_2.txt", file_2)], None)
            .await?;

        // Make sure that we can get those back (doesn't have to be bare here)
        let file_1_read = repo_2
            .read_file_from_repo("file_1.txt", None)
            .await?
            .unwrap();
        let file_2_read = repo_2
            .read_file_from_repo("file_2.txt", None)
            .await?
            .unwrap();

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
        let file_1_read = repo_1
            .read_file_from_repo("file_1.txt", None)
            .await?
            .unwrap();
        let file_2_read = repo_1
            .read_file_from_repo("file_2.txt", None)
            .await?
            .unwrap();

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

        let repo = Git2Wrapper::open(Some(&tmp_repo_path))?;

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
        repo.create_commit(None, "Test commit", &path_and_files, None)
            .await?;

        // list single file
        let root_path = "data/imgs/5.png";
        let recursive = false;
        let files = repo
            .list_files_from_repo(root_path, None, recursive)
            .await?;
        let expected = ["data/imgs/5.png"];
        assert_eq!(&files, &expected);

        // list single file recursive
        let root_path = "data/mov/6.mov";
        let recursive = true;
        let files = repo
            .list_files_from_repo(root_path, None, recursive)
            .await?;
        let expected = ["data/mov/6.mov"];
        assert_eq!(&files, &expected);

        // list directory
        let root_path = "data";
        let recursive = false;
        let mut files = repo
            .list_files_from_repo(root_path, None, recursive)
            .await?;
        let mut expected = ["data/3.dat", "data/4.mp3"];
        files.sort();
        expected.sort();
        assert_eq!(&files, &expected);

        // list directory recursive
        let root_path = "data";
        let recursive = true;
        let mut files = repo
            .list_files_from_repo(root_path, None, recursive)
            .await?;
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
        let mut files = repo
            .list_files_from_repo(root_path, None, recursive)
            .await?;
        let mut expected = ["1.txt", "2.csv"];
        files.sort();
        expected.sort();
        assert_eq!(&files, &expected);

        // list root recursive
        let root_path: &str = ".";
        let recursive = true;
        let mut files = repo
            .list_files_from_repo(root_path, None, recursive)
            .await?;
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
        let files = repo
            .list_files_from_repo(root_path, None, recursive)
            .await?;
        let expected = Vec::<String>::new();
        assert_eq!(&files, &expected);

        Ok(())
    }
}
