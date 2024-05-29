use super::git_process_wrapping;
use super::git_url::{authenticate_remote_url, is_remote_url, parse_remote_url};
use super::git_xet_repo::GitXetRepo;
use crate::config::XetConfig;
use crate::errors::{GitXetRepoError, Result};
use crate::git_integration::git_commits::atomic_commit_impl;
use crate::git_integration::git_commits::ManifestEntry;
use crate::git_integration::git_user_config::get_user_info_for_commit;
use anyhow::anyhow;
use git2::ObjectType;
use git2::Repository;
use git2::TreeWalkMode;
use git2::TreeWalkResult;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, error, info};
use walkdir::WalkDir;

/// Open the repo using libgit2
pub fn open_libgit2_repo(
    repo_path: impl AsRef<Path> + std::fmt::Debug,
) -> std::result::Result<Arc<Repository>, git2::Error> {
    let repo = Repository::discover(&repo_path).map_err(|e| {
        error!("Error opening git repository from {repo_path:?};");
        e
    })?;

    #[allow(unknown_lints)]
    #[allow(clippy::arc_with_non_send_sync)]
    Ok(Arc::new(repo))
}
pub fn repo_dir_from_repo(repo: &Arc<Repository>) -> PathBuf {
    match repo.workdir() {
        Some(p) => p,
        None => repo.path(), // When it's a bare directory
    }
    .to_path_buf()
}

/// Normalize the path inside of a repo to eliminate components that mess up git2 but are
/// valid paths.  E.g. ./file.dat instead of file.dat.
///
/// Also normalizes /a/b/c to a/b/c, with the first component assumed to be from the start of the repo.
pub fn normalize_repo_path(path: &str) -> String {
    let mut s: String = String::with_capacity(path.len());

    for c in path.split('/') {
        if !(c == "." || c.is_empty()) {
            if !s.is_empty() {
                s.push('/');
            }
            s += c;
        }
    }

    s
}

/// Clone a repo and swallow internal errors, display
/// the reply from server nicely.
pub fn clone_xet_repo_or_display_remote_error_message(
    config: Option<&XetConfig>,
    git_args: &[&str],
    no_smudge: bool,
    base_dir: Option<&PathBuf>,
    allow_stdin: bool,
) -> Result<(String, Option<String>)> {
    let (clone_ret, repo, branch) = clone_xet_repo_impl(
        config,
        git_args,
        no_smudge,
        base_dir,
        false, // no pass through leads to Captured result below
        allow_stdin,
        false, // don't check result internally, will check below
    )?;

    let CloneRet::Captured((status_code, _stdout, stderr)) = clone_ret else {
        unreachable!();
    };

    // clone finished correctly
    if let Some(0) = status_code {
        return Ok((repo, branch));
    }

    // If "remote: .*" exists in the stderr message (which is how git print
    // out the reply from server), capture the reply as "remote".
    //
    // Print "remote" if it exists, otherwise re-print the entire captured stderr.
    let regex = regex::Regex::new(r#"remote:\s*(?P<remote>.*)"#).unwrap();
    if let Some(cap) = regex.captures(&stderr) {
        if let Some(remote_message) = cap.name("remote") {
            eprintln!("{}", remote_message.as_str());
        } else {
            eprintln!("{}", stderr);
        }
    }

    Err(GitXetRepoError::Other("clone repo failed".to_owned()))
}

pub fn clone_xet_repo(
    config: Option<&XetConfig>,
    git_args: &[&str],
    no_smudge: bool,
    base_dir: Option<&PathBuf>,
    pass_through: bool,
    allow_stdin: bool,
    check_result: bool,
) -> Result<(String, Option<String>)> {
    let ret = clone_xet_repo_impl(
        config,
        git_args,
        no_smudge,
        base_dir,
        pass_through,
        allow_stdin,
        check_result,
    );

    ret.map(|(_, repo, branch)| (repo, branch))
}

enum CloneRet {
    #[allow(dead_code)]
    PassThrough(i32), // status_code
    Captured((Option<i32>, String, String)), // status_code, stdout, stderr
}

/// Clone a repo -- just a pass-through to git clone.
/// Return repo name and a branch field if that exists in the remote url.
fn clone_xet_repo_impl(
    config: Option<&XetConfig>,
    git_args: &[&str],
    no_smudge: bool,
    base_dir: Option<&PathBuf>,
    pass_through: bool,
    allow_stdin: bool,
    check_result: bool,
) -> Result<(CloneRet, String, Option<String>)> {
    let mut git_args = git_args.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    // attempt to rewrite URLs with authentication information
    // if config provided

    let mut repo = String::default();
    let mut branch = None;
    if let Some(config) = config {
        for ent in &mut git_args {
            if is_remote_url(ent) {
                (*ent, repo, branch) = parse_remote_url(ent)?;
                *ent = authenticate_remote_url(ent, config)?;
            }
        }
    }
    if let Some(ref br) = branch {
        git_args.extend(vec!["--branch".to_owned(), br.clone()]);
    }

    // First, make sure that everything is properly installed and that the git-xet filter will be run correctly.
    GitXetRepo::write_global_xet_config()?;

    let smudge_arg: Option<&[_]> = if no_smudge {
        Some(&[("XET_NO_SMUDGE", "1")])
    } else {
        None
    };
    let git_args_ref: Vec<&str> = git_args.iter().map(|s| s.as_ref()).collect();

    // Now run git clone, and everything should work fine.
    let ret = if pass_through {
        CloneRet::PassThrough(git_process_wrapping::run_git_passthrough(
            base_dir,
            "clone",
            &git_args_ref,
            check_result,
            allow_stdin,
            smudge_arg,
        )?)
    } else {
        CloneRet::Captured(git_process_wrapping::run_git_captured(
            base_dir,
            "clone",
            &git_args_ref,
            check_result,
            smudge_arg,
        )?)
    };

    Ok((ret, repo, branch))
}

/// Add files to a repo by directly going to the index.  Works on regular or bare repos.  Will not change checked-out
/// files.
///
/// If the branch_name is given, the commit will be added to that branch.  If branch_name is None, than HEAD will be used.
/// If main_branch_name_if_empty_repo is given, then a branch will be created containing only this commit if there are no
/// branches in the repo.
pub fn create_commit(
    repo: &Arc<Repository>,
    branch_name: Option<&str>,
    commit_message: &str,
    files: &[(&str, &[u8])],
    main_branch_name_if_empty_repo: Option<&str>, // If given, make sure the repo has at least one branch
    user_info: Option<(String, String)>,
) -> Result<git2::Oid> {
    let default_branch = main_branch_name_if_empty_repo.unwrap_or("main");

    let branch_name = branch_name.unwrap_or("HEAD");

    let mut _head = None;

    // Create the commit
    let (refname, mut set_head) = {
        if branch_name != "HEAD" {
            (branch_name, false)
        } else if !repo.branches(None)?.any(|_| true) {
            info!("git_wrap:create_commit: Setting HEAD to point to new branch {default_branch}.");
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

    // Retrieve the user's name and email
    let (user_name, user_email) = user_info
        .map(|(sn, se)| (sn.to_owned(), se.to_owned()))
        .unwrap_or_else(|| get_user_info_for_commit(None, repo.clone()));

    let manifest_entries: Vec<_> = files
        .iter()
        .map(|(name, data)| {
            let file = PathBuf::from(normalize_repo_path(name));

            ManifestEntry::Upsert {
                file,
                modeexec: false,
                content: (*data).into(),
                githash_content: None,
            }
        })
        .collect();

    let (refname, new_commit) = atomic_commit_impl(
        repo,
        manifest_entries,
        refname,
        commit_message,
        &user_name,
        &user_email,
        true,
    )?;

    if set_head {
        repo.set_head(&refname)?;
    }

    Ok(new_commit.id())
}

/// Read a file from the repo directly from the index.  If the branch is not given, then HEAD is used.  
pub fn read_file_from_repo(
    repo: &Arc<Repository>,
    file_path: &str,
    branch: Option<&str>,
) -> Result<Option<Vec<u8>>> {
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

    if let Some(blob) = 'a: {
        if let Ok(tree) = commit.tree() {
            if let Ok(entry) = tree.get_path(std::path::Path::new(file_path)) {
                if entry.kind() == Some(git2::ObjectType::Blob) {
                    if let Ok(blob) = repo.find_blob(entry.id()) {
                        break 'a Some(blob);
                    }
                }
            }
        }

        None
    } {
        let ret: Vec<u8> = blob.content().into();

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

/// An entry in a tree -- may be a blob or tree.
#[derive(Clone, Debug, PartialEq)]
pub struct ListObjectEntry {
    pub path: String,
    pub oid: git2::Oid,
    pub size: usize,
    pub is_tree: bool,
}

impl ListObjectEntry {
    pub fn from_tree_entry(
        repo: &Repository,
        prefix: &str,
        entry: &git2::TreeEntry<'_>,
    ) -> Option<Self> {
        let file_name = entry.name().unwrap().to_owned();
        let file_path = Path::new(prefix).join(file_name);
        let name = file_path.to_str().unwrap().to_owned();

        match entry.kind() {
            Some(git2::ObjectType::Blob) => {
                let size = entry.to_object(repo).unwrap().as_blob().unwrap().size();
                Some(ListObjectEntry {
                    path: name,
                    oid: entry.id(),
                    size,
                    is_tree: false,
                })
            }
            Some(git2::ObjectType::Tree) => Some(ListObjectEntry {
                path: name,
                oid: entry.id(),
                size: 0,
                is_tree: true,
            }),
            _ => None,
        }
    }
}

/// List files from a repo below a root path.
/// If root path is "." list files under the repo root.
/// Return a list of file paths relative to the repo root.
/// Return empty list if the root path is not found under the specified branch.
pub fn list_objects(
    repo: &Arc<Repository>,
    prefix: &str,
    branch_or_commit: Option<&str>,
    recursive: bool,
) -> Result<Vec<ListObjectEntry>> {
    const REPO_ROOT_PATH: &str = ".";

    // Resolve HEAD or the specified branch to the corresponding commit
    let commit = match branch_or_commit {
        Some(name_or_oid) => {
            if let Ok(commit) =
                git2::Oid::from_str(name_or_oid).and_then(|oid| repo.find_commit(oid))
            {
                commit
            } else {
                let reference = repo.find_reference(&format!("refs/heads/{}", name_or_oid))?;
                reference.peel_to_commit()?
            }
        }
        None => {
            let Ok(head) = repo.head() else {
                return Ok(vec![]);
            };
            head.peel_to_commit()?
        }
    };

    let prefix = normalize_repo_path(prefix);

    // find the tree entry with name equal to root_path
    let tree = commit.tree()?;

    let (subtree, prefix) = 'a: {
        if prefix.is_empty() {
            break 'a (tree, "");
        }

        if let Ok(entry) = tree.get_path(Path::new(&prefix)) {
            match entry.kind() {
                None => {
                    // entry for path not found and if path is not special case "."
                    if prefix != REPO_ROOT_PATH {
                        return Ok(vec![]);
                    } else {
                        (tree, "")
                    }
                }
                Some(ObjectType::Blob) => {
                    // this is a single file, directly return it
                    let size = entry.to_object(repo).unwrap().as_blob().unwrap().size();
                    let entry = ListObjectEntry {
                        path: prefix.to_owned(),
                        oid: entry.id(),
                        size,
                        is_tree: false,
                    };

                    return Ok(vec![entry]);
                }
                Some(ObjectType::Tree) => (repo.find_tree(entry.id())?, &prefix), // continue to list the subtree
                _ => return Ok(vec![]), // unrecognized kind, return empty list
            }
        } else {
            return Ok(vec![]);
        }
    };

    let mut list = vec![];
    if !recursive {
        // only the direct children of this root_path
        for entry in subtree.iter() {
            if let Some(loe) = ListObjectEntry::from_tree_entry(repo, prefix, &entry) {
                list.push(loe);
            }
        }
    } else {
        // recursively list all children under this root_path
        subtree.walk(TreeWalkMode::PreOrder, |parent, entry| {
            if let Some(git2::ObjectType::Blob) = entry.kind() {
                let dir_prefix = PathBuf::from(prefix).join(parent);
                if let Some(loe) =
                    ListObjectEntry::from_tree_entry(repo, dir_prefix.to_str().unwrap(), entry)
                {
                    list.push(loe);
                }
            }
            TreeWalkResult::Ok
        })?;
    }

    Ok(list)
}

/// Return a list of objects at a specific tree object
pub fn list_objects_at_tree_oid(
    repo: &Arc<Repository>,
    tree_oid: git2::Oid, // The oid of the tree representing that directory.
) -> Result<Vec<ListObjectEntry>> {
    // Returns a sorted list of dir entries, either blobs or directories, given by the tree oid
    let tree = repo.find_tree(tree_oid)?;
    let mut entries = Vec::new();

    for entry in tree.iter() {
        let object = entry.to_object(repo)?;

        let (size, is_tree) = match object.kind() {
            Some(ObjectType::Blob) => (object.as_blob().unwrap().size(), false),
            Some(ObjectType::Tree) => (0, true),
            _ => {
                info!("Git object entry {object:?} in tree {tree:?} has type other than blob or tree.");
                continue; // Skip if it's neither a blob nor a tree
            }
        };

        entries.push(ListObjectEntry {
            path: entry.name().unwrap_or_default().to_owned(),
            oid: entry.id(),
            size,
            is_tree,
        });
    }

    Ok(entries)
}

pub fn list_files_from_repo(
    repo: &Arc<Repository>,
    root_path: &str,
    branch_or_commit: Option<&str>,
    recursive: bool,
) -> Result<Vec<PathBuf>> {
    Ok(list_objects(repo, root_path, branch_or_commit, recursive)?
        .into_iter()
        .filter_map(|e| {
            if !e.is_tree {
                Some(PathBuf::from(e.path))
            } else {
                None
            }
        })
        .collect())
}

/// Walk the repo working directory starting from search_root.
/// Return a list of file paths under the search_root, the
/// file paths are relative to the working dir root.
/// Note that symlinks are ignored because they are difficult to
/// deal with: git deals with the symlink file itself without
/// following the link.
pub fn walk_working_dir(
    work_root: impl AsRef<Path>,
    search_root: impl AsRef<Path>,
    recursive: bool,
) -> anyhow::Result<Vec<PathBuf>> {
    // check existence
    let work_root = work_root.as_ref();
    let search_root = search_root.as_ref();

    if !work_root.exists() || !search_root.exists() {
        return Ok(vec![]);
    }

    // get absolute paths and check if search below work root
    let work_root = work_root.canonicalize()?;
    let search_root = search_root.canonicalize()?;

    if !search_root.starts_with(&work_root) {
        return Err(anyhow!(
            "Path {search_root:?} is outside repository at {work_root:?}"
        ));
    }

    // ignore .git folder, .gitignore, .gitattributes
    if is_git_special_files(
        search_root
            .file_name()
            .unwrap_or_default()
            .to_str()
            .unwrap_or_default(),
    ) {
        return Ok(vec![]);
    }

    // directly return single file itself
    if search_root.is_file() {
        return Ok(vec![search_root.strip_prefix(work_root)?.to_owned()]);
    }

    // now search_root is a regular directory, walk below it

    let ret: Vec<_> = WalkDir::new(&search_root)
        .follow_links(false)
        .max_depth(if recursive { usize::MAX } else { 1 })
        .into_iter()
        .filter_entry(|e| !is_git_special_files(e.file_name().to_str().unwrap_or_default()))
        .flatten()
        .filter(|e| {
            e.file_type().is_file()
                && !is_git_special_files(e.file_name().to_str().unwrap_or_default())
        })
        .filter_map(|e| {
            e.path()
                .strip_prefix(&work_root)
                .map_or(None, |p| Some(p.to_owned()))
        })
        .collect();

    Ok(ret)
}

/// Filter out file paths that are not in the repo index (untracked files).
pub fn filter_files_from_index(
    files: &[PathBuf],
    repo: Arc<Repository>,
) -> anyhow::Result<Vec<PathBuf>> {
    let index = repo.index()?;

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

pub fn is_git_special_files(path: &str) -> bool {
    matches!(path, ".git" | ".gitignore" | ".gitattributes" | ".xet")
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
    use crate::git_integration::{
        git_process_wrapping::run_git_captured, git_repo_plumbing::open_libgit2_repo,
        git_repo_test_tools::TestRepo,
    };
    use serial_test::serial;
    use std::env::set_current_dir;
    use tempfile::TempDir;

    // macro to make a PathBuf from something
    macro_rules! pb {
        ($segment:expr) => {{
            PathBuf::from($segment)
        }};
    }

    #[test]
    fn test_direct_repo_read_write_branches() -> anyhow::Result<()> {
        // Create a temporary directory
        let tmp_repo = TempDir::new().unwrap();
        let tmp_repo_path = tmp_repo.path().to_path_buf();

        let _ = run_git_captured(Some(&tmp_repo_path), "init", &["--bare"], true, None)?;

        let repo = open_libgit2_repo(&tmp_repo_path)?;

        let file_1 = "Random Content 1".as_bytes();
        let file_2 = "Random Content 2".as_bytes();
        let file_2b = "Random Content 2b".as_bytes();
        let file_2c = "Random Content 2c".as_bytes();
        let file_3 = "Random Content 3".as_bytes();

        create_commit(
            &repo,
            None,
            "Test commit",
            &[("file_1.txt", file_1), ("file_2.txt", file_2)],
            None,
            None,
        )?;

        // Make sure that we can get those back
        let file_1_read = read_file_from_repo(&repo, "file_1.txt", None)?.unwrap();
        let file_2_read = read_file_from_repo(&repo, "file_2.txt", None)?.unwrap();

        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2, file_2_read);

        // Make sure we can overwrite things correctly.
        create_commit(
            &repo,
            None,
            "Test commit updated",
            &[("file_2.txt", file_2b)],
            None,
            None,
        )?;
        let file_1_read = read_file_from_repo(&repo, "file_1.txt", None)?.unwrap();
        let file_2_read = read_file_from_repo(&repo, "file_2.txt", None)?.unwrap();

        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2b, file_2_read);

        // Now write to a specified branch.  This branch doesn't exist, so it should create a new branch off of main
        create_commit(
            &repo,
            Some("my_branch"),
            "Test commit",
            &[("file_3.txt", file_3)],
            None,
            None,
        )?;

        // Read this off of HEAD
        let file_1_read = read_file_from_repo(&repo, "file_1.txt", None)?.unwrap();
        let file_2_read = read_file_from_repo(&repo, "file_2.txt", None)?.unwrap();
        let file_3_read = read_file_from_repo(&repo, "file_3.txt", Some("my_branch"))?.unwrap();
        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2b, file_2_read);
        assert_eq!(file_3, file_3_read);

        // Read this off of the branch name
        let file_1_read = read_file_from_repo(&repo, "file_1.txt", Some("my_branch"))?.unwrap();
        let file_2_read = read_file_from_repo(&repo, "file_2.txt", Some("my_branch"))?.unwrap();
        let file_3_read = read_file_from_repo(&repo, "file_3.txt", Some("my_branch"))?.unwrap();
        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2b, file_2_read);
        assert_eq!(file_3, file_3_read);

        // Make sure main doesn't change
        let file_1_read = read_file_from_repo(&repo, "file_1.txt", Some("main"))?.unwrap();
        let file_2_read = read_file_from_repo(&repo, "file_2.txt", Some("main"))?.unwrap();
        let file_3_query = read_file_from_repo(&repo, "file_3.txt", Some("main"))?;
        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2b, file_2_read);
        assert!(file_3_query.is_none());

        // Write to main, overwrite
        create_commit(
            &repo,
            Some("main"),
            "Test commit",
            &[("file_2.txt", file_2c)],
            None,
            None,
        )?;
        let file_1_read = read_file_from_repo(&repo, "file_1.txt", Some("main"))?.unwrap();
        let file_2_read = read_file_from_repo(&repo, "file_2.txt", Some("main"))?.unwrap();
        let file_3_query = read_file_from_repo(&repo, "file_3.txt", Some("main"))?;
        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2c, file_2_read);
        assert!(file_3_query.is_none());

        Ok(())
    }

    #[test]
    fn test_repo_respect_delete() -> anyhow::Result<()> {
        // Create a temporary directory
        let tmp_repo = TempDir::new().unwrap();
        let tmp_repo_path = tmp_repo.path().to_path_buf();
        let tmp_repo_1_path = tmp_repo.path().join("repo_1");
        std::fs::create_dir_all(&tmp_repo_1_path)?;

        let _ = run_git_captured(Some(&tmp_repo_1_path), "init", &["--bare"], true, None)?;

        let repo_1 = open_libgit2_repo(&tmp_repo_1_path)?;

        let file_1 = "Random Content 1".as_bytes();
        let file_2 = "Random Content 2".as_bytes();
        let file_2b = "Random Content 2b".as_bytes();
        let file_3 = "Random Content 3".as_bytes();
        let file_4 = "Random Content 4".as_bytes();

        create_commit(
            &repo_1,
            None,
            "Test commit",
            &[
                ("file_1.txt", file_1),
                ("file_2.txt", file_2),
                ("file_3.txt", file_3),
            ],
            None,
            None,
        )?;
        let file_1_read = read_file_from_repo(&repo_1, "file_1.txt", None)?.unwrap();
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
        assert!(read_file_from_repo(&repo_1, "file_1.txt", None)?.is_none());

        let file_2_read = read_file_from_repo(&repo_1, "file_2.txt", None)?.unwrap();
        assert_eq!(file_2b, file_2_read);

        let file_3_read = read_file_from_repo(&repo_1, "file_3.txt", None)?.unwrap();
        assert_eq!(file_3, file_3_read);

        let file_4_read = read_file_from_repo(&repo_1, "file_4.txt", None)?.unwrap();
        assert_eq!(file_4, file_4_read);
        Ok(())
    }

    #[test]
    fn test_repo_read_write_through_mirror_push() -> anyhow::Result<()> {
        // Create a temporary directory
        let tmp_repo = TempDir::new().unwrap();
        let tmp_repo_path = tmp_repo.path().to_path_buf();
        let tmp_repo_1_path = tmp_repo.path().join("repo_1");
        std::fs::create_dir_all(&tmp_repo_1_path)?;

        let _ = run_git_captured(Some(&tmp_repo_1_path), "init", &["--bare"], true, None)?;

        let repo_1 = open_libgit2_repo(&tmp_repo_1_path)?;

        let file_1 = "Random Content 1".as_bytes();
        let file_2 = "Random Content 2".as_bytes();

        create_commit(
            &repo_1,
            None,
            "Test commit",
            &[("file_1.txt", file_1)],
            None,
            None,
        )?;
        let file_1_read = read_file_from_repo(&repo_1, "file_1.txt", None)?.unwrap();
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

        let repo_2 = open_libgit2_repo(&tmp_repo_2_path)?;

        create_commit(
            &repo_2,
            None,
            "Test commit 2",
            &[("file_2.txt", file_2)],
            None,
            None,
        )?;

        // Make sure that we can get those back (doesn't have to be bare here)
        let file_1_read = read_file_from_repo(&repo_2, "file_1.txt", None)?.unwrap();
        let file_2_read = read_file_from_repo(&repo_2, "file_2.txt", None)?.unwrap();

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
        let file_1_read = read_file_from_repo(&repo_1, "file_1.txt", None)?.unwrap();
        let file_2_read = read_file_from_repo(&repo_1, "file_2.txt", None)?.unwrap();

        assert_eq!(file_1, file_1_read);
        assert_eq!(file_2, file_2_read);

        Ok(())
    }

    #[test]
    fn test_list_repo_files() -> anyhow::Result<()> {
        // Create a temporary directory
        let tmp_repo = TempDir::new().unwrap();
        let tmp_repo_path = tmp_repo.path().to_path_buf();

        let _ = run_git_captured(Some(&tmp_repo_path), "init", &["--bare"], true, None)?;

        let repo = open_libgit2_repo(&tmp_repo_path)?;

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
        create_commit(&repo, None, "Test commit", &path_and_files, None, None)?;

        // list single file
        let root_path = "data/imgs/5.png";
        let recursive = false;
        let files = list_files_from_repo(&repo, root_path, None, recursive)?;
        let expected = [pb!("data/imgs/5.png")];
        assert_eq!(&files, &expected);

        // list single file recursive
        let root_path = "data/mov/6.mov";
        let recursive = true;
        let files = list_files_from_repo(&repo, root_path, None, recursive)?;
        let expected = [pb!("data/mov/6.mov")];
        assert_eq!(&files, &expected);

        // list directory
        let root_path = "data";
        let recursive = false;
        let mut files = list_files_from_repo(&repo, root_path, None, recursive)?;
        let mut expected = [pb!("data/3.dat"), pb!("data/4.mp3")];
        files.sort();
        expected.sort();
        assert_eq!(&files, &expected);

        // list directory recursive
        let root_path = "data";
        let recursive = true;
        let mut files = list_files_from_repo(&repo, root_path, None, recursive)?;
        let mut expected = [
            pb!("data/3.dat"),
            pb!("data/4.mp3"),
            pb!("data/imgs/5.png"),
            pb!("data/mov/6.mov"),
        ];
        files.sort();
        expected.sort();
        assert_eq!(&files, &expected);

        // list root
        let root_path: &str = ".";
        let recursive = false;
        let mut files = list_files_from_repo(&repo, root_path, None, recursive)?;
        let mut expected = [pb!("1.txt"), pb!("2.csv")];
        files.sort();
        expected.sort();
        assert_eq!(&files, &expected);

        // list root recursive
        let root_path: &str = ".";
        let recursive = true;
        let mut files = list_files_from_repo(&repo, root_path, None, recursive)?;
        let mut expected = [
            pb!("1.txt"),
            pb!("2.csv"),
            pb!("data/3.dat"),
            pb!("data/4.mp3"),
            pb!("data/imgs/5.png"),
            pb!("data/mov/6.mov"),
        ];
        files.sort();
        expected.sort();
        assert_eq!(&files, &expected);

        // list invalid path
        let root_path: &str = "xx";
        let recursive = false;
        let files = list_files_from_repo(&repo, root_path, None, recursive)?;
        let expected = Vec::<PathBuf>::new();
        assert_eq!(&files, &expected);

        Ok(())
    }

    #[test]
    #[serial(set_curdir)] // tests messing up with 'set_current_dir' run serial
    #[ignore] // however, tests with neither #[serial] nor #[parallel] may run at any time, thus to ignore in CI to avoid occasional errors.
    fn test_walk_working_dir() -> anyhow::Result<()> {
        let repo = TestRepo::new()?;

        // Prepare a repo with files in the below structure.
        let paths = [
            "1.txt",
            "2.csv",
            "data/3.dat",
            "data/4.mp3",
            "data/imgs/5.png",
            "data/mov/6.mov",
        ];

        for p in paths {
            repo.write_file(p, 0, 100)?;
        }

        let work_root = repo.repo.repo_dir;

        let assert_search_result = |search_root: &str,
                                    recursive: bool,
                                    mut expected: Vec<PathBuf>|
         -> anyhow::Result<()> {
            let mut files = walk_working_dir(&work_root, search_root, recursive)?;
            files.sort();
            expected.sort();
            assert_eq!(&files, &expected);
            Ok(())
        };

        let assert_search_is_err = |search_root: &str, recursive: bool| {
            assert!(walk_working_dir(&work_root, search_root, recursive).is_err())
        };

        // tests under the working directory root
        {
            set_current_dir(&work_root)?;

            // list single file
            assert_search_result("data/imgs/5.png", false, vec![pb!("data/imgs/5.png")])?;
            assert_search_result("data/mov/6.mov", true, vec![pb!("data/mov/6.mov")])?;

            // list directory
            assert_search_result("data", false, vec![pb!("data/3.dat"), pb!("data/4.mp3")])?;

            // list directory recursive
            assert_search_result(
                "data",
                true,
                vec![
                    pb!("data/3.dat"),
                    pb!("data/4.mp3"),
                    pb!("data/imgs/5.png"),
                    pb!("data/mov/6.mov"),
                ],
            )?;

            // list root
            assert_search_result(".", false, vec![pb!("1.txt"), pb!("2.csv")])?;

            // list root recursive
            assert_search_result(
                ".",
                true,
                vec![
                    pb!("1.txt"),
                    pb!("2.csv"),
                    pb!("data/3.dat"),
                    pb!("data/4.mp3"),
                    pb!("data/imgs/5.png"),
                    pb!("data/mov/6.mov"),
                ],
            )?;

            // list invalid path
            assert_search_result("xx", false, vec![])?;

            // path outside of the working directory
            assert_search_is_err("../", true);
        }

        // tests in a sub-directory
        {
            set_current_dir(&work_root.join("data"))?;

            // list single file
            assert_search_result("3.dat", false, vec![pb!("data/3.dat")])?;
            assert_search_result("imgs/5.png", true, vec![pb!("data/imgs/5.png")])?;
            assert_search_result("../1.txt", true, vec![pb!("1.txt")])?;

            // list directory
            assert_search_result("imgs", false, vec![pb!("data/imgs/5.png")])?;
            assert_search_result("..", false, vec![pb!("1.txt"), pb!("2.csv")])?;
            assert_search_result("./", false, vec![pb!("data/3.dat"), pb!("data/4.mp3")])?;

            // list directory recursive
            assert_search_result(
                ".",
                true,
                vec![
                    pb!("data/3.dat"),
                    pb!("data/4.mp3"),
                    pb!("data/imgs/5.png"),
                    pb!("data/mov/6.mov"),
                ],
            )?;

            // complicated relative path
            assert_search_result("../data/imgs", true, vec![pb!("data/imgs/5.png")])?;

            // invalid file
            assert_search_result("1.txt", false, vec![])?;

            // path outside of the working directory
            assert_search_is_err("../../", true);
        }

        Ok(())
    }

    #[test]
    #[serial(set_curdir)] // tests messing up with 'set_current_dir' run serial
    #[ignore] // however, tests with neither #[serial] nor #[parallel] may run at any time, thus to ignore in CI to avoid occasional errors.
    fn test_filtered_untracked_files() -> anyhow::Result<()> {
        let repo = TestRepo::new()?;

        // Prepare a repo with files in the below structure.
        let paths = [
            "1.txt",
            "2.csv",
            "data/3.dat",
            "data/4.mp3",
            "data/imgs/5.png",
            "data/mov/6.mov",
        ];

        for p in paths {
            repo.write_file(p, 0, 100)?;
        }

        let work_root = repo.repo.repo_dir.clone();
        set_current_dir(&work_root)?;

        // check in the files
        repo.repo.run_git_checked_in_repo("add", &["."])?;
        repo.repo
            .run_git_checked_in_repo("commit", &["-m", "Add many files"])?;

        let assert_filtered_search_result = |search_root: &str,
                                             recursive: bool,
                                             filtered_out: Vec<PathBuf>|
         -> anyhow::Result<()> {
            // make sure untracked files are indeed found
            let files = walk_working_dir(&work_root, search_root, recursive)?;
            for f in &filtered_out {
                assert!(files.contains(f));
            }
            // and later filtered out
            let files = filter_files_from_index(&files, repo.repo.repo.clone())?;
            for f in &filtered_out {
                assert!(!files.contains(f));
            }
            Ok(())
        };

        // now add some untracked files to the working directory
        repo.write_file("hello.txt", 0, 100)?;
        repo.write_file("data/7.vmo", 0, 100)?;
        repo.write_file("amo/go", 0, 100)?;

        // tests under the working directory root
        {
            set_current_dir(&work_root)?;

            assert_filtered_search_result(".", false, vec![pb!("hello.txt")])?;
            assert_filtered_search_result("data", false, vec![pb!("data/7.vmo")])?;
            assert_filtered_search_result(
                ".",
                true,
                vec![pb!("hello.txt"), pb!("data/7.vmo"), pb!("amo/go")],
            )?;
        }

        // tests in a sub-directory
        {
            set_current_dir(&work_root.join("data"))?;

            assert_filtered_search_result(".", false, vec![pb!("data/7.vmo")])?;
            assert_filtered_search_result("../amo", true, vec![pb!("amo/go")])?;
        }

        Ok(())
    }
}
