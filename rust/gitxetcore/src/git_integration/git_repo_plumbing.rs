use super::git_process_wrapping;
use super::git_url::{authenticate_remote_url, is_remote_url, parse_remote_url};
use super::git_xet_repo::GitXetRepo;
use crate::config::XetConfig;
use crate::errors::Result;
use crate::git_integration::git_commits::atomic_commit_impl;
use crate::git_integration::git_commits::ManifestEntry;
use crate::git_integration::git_process_wrapping::run_git_captured_raw;
use git2::ObjectType;
use git2::Oid;
use git2::Repository;
use git2::TreeWalkMode;
use git2::TreeWalkResult;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, info};

/// Open the repo using libgit2
pub fn open_libgit2_repo(
    repo_path: Option<&Path>,
) -> std::result::Result<Arc<Repository>, git2::Error> {
    let repo = match repo_path {
        Some(path) => {
            if *path == PathBuf::default() {
                Repository::open_from_env()?
            } else {
                Repository::discover(path)?
            }
        }
        None => Repository::open_from_env()?,
    };

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

/// Clone a repo -- just a pass-through to git clone.
/// Return repo name and a branch field if that exists in the remote url.
pub fn clone_xet_repo(
    config: Option<&XetConfig>,
    git_args: &[&str],
    no_smudge: bool,
    base_dir: Option<&PathBuf>,
    pass_through: bool,
    allow_stdin: bool,
    check_result: bool,
) -> Result<(String, Option<String>)> {
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
    if pass_through {
        git_process_wrapping::run_git_passthrough(
            base_dir,
            "clone",
            &git_args_ref,
            check_result,
            allow_stdin,
            smudge_arg,
        )?;
    } else {
        git_process_wrapping::run_git_captured(
            base_dir,
            "clone",
            &git_args_ref,
            check_result,
            smudge_arg,
        )?;
    }

    Ok((repo, branch))
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
) -> Result<()> {
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

    let config = git2::Config::open_default()?;

    // Retrieve the user's name and email
    let user_name = config.get_string("user.name")?;
    let user_email = config.get_string("user.email")?;

    let (refname, _) = atomic_commit_impl(
        repo,
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
            let git_out = run_git_captured_raw(
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
            let git_out = run_git_captured_raw(
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

/// List files from a repo below a root path.
/// If root path is "." list files under the repo root.
/// Return a list of file paths relative to the repo root.
/// Return empty list if the root path is not found under the specified branch.
pub fn list_files_from_repo(
    repo: &Arc<Repository>,
    root_path: &str,
    branch: Option<&str>,
    recursive: bool,
) -> Result<Vec<String>> {
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

#[cfg(test)]
mod git_repo_tests {
    use super::*;
    use tempfile::TempDir;

    use crate::git_integration::{
        git_process_wrapping::run_git_captured, git_repo_plumbing::open_libgit2_repo,
    };

    #[test]
    fn test_direct_repo_read_write_branches() -> anyhow::Result<()> {
        // Create a temporary directory
        let tmp_repo = TempDir::new().unwrap();
        let tmp_repo_path = tmp_repo.path().to_path_buf();

        let _ = run_git_captured(Some(&tmp_repo_path), "init", &["--bare"], true, None)?;

        let repo = open_libgit2_repo(Some(&tmp_repo_path))?;

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

        let repo_1 = open_libgit2_repo(Some(&tmp_repo_1_path))?;

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

        let repo_1 = open_libgit2_repo(Some(&tmp_repo_1_path))?;

        let file_1 = "Random Content 1".as_bytes();
        let file_2 = "Random Content 2".as_bytes();

        create_commit(
            &repo_1,
            None,
            "Test commit",
            &[("file_1.txt", file_1)],
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

        let repo_2 = open_libgit2_repo(Some(&tmp_repo_2_path))?;

        create_commit(
            &repo_2,
            None,
            "Test commit 2",
            &[("file_2.txt", file_2)],
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

        let repo = open_libgit2_repo(Some(&tmp_repo_path))?;

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
        create_commit(&repo, None, "Test commit", &path_and_files, None)?;

        // list single file
        let root_path = "data/imgs/5.png";
        let recursive = false;
        let files = list_files_from_repo(&repo, root_path, None, recursive)?;
        let expected = ["data/imgs/5.png"];
        assert_eq!(&files, &expected);

        // list single file recursive
        let root_path = "data/mov/6.mov";
        let recursive = true;
        let files = list_files_from_repo(&repo, root_path, None, recursive)?;
        let expected = ["data/mov/6.mov"];
        assert_eq!(&files, &expected);

        // list directory
        let root_path = "data";
        let recursive = false;
        let mut files = list_files_from_repo(&repo, root_path, None, recursive)?;
        let mut expected = ["data/3.dat", "data/4.mp3"];
        files.sort();
        expected.sort();
        assert_eq!(&files, &expected);

        // list directory recursive
        let root_path = "data";
        let recursive = true;
        let mut files = list_files_from_repo(&repo, root_path, None, recursive)?;
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
        let mut files = list_files_from_repo(&repo, root_path, None, recursive)?;
        let mut expected = ["1.txt", "2.csv"];
        files.sort();
        expected.sort();
        assert_eq!(&files, &expected);

        // list root recursive
        let root_path: &str = ".";
        let recursive = true;
        let mut files = list_files_from_repo(&repo, root_path, None, recursive)?;
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
        let files = list_files_from_repo(&repo, root_path, None, recursive)?;
        let expected = Vec::<String>::new();
        assert_eq!(&files, &expected);

        Ok(())
    }
}
