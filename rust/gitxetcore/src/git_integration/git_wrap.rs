use crate::constants::MINIMUM_GIT_VERSION;
use crate::errors::GitXetRepoError;
use crate::errors::Result;
use git2::Repository;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;
use tracing::error;
use tracing::{debug, info};
use version_compare::Version;

lazy_static! {
    static ref GIT_EXECUTABLE: String = {
        match std::env::var_os("XET_GIT_EXECUTABLE") {
            Some(v) => v.to_str().unwrap().to_owned(),
            None => String::default(),
        }
    };
}

pub fn get_git_executable() -> &'static str {
    if !GIT_EXECUTABLE.is_empty() {
        &(GIT_EXECUTABLE)
    } else {
        "git"
    }
}

/// Sets up the git command to run how the caller chooses.
fn spawn_git_command(
    base_directory: Option<&PathBuf>,
    command: &str,
    args: &[&str],
    env: Option<&[(&str, &str)]>,
    capture_output: bool,
) -> Result<Child> {
    let git_executable = get_git_executable();

    let mut cmd = Command::new(git_executable);

    cmd.arg(command).args(args);
    if let Some(env) = env {
        for (k, v) in env.iter() {
            cmd.env(k, v);
        }
    }

    if let Some(dir) = base_directory {
        debug_assert!(dir.exists());

        cmd.current_dir(dir);
    }
    debug!(
        "Calling git, working dir={:?}, command = git {:?} {:?}, env = {:?}",
        match base_directory {
            Some(bd) => bd.to_owned(),
            None => std::env::current_dir()?,
        },
        &command,
        &args,
        &env
    );

    // Disable stdin so it doesn't hang silently in the background.
    cmd.stdin(std::process::Stdio::piped());

    // Set up the command to capture or pass through stdout and stderr
    if capture_output {
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
    } else {
        cmd.stdout(std::process::Stdio::inherit());
        cmd.stderr(std::process::Stdio::inherit());
    }

    // Spawn the child
    let mut child = cmd.spawn()?;

    // Immediately drop the writing end of the stdin pipe; if git attempts to wait on stdin, it will cause an error.
    drop(child.stdin.take());

    Ok(child)
}

/// Calls git directly, piping both stdout and stderr through.  
///
/// The command is run in the directory base_directory.  On nonzero exit status, an error is
/// returned.
#[tracing::instrument(skip_all, err, fields(command = command, ?args))]
pub fn run_git_captured(
    base_directory: Option<&PathBuf>,
    command: &str,
    args: &[&str],
    check_result: bool,
    env: Option<&[(&str, &str)]>,
) -> Result<(Option<i32>, String, String)> {
    // Block use of credential manager for this bit.
    let env: Vec<(&str, &str)> = match env {
        Some(d) => {
            let mut dv = Vec::from(d);
            dv.push(("GCM_INTERACTIVE", "never"));
            dv
        }
        None => vec![("GCM_INTERACTIVE", "never")],
    };

    let child = spawn_git_command(base_directory, command, args, Some(&env), true)?;

    let out = child.wait_with_output()?;

    let res_stdout = std::str::from_utf8(&out.stdout[..]).unwrap_or("<Binary Data>");
    let res_stderr = std::str::from_utf8(&out.stderr[..]).unwrap_or("<Binary Data>");

    debug!(
        "Git return: status = {:?}, stdout = {:?}, stderr = {:?}.",
        &out.status,
        if res_stdout.len() > 64 {
            format!("{} ... [len={:?}]", &res_stdout[..64], res_stdout.len())
        } else {
            res_stdout.to_owned()
        },
        if res_stderr.len() > 64 {
            format!("{} ... [len={:?}]", &res_stderr[..64], res_stderr.len())
        } else {
            res_stderr.to_owned()
        }
    );

    let ret = (
        out.status.code(),
        res_stdout.trim().to_string(),
        res_stderr.trim().to_string(),
    );

    if check_result {
        if let Some(0) = ret.0 {
            Ok(ret)
        } else {
            Err(GitXetRepoError::Other(format!(
                "Error running git command: git {:?} {:?} err_code={:?}, stdout=\"{:?}\", stderr=\"{:?}\"",
                &command, args.iter().map(|s| format!("\"{s}\"")).join(" "), &ret.0, &ret.1, &ret.2
            )))
        }
    } else {
        Ok(ret)
    }
}

/// Calls git directly, letting stdout and stderr through to the console
/// (user will see directly see the git output).
///
/// The command is run in the directory base_directory.  On nonzero exit status, an error is
/// returned.
#[tracing::instrument(skip_all, err, fields(command = command, ?args))]
pub fn run_git_passthrough(
    base_directory: Option<&PathBuf>,
    command: &str,
    args: &[&str],
    check_result: bool,
    env: Option<&[(&str, &str)]>,
) -> Result<i32> {
    let mut child = spawn_git_command(base_directory, command, args, env, false)?;

    let status = child.wait()?;

    let ret = status.code();

    match ret {
        Some(0) => Ok(0),
        Some(r) => {
            if check_result {
                Err(GitXetRepoError::Other(format!(
                    "git command returned non-zero exit code: git {:?} {:?} err_code={:?}",
                    &command,
                    args.iter().map(|s| format!("\"{s}\"")).join(" "),
                    &ret
                )))
            } else {
                Ok(r)
            }
        }
        _ => Err(GitXetRepoError::Other(format!(
            "Unknown error running git command: git {:?} {:?}",
            &command,
            args.iter().map(|s| format!("\"{s}\"")).join(" ")
        ))),
    }
}

/// Returns true if the path is a bare repo
pub fn is_bare_repo(start_path: Option<PathBuf>) -> Result<bool> {
    let start_path = match start_path {
        Some(p) => p,
        None => std::env::current_dir()?,
    };
    // check if this is a bare repo
    let capture = run_git_captured(
        Some(&start_path),
        "rev-parse",
        &["--is-bare-repository"],
        false,
        None,
    );
    if let Ok((_, stdout, _)) = capture {
        Ok(stdout == "true")
    } else {
        Ok(false)
    }
}

/// Returns the path of the repository we're operating in.  
///
/// If start_path is given, begin the search from there; otherwise start from the current working directory.
/// If return_gitdir is true, then return the git directory (typically .git/); otherwise, return the top
/// directory of the repo
///
/// If this is a bare repo, return_gitdir is irrelevant; both return_gitdir == true or false
/// will return the same path.
pub fn resolve_repo_path(
    start_path: Option<PathBuf>,
    return_gitdir: bool,
) -> Result<Option<PathBuf>> {
    let start_path = match start_path {
        Some(p) => p,
        None => std::env::current_dir()?,
    };

    // --show-toplevel is fatal for a bare repo
    let is_bare = is_bare_repo(Some(start_path.clone()))?;

    let (err_code, stdout, stderr) = run_git_captured(
        Some(&start_path),
        "rev-parse",
        &[if return_gitdir || is_bare {
            "--git-dir"
        } else {
            "--show-toplevel"
        }],
        false,
        None,
    )?;

    if let Some(0) = err_code {
        let repo_path = PathBuf::from_str(stdout.trim()).unwrap();

        let repo_path = if repo_path.is_absolute() {
            repo_path
        } else {
            start_path.join(repo_path)
        };

        info!("Resolved git repo directory to {:?}.", &repo_path);
        debug_assert!(repo_path.exists());
        Ok(Some(repo_path))
    } else {
        info!(
            "Resolving git repo failed with error code {:?}, stdout = {:?}, stderr = {:?}.",
            &err_code, &stdout, &stderr
        );
        Ok(None)
    }
}

/// Returns the top level path of the repository we're operating in.  
///
/// If start_path is given, begin the search from there; otherwise start from the current working directory.
pub fn get_repo_path(start_path: Option<PathBuf>) -> Result<Option<PathBuf>> {
    resolve_repo_path(start_path, false)
}

/// Returns the git directory of the repository we're operating in.  
///
/// If start_path is given, begin the search from there; otherwise start from the current working directory.
pub fn get_git_path(start_path: Option<PathBuf>) -> Result<Option<PathBuf>> {
    resolve_repo_path(start_path, true)
}
/// Given a repo directory, determine the git path reliably.
pub fn get_git_dir_from_repo_path(repo_path: &PathBuf) -> Result<PathBuf> {
    let git_path = PathBuf::from_str(
        &run_git_captured(Some(repo_path), "rev-parse", &["--git-dir"], true, None)?.1,
    )
    .unwrap();

    Ok(if git_path.is_absolute() {
        git_path
    } else {
        repo_path.join(git_path)
    })
}

// Version checking information.

lazy_static! {
    static ref GIT_NO_VERSION_CHECK: bool = {
        match std::env::var_os("XET_NO_GIT_VERSION_CHECK") {
            Some(v) => v != "0",
            None => false,
        }
    };
}

lazy_static! {
    static ref GIT_VERSION_REGEX: Regex = Regex::new(r"^.*version ([0-9\\.a-zA-Z]+).*$").unwrap();
}

// Tools to retrieve and check the git version.
fn get_git_version() -> Result<String> {
    let raw_version_string = run_git_captured(None, "--version", &[], true, None)?.1;

    let captured_text = match GIT_VERSION_REGEX.captures(&raw_version_string) {
        Some(m) => m,
        None => {
            return Err(GitXetRepoError::Other(format!(
                "Error: cannot parse version string {raw_version_string:?}."
            )));
        }
    };

    let version = captured_text.get(1).unwrap().as_str();

    Ok(version.to_owned())
}

fn verify_git_version(version: &str) -> bool {
    let vv_test = match Version::from(version) {
        Some(v) => v,
        None => {
            error!("Could not parse \"{}\" as a version string.", version);
            return false;
        }
    };

    let vv_min = Version::from(MINIMUM_GIT_VERSION).unwrap();

    if vv_test >= vv_min {
        info!("Current git version {:?} acceptable.", &version);
        true
    } else {
        error!(
            "Git version {:?} does not meet minimum requirements.",
            &version
        );
        false
    }
}

lazy_static! {
    static ref GIT_VERSION_CHECK_PASSED: bool = {
        if *GIT_NO_VERSION_CHECK {
            true
        } else {
            match get_git_version() {
                Err(_) => false,
                Ok(v) => verify_git_version(&v),
            }
        }
    };
}

pub fn perform_git_version_check() -> Result<()> {
    if *GIT_VERSION_CHECK_PASSED {
        Ok(())
    } else {
        // There's a reason it's wrong, but perform the version checking again in case there's an error or other issue.
        let version = get_git_version()?;
        if verify_git_version(&version) {
            Ok(())
        } else {
            Err(GitXetRepoError::Other(format!("Only git version 2.29 or later is compatible with git-xet.  Please upgrade your version of git. (Installed version = {}", &version)))
        }
    }
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
    // Create blobs for the files
    let file_oids: Vec<_> = files
        .iter()
        .map(|(file_name, data)| {
            let blob_oid = repo.blob(data)?;
            Ok((
                file_name.to_owned().trim_end_matches('/'),
                blob_oid,
                data.len(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    info!(
        "git_wrap:create_commit: Creating new commit with {} new files.",
        file_oids.len()
    );

    // Create a tree from the blobs; do this in memory, then do it one by one.
    let mut index = git2::Index::new()?;

    for (file_name, file_oid, data_len) in file_oids {
        let entry = git2::IndexEntry {
            path: file_name.as_bytes().into(),
            id: file_oid,
            file_size: data_len as u32,
            mode: 0o100644, // represents a blob (file)
            dev: 0,
            ino: 0,
            uid: 0,
            gid: 0,
            flags: 0,
            flags_extended: 0,
            mtime: git2::IndexTime::new(0, 0),
            ctime: git2::IndexTime::new(0, 0),
        };
        index.add(&entry)?;
    }

    // Now, write the whole index to the repo
    let tree_oid = index.write_tree_to(repo)?;
    debug!("git_wrap:create_commit: tree = {tree_oid:?}.");
    let tree = repo.find_tree(tree_oid)?;

    // Create the commit
    let (update_ref, parent_ref, set_head) = {
        if let Some(bn) = branch_name {
            if let Ok(branch) = repo.find_branch(bn, git2::BranchType::Local) {
                (
                    format!("refs/heads/{bn}"),
                    Some(branch.into_reference()),
                    false,
                )
            } else {
                (format!("refs/heads/{bn}"), repo.head().ok(), false)
            }
        } else if let (Some(new_br), false) = (
            main_branch_name_if_empty_repo,
            repo.branches(None)?.any(|_| true),
        ) {
            info!("git_wrap:create_commit: Setting HEAD to point to new branch {new_br}.");
            (format!("refs/heads/{new_br}"), repo.head().ok(), true)
        } else {
            ("HEAD".to_owned(), repo.head().ok(), false)
        }
    };
    debug!(
        "git_wrap:create_commit: update_ref = {update_ref:?}, parent_ref = {:?}.",
        parent_ref.as_ref().map(|p| p.name())
    );

    let parent_commit;

    let parents = if let Some(pr) = parent_ref {
        parent_commit = repo.find_commit(pr.target().unwrap())?;
        info!("git_wrap:create_commit: creating commit with parent commit {parent_commit:?}.");
        vec![&parent_commit]
    } else {
        info!(
            "git_wrap:create_commit: creating commit with no parents as repo appears to be empty."
        );
        Vec::new()
    };

    let signature = repo.signature()?;

    let commit_oid = repo.commit(
        Some(&update_ref),
        &signature,
        &signature,
        commit_message,
        &tree,
        &parents,
    )?;

    info!("git_wrap:create_commit: New commit created with oid {commit_oid}.");

    if set_head {
        let commit = repo.find_commit(commit_oid)?;
        let branch = repo.branch(main_branch_name_if_empty_repo.unwrap(), &commit, true)?;
        repo.set_head(branch.get().name().unwrap())?;
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
    let mut commit = match branch {
        Some(branch_name) => {
            let reference = repo.find_reference(&format!("refs/heads/{}", branch_name))?;
            reference.peel_to_commit()?
        }
        None => {
            let Ok(head) = repo.head() else {
                return Ok(None);
            };
            head.peel_to_commit()?
        }
    };

    if let Some(blob) = loop {
        if let Ok(tree) = commit.tree() {
            if let Ok(entry) = tree.get_path(std::path::Path::new(file_path)) {
                if entry.kind() == Some(git2::ObjectType::Blob) {
                    if let Ok(blob) = repo.find_blob(entry.id()) {
                        break Some(blob);
                    }
                }
            }
        }

        match commit.parent(0) {
            Ok(parent) => commit = parent,
            Err(_) => break None, // End of commit history
        }
    } {
        Ok(Some(blob.content().into()))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod git_repo_tests {
    use super::*;
    use tempfile::TempDir;

    use crate::git_integration::git_repo::open_libgit2_repo;

    #[test]
    fn test_direct_repo_read_write_empty() -> anyhow::Result<()> {
        // Create a temporary directory
        let tmp_repo = TempDir::new().unwrap();
        let tmp_repo_path = tmp_repo.path().to_path_buf();

        let _ = run_git_captured(Some(&tmp_repo_path), "init", &["--bare"], true, None)?;

        let repo = open_libgit2_repo(Some(&tmp_repo_path))?;

        let file_1 = "Random Content 1".as_bytes();
        let file_2 = "Random Content 2".as_bytes();
        let file_3 = "Random Content 3".as_bytes();
        let file_4 = "Random Content 4".as_bytes();

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

        // Now write to a specified branch
        create_commit(
            &repo,
            Some("my_branch"),
            "Test commit",
            &[("file_3.txt", file_3)],
            None,
        )?;
        let file_3_read = read_file_from_repo(&repo, "file_3.txt", Some("my_branch"))?.unwrap();
        assert_eq!(file_3, file_3_read);

        // Repeat
        create_commit(
            &repo,
            Some("my_branch"),
            "Test commit",
            &[("file_4.txt", file_4)],
            None,
        )?;
        let file_3_read = read_file_from_repo(&repo, "file_3.txt", Some("my_branch"))?.unwrap();
        let file_4_read = read_file_from_repo(&repo, "file_4.txt", Some("my_branch"))?.unwrap();
        assert_eq!(file_3, file_3_read);
        assert_eq!(file_4, file_4_read);

        Ok(())
    }
}
