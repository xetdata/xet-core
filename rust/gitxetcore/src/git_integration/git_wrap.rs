use crate::constants::MINIMUM_GIT_VERSION;
use crate::errors::GitXetRepoError;
use crate::errors::Result;
use crate::git_integration::git_commits::atomic_commit_impl;
use crate::git_integration::git_commits::ManifestEntry;
use git2::Repository;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Output;
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
    allow_stdin: bool,
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

    // Using idea from https://stackoverflow.com/questions/30776520/closing-stdout-or-stdin

    if allow_stdin {
        cmd.stdin(std::process::Stdio::inherit());
    } else {
        // Disable stdin so it doesn't hang silently in the background.
        cmd.stdin(std::process::Stdio::piped());
    }

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
    if !allow_stdin {
        drop(child.stdin.take());
    }

    Ok(child)
}

/// Calls git directly, piping both stdout and stderr through.  
///
/// The command is run in the directory base_directory.  On nonzero exit status, an error is
/// returned.
#[tracing::instrument(skip_all, err, fields(command = command, ?args))]
pub fn run_git_captured_raw(
    base_directory: Option<&PathBuf>,
    command: &str,
    args: &[&str],
    check_result: bool,
    env: Option<&[(&str, &str)]>,
) -> Result<Output> {
    // Block use of credential manager for this bit.
    let env: Vec<(&str, &str)> = match env {
        Some(d) => {
            let mut dv = Vec::from(d);
            dv.push(("GCM_INTERACTIVE", "never"));
            dv
        }
        None => vec![("GCM_INTERACTIVE", "never")],
    };

    let child = spawn_git_command(base_directory, command, args, Some(&env), true, false)?;

    let ret = child.wait_with_output()?;

    if check_result {
        if let Some(0) = ret.status.code() {
            Ok(ret)
        } else {
            let res_stdout = std::str::from_utf8(&ret.stdout[..])
                .unwrap_or("<Binary Data>")
                .trim();
            let res_stderr = std::str::from_utf8(&ret.stderr[..])
                .unwrap_or("<Binary Data>")
                .trim();
            Err(GitXetRepoError::Other(format!(
                "Error running git command: git {:?} {:?} err_code={:?}, stdout=\"{:?}\", stderr=\"{:?}\"",
                &command, args.iter().map(|s| format!("\"{s}\"")).join(" "), &ret.status, &res_stdout, &res_stderr
            )))
        }
    } else {
        Ok(ret)
    }
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
    let out = run_git_captured_raw(base_directory, command, args, check_result, env)?;

    let res_stdout = std::str::from_utf8(&out.stdout[..])
        .unwrap_or("<Binary Data>")
        .trim();
    let res_stderr = std::str::from_utf8(&out.stderr[..])
        .unwrap_or("<Binary Data>")
        .trim();

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

    Ok((
        out.status.code(),
        res_stdout.to_owned(),
        res_stderr.to_owned(),
    ))
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
    allow_stdin: bool,
    env: Option<&[(&str, &str)]>,
) -> Result<i32> {
    let mut child = spawn_git_command(base_directory, command, args, env, false, allow_stdin)?;

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

#[cfg(test)]
mod git_repo_tests {
    use super::*;
    use tempfile::TempDir;

    use crate::git_integration::git_repo::open_libgit2_repo;

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
}
