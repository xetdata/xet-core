use crate::constants::MINIMUM_GIT_VERSION;
use crate::errors::GitXetRepoError;
use crate::errors::Result;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::str::FromStr;
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
