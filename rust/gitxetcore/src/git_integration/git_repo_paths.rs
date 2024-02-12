use crate::config::XetConfig;
use crate::errors::Result;
use crate::git_integration::git_process_wrapping::run_git_captured;
use std::path::PathBuf;
use std::str::FromStr;
use tracing::info;

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

/// Obtains the repository path either from config or current directory
pub fn get_repo_path_from_config(config: &XetConfig) -> Result<PathBuf> {
    match config.repo_path() {
        Ok(path) => Ok(path.clone()),
        Err(_) => Ok(std::env::current_dir()?),
    }
}
