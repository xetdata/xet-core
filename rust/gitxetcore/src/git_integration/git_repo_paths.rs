use crate::errors::Result;
use git2::Repository;
use std::path::PathBuf;

/// Returns the path of the repository we're operating in.  
///
/// If start_path is given, begin the search from there; otherwise start from the current working directory.
/// If return_gitdir is true, then return the git directory (typically .git/); otherwise, return the top
/// directory of the repo
///
/// If this is a bare repo, return_gitdir is irrelevant; both return_gitdir == true or false
/// will return the same path.
fn resolve_repo_path(start_path: Option<PathBuf>, return_gitdir: bool) -> Result<Option<PathBuf>> {
    let start_path = match start_path {
        Some(p) => p,
        None => std::env::current_dir()?,
    };

    let Ok(repo) = Repository::discover(&start_path).map_err(|e| {
        eprintln!("ERROR: Error discovering repo from {start_path:?} : {e:?}");
        e
    }) else {
        return Ok(None);
    };

    if return_gitdir || repo.is_bare() {
        Ok(repo.path().canonicalize().ok())
    } else {
        Ok(repo.workdir().and_then(|p| p.canonicalize().ok()))
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

#[cfg(test)]
mod test {
    use crate::git_integration::git_repo_paths::resolve_repo_path;

    #[test]
    fn test_repo_path_2() {
        let start_path = "/Users/di/tt/bsf13/test.csv";
        eprintln!("{:?}", resolve_repo_path(Some(start_path.into()), false));
        eprintln!("{:?}", resolve_repo_path(Some(start_path.into()), true));
    }
}
