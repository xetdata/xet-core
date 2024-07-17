use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use libc::c_char;

use crate::real_stat;
use crate::runtime::with_interposing_disabled;

/// Returns true if query_path is a non-git path within the repo at repo_path
pub fn is_path_in_repo(query_path: impl AsRef<Path>, repo_path: impl AsRef<Path>) -> bool {
    // Assume a well-formed path (absolute, no /./ or /../ or // etc.)
    let query_path = query_path.as_ref();
    let repo_path = repo_path.as_ref();

    ld_func_trace!("in_path_in_repo", query_path, repo_path);

    // Do it with bytes so we can test for the /.git/ in one go.
    let qp = query_path.as_os_str().as_bytes();
    let rp = repo_path.as_os_str().as_bytes();

    let mut n = rp.len();

    // For consistency, don't test the trailing / if it's not
    if *rp.last().unwrap() == b'/' {
        n -= 1;
    }

    if qp.len() < n {
        debug_assert!(!query_path.starts_with(repo_path));
        ld_trace!("in_path_in_repo: file {query_path:?} is not in {repo_path:?}");
        return false;
    }

    if qp[..n] != rp[..n] {
        ld_trace!("in_path_in_repo: file {query_path:?} is not in {repo_path:?}");
        ld_trace!("\nqp[..n]={:?}\nrp[..n]={:?}", &qp[..n], &rp[..n]);
        assert!(!query_path.starts_with(repo_path));
        return false;
    }

    // Make sure qp is starting a directory
    if qp.len() > n && qp[n] != b'/' {
        debug_assert!(!query_path.starts_with(repo_path));
        ld_trace!("in_path_in_repo: file {query_path:?} is not in {repo_path:?}");
        return false;
    }
    n += 1;

    // Make sure that we aren't accessing something in the .git/ part of the repo.
    const GITDIR: &[u8] = b".git/";

    if qp.len() >= n + GITDIR.len() {
        let n = rp.len();
        if &qp[n..(n + GITDIR.len())] == GITDIR {
            return false;
        }
    }

    debug_assert!(query_path.starts_with(repo_path));
    ld_trace!("in_path_in_repo: file {query_path:?} is in {repo_path:?}");
    true
}

pub fn verify_path_is_git_repo(resolved_path: &Path) -> bool {
    use libc::{stat, S_IFDIR};

    let _lg = with_interposing_disabled();

    // Just see if .git exists in this repo and .
    let git_path = resolved_path.join(".git");
    git_path.exists() && git_path.is_dir()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn test_is_path_in_repo() {
        // Setup temporary directories for testing
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path();

        // Helper function to create a .git directory
        fn create_git_dir(path: &Path) {
            let git_dir = path.join(".git");
            fs::create_dir(&git_dir).unwrap();
        }

        // Case: valid path within repo
        let repo_path = temp_path.join("valid_repo/");
        fs::create_dir_all(&repo_path).unwrap();
        let query_path_1 = repo_path.join("src/file.rs");
        assert!(is_path_in_repo(&query_path_1, &repo_path));

        // Case: path exactly the repo path
        assert!(is_path_in_repo(&repo_path, &repo_path));

        // Case: path within .git directory
        create_git_dir(&repo_path);
        let query_path_3 = repo_path.join(".git/config");
        assert!(!is_path_in_repo(&query_path_3, &repo_path));

        // Case: path outside repo
        let outside_repo_path = temp_path.join("another_repo/file.rs");
        assert!(!is_path_in_repo(&outside_repo_path, &repo_path));

        // Case: path in a similarly named directory but not the same repo
        let similar_repo_path = temp_path.join("valid_repo_backup/src/file.rs");
        assert!(!is_path_in_repo(&similar_repo_path, &repo_path));

        // Case: path within a subdirectory named .git, not the top-level .git directory
        let sub_git_path = repo_path.join("src/.git/file.rs");
        assert!(is_path_in_repo(&sub_git_path, &repo_path));

        // Case: path within the repo but with additional similar path element
        let similar_element_path = repo_path.join("src/.gitignored/file.rs");
        assert!(is_path_in_repo(&similar_element_path, &repo_path));

        // Cleanup
        temp_dir.close().unwrap();
    }

    #[test]
    fn test_verify_path_is_git_repo() {
        // Setup temporary directories for testing
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path();

        // Helper function to create a .git directory
        fn create_git_dir(path: &Path) {
            let git_dir = path.join(".git");
            fs::create_dir(&git_dir).unwrap();
        }

        // Case: valid git repo path
        let valid_repo_path = temp_path.join("valid_repo");
        fs::create_dir(&valid_repo_path).unwrap();
        create_git_dir(&valid_repo_path);
        assert!(verify_path_is_git_repo(&valid_repo_path));

        // Case: path without .git directory
        let no_git_repo_path = temp_path.join("no_git_repo");
        fs::create_dir(&no_git_repo_path).unwrap();
        assert!(!verify_path_is_git_repo(&no_git_repo_path));

        // Case: path to a file named .git instead of a directory
        let file_git_repo_path = temp_path.join("file_git_repo");
        fs::create_dir(&file_git_repo_path).unwrap();
        let file_git = file_git_repo_path.join(".git");
        fs::write(&file_git, "This is a file, not a directory").unwrap();
        assert!(!verify_path_is_git_repo(&file_git_repo_path));

        // Cleanup
        temp_dir.close().unwrap();
    }
}
