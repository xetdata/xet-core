use lazy_static::lazy_static;
use path_absolutize::Absolutize;
use std::collections::HashMap;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;

use crate::path_utils::{absolute_path, absolute_path_c, path_of_fd};
use crate::real_close;
use crate::repo_path_utils::verify_path_is_git_repo;
use crate::runtime::{self, with_interposing_disabled};
use crate::utils::cstring_to_str;
use crate::xet_repo_wrapper::XetFSRepoWrapper;
use crate::xet_rfile::XetFdReadHandle;
use crate::{ld_trace, ld_warn};
use crate::{my_dup2, real_open};

use libc::{c_char, c_int};

// Constructing and storing the repository paths
lazy_static! {
    static ref XET_REPOS: Vec<Arc<XetFSRepoWrapper>> = initialize_repo_paths();
}

/// Loads the repository paths from the environment variable XET_LDFS_REPO.
///
/// This should be a ;-separated list of one or more absolute paths to a repository.
fn initialize_repo_paths() -> Vec<Arc<XetFSRepoWrapper>> {
    let _ig = with_interposing_disabled();

    // Get the environment variable
    let repo_env = std::env::var("XET_LDFS_REPO").unwrap_or_default();

    // Split the environment variable by ';'
    let paths: Vec<&str> = repo_env.split(';').collect();

    // Initialize the vector to hold the results
    let mut repos: Vec<Arc<XetFSRepoWrapper>> = Vec::with_capacity(paths.len());

    for mut path in paths {
        // Trim out the whitespace.
        path = path.trim();

        // Is the path quoted?
        if path.len() >= 2
            && ((path.starts_with('"') && path.ends_with('"'))
                || (path.starts_with('\'') && path.ends_with('\'')))
        {
            path = &path[1..(path.len() - 1)];
        }

        if path.is_empty() {
            ld_trace!("Skipping load of empty string.");
            continue;
        }

        // Check if the path is absolute
        if !path.starts_with('/') {
            ld_error!("Repositories specified with XET_LDFS_REPO must be absolute paths ({path} not absolute).");
            continue;
        }

        let Ok(repo_path) = Path::new(path).absolutize().map_err(|e| {
            ld_error!("Error converting {path} to an absolute path: {e}");
            e
        }) else {
            continue;
        };

        let abs_repo_path = repo_path.into_owned();

        if !verify_path_is_git_repo(&abs_repo_path) {
            if abs_repo_path.as_os_str().as_bytes() != path.as_bytes() {
                ld_error!("Path {abs_repo_path:?} (resolved from {path}) does not appear to be a git repo, skipping.");
            } else {
                ld_error!("Path {abs_repo_path:?} does not appear to be a git repo, skipping.");
            }
            continue;
        }

        // Push the tuple into the vector
        repos.push(XetFSRepoWrapper::new(abs_repo_path));
    }

    if repos.is_empty() {
        ld_warn!("XetLDFS interpose library loaded, but no valid repositories specified with XET_LDFS_REPO; disabling.");
    }

    repos
}

pub fn get_repo_context(file_path: impl AsRef<Path>) -> Option<Arc<XetFSRepoWrapper>> {
    for repo_wrapper in XET_REPOS.iter() {
        if repo_wrapper.file_in_repo(file_path.as_ref()) {
            return Some(repo_wrapper.clone());
        }
    }
    None
}

///////
// Tracking file reading

lazy_static! {
    static ref FD_LOOKUP: std::sync::RwLock<HashMap<c_int, Arc<XetFdReadHandle>>> =
        std::sync::RwLock::new(HashMap::new());
}

pub fn set_fd_read_interpose(fd: c_int, fd_info: Arc<XetFdReadHandle>) {
    // We now have one thing to track, so go ahead and activate all the read and fstat commands.
    runtime::activate_fd_runtime();

    FD_LOOKUP.write().unwrap().insert(fd, fd_info);
}

pub fn maybe_fd_read_managed(fd: c_int) -> Option<Arc<XetFdReadHandle>> {
    FD_LOOKUP.read().unwrap().get(&fd).cloned()
}

pub fn close_fd_if_registered(fd: c_int) -> bool {
    FD_LOOKUP.write().unwrap().remove_entry(&fd).is_some()
}

pub fn register_read_fd(path: impl AsRef<Path>, fd: c_int) -> bool {
    let path = path.as_ref();

    ld_trace!("register_read_fd: {path:?} for {fd}");

    if let Some(xet_repo) = get_repo_context(path) {
        if let Some(mut fd_info) = xet_repo.get_read_handle_if_valid(path) {
            fd_info.fd = fd;

            set_fd_read_interpose(fd, Arc::new(fd_info));

            ld_trace!("{path:?} registered to {fd}");

            return true;
        } else {
            ld_trace!("open for read if pointer file for {path:?} returned None.");
        }
    } else {
        ld_trace!("get_repo_context did not return repo for : {path:?}");
    }

    false
}

pub unsafe fn register_read_fd_c(raw_path: *const c_char, fd: c_int) -> bool {
    if let Some(path) = absolute_path_c(&raw_path) {
        register_read_fd(path, fd)
    } else {
        false
    }
}

/// Returns true if the open flags mean the file should be materialized.
pub fn file_needs_materialization(open_flags: libc::c_int) -> bool {
    let on = |flag| open_flags & flag != 0;

    let will_write = matches!(open_flags & libc::O_ACCMODE, libc::O_WRONLY | libc::O_RDWR);

    // need to materialize if writing and expect to keep any data
    will_write && !on(libc::O_TRUNC)
}

/// Ensures the file at the given path has been materialized.
pub fn ensure_file_materialized(path: impl AsRef<Path>) -> bool {
    let path = path.as_ref();

    let Some(repo) = get_repo_context(path) else {
        ld_trace!("Failed to load repo context for {path:?}.");
        return false;
    };

    repo.ensure_path_materialized(path)
}

pub unsafe fn ensure_file_materialized_c(raw_path: *const c_char) -> bool {
    if let Some(path) = absolute_path_c(&raw_path) {
        ensure_file_materialized(path)
    } else {
        false
    }
}

pub fn materialize_file_under_fd(fd: libc::c_int) -> bool {
    let _ig = with_interposing_disabled();

    // Convert the file descriptor to a file path
    let Some(path) = path_of_fd(fd) else {
        ld_trace!("Error getting path from fd={fd}, not attempting materialization.");
        return false;
    };

    // Get the absolute path
    let Some(abs_path) = absolute_path(cstring_to_str(&path)) else {
        ld_trace!("Error getting absolute path from path {path:?}, fd={fd},  not attempting materialization.");
        return false;
    };

    if ensure_file_materialized(&abs_path) {
        ld_trace!("File at path {abs_path:?} materialized, reopening.");

        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags == -1 {
            ld_warn!("materialize_file_under_fd: Error retrieving flags for orginial fd={fd}, path={abs_path:?}.");
            return false;
        }

        // Get the original file's mode
        let file_mode = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if file_mode == -1 {
            ld_warn!("materialize_file_under_fd: Error retrieving mode for orginial fd={fd}.");
            return false;
        }

        let new_fd = unsafe { real_open(path.as_ptr(), flags, file_mode as libc::mode_t) };

        if new_fd == -1 {
            ld_warn!(
                "materialize_file_under_fd: Error opening materialized file at {path:?} : {:?}",
                std::io::Error::last_os_error()
            );
            return false;
        }

        let dup2_res = unsafe { my_dup2(new_fd, fd) };

        if dup2_res == -1 {
            ld_warn!(
                "materialize_file_under_fd: Error calling dup2 to replace old path: {:?}",
                std::io::Error::last_os_error()
            );
            return false;
        }

        unsafe { real_close(new_fd) };

        ld_trace!("materialize_file_under_fd_if_needed: fd={fd}, path={path:?} materialized.");

        true
    } else {
        false
    }
}
