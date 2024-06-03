use crate::utils::{reserve_fd, resolve_path};
use lazy_static::lazy_static;
use libxet::git_integration::{get_git_dir_from_repo_path, resolve_repo_path, GitXetRepo};
use std::sync::RwLock;
use std::{path::PathBuf, sync::Arc};
use tokio::runtime::Runtime;

lazy_static! {
    static ref TOKIO_RUNTIME: Arc<Runtime> = {
        let rt = Runtime::new().expect("Failed to create Tokio runtime");
        Arc::new(rt)
    };
    static ref XET_REPO_WRAPPERS: RwLock<Vec<Arc<XetFSRepoWrapper>>> = RwLock::new(Vec::new());
    static ref XET_REPO_FN: RwLock<Vec<Arc<XetFSRepoWrapper>>> = RwLock::new(Vec::new());
}

// Attempt to find all the instances.
pub fn get_xet_instance(raw_path: &str) -> Result<Option<Arc<XetFSRepoWrapper>>, std::io::Error> {
    let path = resolve_path(raw_path)?;
    eprintln!("XetLDFS: get_xet_instance: {raw_path} resolved to {path:?}.");
    return Ok(None);

    if let Some(repo_wrapper) = XET_REPO_WRAPPERS
        .read()
        .unwrap()
        .iter()
        .find(|e| path.starts_with(&e.xet_root))
        .map(|xfs| xfs.clone())
    {
        eprintln!("No xet instance found for {path:?} ( from {raw_path}");

        Ok(Some(repo_wrapper))
    } else {
        // See if we need to create it.
        let Some(start_path) = path.parent() else {
            return Ok(None);
        };

        // TODO: cache known directories as known non-xet paths.
        let repo_path = resolve_repo_path(Some(start_path.to_path_buf()), false).unwrap_or(None);

        // TODO: Do more than print that we have this.
        eprintln!("Repo path for {path:?}: {repo_path:?}");

        Ok(None)
    }
}

// Utilities for

#[derive(Clone, Debug)]
struct XetFDData {
    xet_fsw: Arc<XetFSRepoWrapper>,
    sub_path: PathBuf,
    mode: String,
    offset: u64,
    // More to come.
}

lazy_static! {
    static ref XET_FD_TABLE: Vec<Option<XetFDData>> = vec![None; 4096];
}

#[derive(Debug)]
pub struct XetFSRepoWrapper {
    xet_root: PathBuf,
}

impl XetFSRepoWrapper {}
