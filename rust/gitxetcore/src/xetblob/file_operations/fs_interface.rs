use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;

use tracing::warn;
use walkdir::WalkDir;

use crate::errors::{GitXetRepoError, Result};
use crate::xetblob::{DirEntry, XetRepo};

/// Trait to encapsulate the required FS operations for flexibility and abstraction.
#[async_trait::async_trait] // Use async_trait for async methods in traits
pub trait FSInterface {
    fn file_name<'a>(&self, path: &'a str) -> &'a str;

    fn parent<'a>(&self, path: &'a str) -> Option<&'a str>;

    fn join(&self, path_1: &str, path_2: &str) -> String;

    async fn listdir(&self, path: &str, recursive: bool) -> Result<Vec<DirEntry>>;

    async fn info(&self, path: &str) -> Result<Option<DirEntry>>;

    fn is_xet(&self) -> bool;
}

fn unix_join(path_1: &str, path_2: &str) -> String {
    let path_1 = path_1.strip_suffix('/').unwrap_or(path_1);
    format!("{path_1}/{path_2}")
}

fn unix_file_name<'a>(path: &'a str) -> &'a str {
    if let Some(index) = path.rfind('/') {
        &path[(index + 1)..]
    } else {
        path
    }
}

fn unix_parent<'a>(path: &'a str) -> Option<&'a str> {
    let path = path.strip_suffix('/').unwrap_or(path);
    if let Some(index) = path.rfind('/') {
        let ret = &path[..index];
        Some(ret.strip_suffix('/').unwrap_or(ret))
    } else {
        None
    }
}

pub struct LocalFSHandle {}

#[async_trait::async_trait] // Use async_trait for async methods in traits
impl FSInterface for LocalFSHandle {
    fn is_xet(&self) -> bool {
        false
    }
    fn join(&self, path_1: &str, path_2: &str) -> String {
        let p: &Path = path_1.as_ref();
        p.join(path_2)
            .to_str()
            .map(|s| s.to_owned())
            .unwrap_or_else(|| unix_join(path_1, path_2))
    }

    fn file_name<'a>(&self, path: &'a str) -> &'a str {
        let p: &Path = path.as_ref();
        p.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_else(|| unix_file_name(path))
    }

    fn parent<'a>(&self, path: &'a str) -> Option<&'a str> {
        let p: &Path = path.as_ref();
        p.parent().and_then(|pp| pp.to_str())
    }

    async fn info(&self, path: &str) -> Result<Option<DirEntry>> {
        // Populate DirEntry struct from the local file system, if it exists
        match std::fs::metadata(path) {
            Ok(metadata) => Ok(Some(DirEntry::from_metadata(path.to_owned(), &metadata))),
            Err(e) => {
                if matches!(e.kind(), ErrorKind::NotFound) {
                    Ok(None)
                } else {
                    Err(e)?;
                    unreachable!();
                }
            }
        }
    }

    // Lists out a directory.
    async fn listdir(&self, path: &str, recursive: bool) -> Result<Vec<DirEntry>> {
        // Implement the following using walkdir
        let mut entries = Vec::new();
        let walkdir = WalkDir::new(path).follow_links(false).into_iter();

        for entry in walkdir.filter_entry(|e| recursive || e.depth() == 0) {
            // Handle errors
            let entry = entry?;
            let metadata = entry.metadata()?;

            // When not recursive, include directories; when recursive, skip directories
            if !recursive && metadata.is_dir() {
                continue;
            }

            let Some(rel_path) = entry
                .path()
                .strip_prefix(Path::new(path))
                .unwrap_or(entry.path())
                .to_str()
            else {
                warn!("Unicode error with path {:?}; skipping.", entry.path());
                continue;
            };

            entries.push(DirEntry::from_metadata(rel_path.to_owned(), &metadata));
        }

        Ok(entries)
    }
}

pub struct XetFSHandle {
    pub repo: Arc<XetRepo>,
    pub branch: String,
}

#[async_trait::async_trait] // Use async_trait for async methods in traits
impl FSInterface for XetFSHandle {
    fn is_xet(&self) -> bool {
        true
    }

    fn join(&self, path_1: &str, path_2: &str) -> String {
        unix_join(path_1, path_2)
    }

    fn file_name<'a>(&self, path: &'a str) -> &'a str {
        unix_file_name(path)
    }

    fn parent<'a>(&self, path: &'a str) -> Option<&'a str> {
        unix_parent(path)
    }

    async fn info(&self, path: &str) -> Result<Option<DirEntry>> {
        Ok(self.repo.stat(&self.branch, path).await?)
    }

    async fn listdir(&self, path: &str, recursive: bool) -> Result<Vec<DirEntry>> {
        let first_level = self.repo.listdir(&self.branch, path).await?;

        if !recursive {
            Ok(first_level)
        } else {
            // TODO
            Err(GitXetRepoError::InvalidOperation(
                "Not implemented yet.".to_owned(),
            ))
        }
    }
}
