use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;

use walkdir::WalkDir;

use crate::errors::Result;
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
    let path_1_ = path_1.strip_suffix('/').unwrap_or(path_1.as_str());
    format!("{path_1}/{path_2}")
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
            .to_string()
            .unwrap_or_else(|| unix_join(path_1, path_2))
    }

    fn file_name<'a>(&self, path: &'a str) -> &'a str {
        let p: &Path = path.as_ref();
        p.file_name().as_str().unwrap()
    }

    fn parent<'a>(&self, path: &'a str) -> Option<&'a str> {
        let p: &Path = path.as_ref();
        p.parent().map(|pp| pp.as_str().unwrap())
    }

    async fn info(&self, path: &str) -> Result<Option<DirEntry>> {
        // Populate DirEntry struct from the local file system, if it exists
        match std::fs::metadata(path) {
            Ok(metadata) => Ok(Some(DirEntry::from_metadata(path.to_owned(), &metadata))),
            Err(e) => {
                if matches!(e.kind(), ErrorKind::NotFound) {
                    Ok(None)
                } else {
                    e?;
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
            let entry = entry?;
            let metadata = entry.metadata()?;

            // When not recursive, include directories; when recursive, skip directories
            if recursive && metadata.is_dir() {
                continue;
            }

            let rel_path = entry
                .path()
                .strip_prefix(Path::new(path))
                .unwrap_or(entry.path())
                .to_path_buf();

            entries.push(DirEntry::new(rel_path, metadata.len(), metadata.is_dir()));
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
        if let Some(index) = path.rfind('/') {
            &path[(index + 1)..]
        } else {
            path
        }
    }

    fn parent<'a>(&self, path: &'a str) -> Option<&'a str> {
        if let Some(index) = path.rfind('/') {
            let ret = &path[..index];
            ret.strip_suffix('/').unwrap_or(ret)
        }

        let p: &Path = path.as_ref();
        p.parent().map(|pp| pp.as_str().unwrap())
    }

    async fn info(&self, path: &str) -> Option<DirEntry> {
        self.repo.stat(&self.branch, path).await?
    }

    async fn listdir(&self, path: &str, recursive: bool) -> Result<Vec<DirEntry>> {
        todo!();
        /*
            let first_level = self.repo.listdir(&self.branch, path).await?;

            if !recursive {
                return Ok(first_level);
            }

            // Now we have to do the recursive version.
            let mut listing = Vec::new();
            let mut queued_operations = JoinSet::new();

            loop {




            }
        */
    }
}
