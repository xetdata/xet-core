use std::{
    io,
    path::Path,
    time::{Duration, SystemTime},
};

use normalize_path::NormalizePath;

use super::{read_dir_async, DSAsyncIterator};

pub struct Metadata {
    file: Option<web_sys::File>,
}

impl Metadata {
    pub(crate) fn new_directory() -> Self {
        Metadata { file: None }
    }

    pub(crate) fn new_file(file: web_sys::File) -> Self {
        Metadata { file: Some(file) }
    }

    pub fn is_symlink(&self) -> bool {
        false
    }

    pub fn is_dir(&self) -> bool {
        self.file.is_none()
    }

    pub fn is_file(&self) -> bool {
        self.file.is_some()
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> io::Result<u64> {
        if let Some(file) = &self.file {
            Ok(file.size() as u64)
        } else {
            Err(io::Error::other("not a file"))
        }
    }

    pub fn modified(&self) -> io::Result<SystemTime> {
        if let Some(file) = &self.file {
            let res = SystemTime::UNIX_EPOCH
                .checked_add(Duration::from_millis(file.last_modified() as u64))
                .ok_or(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "could not get time correctly",
                ))?;
            Ok(res)
        } else {
            Err(io::Error::other(
                "meh, i don't wanna figure out directory last mod time",
            ))
        }
    }
}

pub async fn metadata<P: AsRef<Path>>(path: P) -> io::Result<Metadata> {
    let path = path.as_ref().normalize();
    let parent = if let Some(parent) = path.parent() {
        parent
    } else {
        return Ok(Metadata::new_directory()); // means that input was root dir
    };

    let mut dirents = read_dir_async(parent).await?;
    while let Some(entry) = dirents.next().await {
        if entry.path() == path {
            return entry.metadata();
        }
    }
    Err(io::Error::new(io::ErrorKind::NotFound, "not found"))
}

pub trait MetadataExt {
    fn size(&self) -> u64;
}

impl MetadataExt for Metadata {
    fn size(&self) -> u64 {
        self.len().unwrap_or(0)
    }
}
