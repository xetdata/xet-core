use std::borrow::Cow;
use std::fs;
use std::fs::Permissions;
use std::path::{Path, PathBuf};

#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

use anyhow::anyhow;
use git2::{self, ObjectType, Oid};
use nfsserve::nfs::{fattr3, fileid3, ftype3, nfsstat3, nfstime3, specdata3};

use crate::constants::POINTER_FILE_LIMIT;
use pointer_file::PointerFile;

#[derive(Default, Debug, Clone)]
pub struct EntryMetadata {
    pub size: u64,
    pub mode: u32,
}

#[derive(Debug, Clone)]
pub struct DirectoryMetadata {
    pub path: PathBuf,
}

#[derive(Debug, Clone)]
pub enum EntryContent {
    XetFile((EntryMetadata, PointerFile)),
    RegularFile(EntryMetadata),
    Directory(DirectoryMetadata),
}

/// Construction
impl EntryContent {
    /// Build an EntryContent from the provided entry stored in the repo at the indicated path.
    /// Returns Err(_) if there are problems interpreting the entry.
    /// Returns Ok(None) if the type of tree entry is unsupported.
    /// TODO: support symlinks using the file mode
    pub fn from_repo_tree_entry(
        repo: &git2::Repository,
        entry: &git2::TreeEntry,
        path: &Path,
    ) -> Result<Option<Self>, anyhow::Error> {
        let kind = entry
            .kind()
            .ok_or(anyhow!("Tree entry at: {path:?} has no git type"))?;
        match kind {
            ObjectType::Any | ObjectType::Commit | ObjectType::Tag => Ok(None),
            ObjectType::Tree => Ok(Some(EntryContent::Directory(DirectoryMetadata {
                path: path.to_path_buf(),
            }))),
            ObjectType::Blob => Ok(Some(Self::from_repo_blob(
                repo,
                entry.id(),
                entry.filemode() as u32,
            )?)),
        }
    }

    /// Convert the oid indicating a blob into an EntryContent.
    /// If the blob is small, it tries to convert it to a PointerFile, returning a XetFile.
    /// Else, we return a RegularFile instead.
    fn from_repo_blob(
        repo: &git2::Repository,
        oid: Oid,
        mode: u32,
    ) -> Result<EntryContent, anyhow::Error> {
        let blob = repo.find_blob(oid)?;
        let blob_size = blob.size();
        if blob_size <= POINTER_FILE_LIMIT {
            let pointer = PointerFile::init_from_string(std::str::from_utf8(blob.content())?, "");
            if pointer.is_valid() {
                return Ok(EntryContent::XetFile((
                    EntryMetadata {
                        size: pointer.filesize(),
                        mode,
                    },
                    pointer,
                )));
            }
        }
        // all fall through has it turn into a regular file
        Ok(EntryContent::RegularFile(EntryMetadata {
            size: blob_size as u64,
            mode,
        }))
    }
}

impl EntryContent {
    /// Gets the metadata attributes for the EntryContent, using the filesystem's metadata and
    /// the fileid to help identify the
    pub fn getattr(&self, fs_metadata: &fs::Metadata, fid: fileid3) -> Result<fattr3, nfsstat3> {
        let (entrymeta, ftype, nlink) = match self {
            EntryContent::XetFile((m, _)) => (Cow::Borrowed(m), ftype3::NF3REG, 1),
            EntryContent::RegularFile(m) => (Cow::Borrowed(m), ftype3::NF3REG, 1),
            EntryContent::Directory(_) => (Cow::default(), ftype3::NF3DIR, 2),
        };
        Ok(self.attr_os(fs_metadata, fid, ftype, nlink, entrymeta))
    }
}

/// OS-specific methods
#[cfg(unix)]
impl EntryContent {
    fn mode_umask_write(mode: u32) -> u32 {
        let mut mode = Permissions::from_mode(mode);
        mode.set_readonly(true);
        mode.mode()
    }

    fn attr_os(
        &self,
        fs_metadata: &fs::Metadata,
        fid: fileid3,
        ftype: ftype3,
        nlink: u32,
        entrymeta: Cow<EntryMetadata>,
    ) -> fattr3 {
        let size = entrymeta.size;
        let mode = Self::mode_umask_write(entrymeta.mode);
        fattr3 {
            ftype,
            mode,
            nlink,
            uid: fs_metadata.uid(),
            gid: fs_metadata.gid(),
            size,
            used: size,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: fid,
            atime: nfstime3 {
                seconds: fs_metadata.atime() as u32,
                nseconds: fs_metadata.atime_nsec() as u32,
            },
            mtime: nfstime3 {
                seconds: fs_metadata.mtime() as u32,
                nseconds: fs_metadata.mtime_nsec() as u32,
            },
            ctime: nfstime3 {
                seconds: fs_metadata.ctime() as u32,
                nseconds: fs_metadata.ctime_nsec() as u32,
            },
        }
    }
}

/// OS-specific methods
#[cfg(windows)]
impl EntryContent {
    fn attr_os(
        &self,
        _fs_metadata: &fs::Metadata,
        fid: fileid3,
        ftype: ftype3,
        nlink: u32,
        entrymeta: Cow<EntryMetadata>,
    ) -> fattr3 {
        let mode = match self {
            EntryContent::Directory(_) => 0o0511,
            _ => 0o0555,
        };
        let size = entrymeta.size;
        fattr3 {
            ftype,
            mode,
            nlink,
            uid: 507,
            gid: 507,
            size,
            used: size,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: fid,
            atime: nfstime3::default(),
            mtime: nfstime3::default(),
            ctime: nfstime3::default(),
        }
    }
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    fn get_tmp_dir_metadata() -> fs::Metadata {
        let d = TempDir::new().unwrap();
        d.path().metadata().unwrap()
    }

    #[test]
    fn test_getattr_regular_file() {
        let size = 27429;
        let file = EntryContent::RegularFile(EntryMetadata { size, mode: 0o0644 });
        let fs_metadata = get_tmp_dir_metadata();
        let attr = file.getattr(&fs_metadata, 1).unwrap();

        assert_eq!(1, attr.fileid);
        assert_eq!(size, attr.size);
        assert_eq!(size, attr.used);
        assert_eq!(0o0444, attr.mode);
        assert_eq!(1, attr.nlink);
        assert_eq!(fs_metadata.gid(), attr.gid);
        assert_eq!(fs_metadata.uid(), attr.uid);
        assert_eq!(0, attr.fsid);

        let atime = nfstime3 {
            seconds: fs_metadata.atime() as u32,
            nseconds: fs_metadata.atime_nsec() as u32,
        };
        let mtime = nfstime3 {
            seconds: fs_metadata.mtime() as u32,
            nseconds: fs_metadata.mtime_nsec() as u32,
        };
        let ctime = nfstime3 {
            seconds: fs_metadata.ctime() as u32,
            nseconds: fs_metadata.ctime_nsec() as u32,
        };
        check_time(atime, attr.atime);
        check_time(mtime, attr.mtime);
        check_time(ctime, attr.ctime);
    }

    #[test]
    fn test_getattr_pointer_file() {
        let size = 5739245;
        let file = EntryContent::XetFile((
            EntryMetadata { size, mode: 0o0666 },
            PointerFile::init_from_info("tmp.txt", "abc23453559287def3", size),
        ));
        let fs_metadata = get_tmp_dir_metadata();
        let attr = file.getattr(&fs_metadata, 1).unwrap();

        assert_eq!(1, attr.fileid);
        assert_eq!(size, attr.size);
        assert_eq!(size, attr.used);
        assert_eq!(0o0444, attr.mode);
        assert_eq!(1, attr.nlink);
        assert_eq!(fs_metadata.gid(), attr.gid);
        assert_eq!(fs_metadata.uid(), attr.uid);
        assert_eq!(0, attr.fsid);

        let atime = nfstime3 {
            seconds: fs_metadata.atime() as u32,
            nseconds: fs_metadata.atime_nsec() as u32,
        };
        let mtime = nfstime3 {
            seconds: fs_metadata.mtime() as u32,
            nseconds: fs_metadata.mtime_nsec() as u32,
        };
        let ctime = nfstime3 {
            seconds: fs_metadata.ctime() as u32,
            nseconds: fs_metadata.ctime_nsec() as u32,
        };
        check_time(atime, attr.atime);
        check_time(mtime, attr.mtime);
        check_time(ctime, attr.ctime);
    }

    #[test]
    fn test_getattr_directory() {
        let dir = EntryContent::Directory(DirectoryMetadata {
            path: PathBuf::from("/foo"),
        });
        let fs_metadata = get_tmp_dir_metadata();
        let attr = dir.getattr(&fs_metadata, 1).unwrap();

        assert_eq!(1, attr.fileid);
        assert_eq!(0, attr.size);
        assert_eq!(0, attr.used);
        assert_eq!(0, attr.mode);
        assert_eq!(2, attr.nlink);
        assert_eq!(fs_metadata.gid(), attr.gid);
        assert_eq!(fs_metadata.uid(), attr.uid);
        assert_eq!(0, attr.fsid);

        let atime = nfstime3 {
            seconds: fs_metadata.atime() as u32,
            nseconds: fs_metadata.atime_nsec() as u32,
        };
        let mtime = nfstime3 {
            seconds: fs_metadata.mtime() as u32,
            nseconds: fs_metadata.mtime_nsec() as u32,
        };
        let ctime = nfstime3 {
            seconds: fs_metadata.ctime() as u32,
            nseconds: fs_metadata.ctime_nsec() as u32,
        };
        check_time(atime, attr.atime);
        check_time(mtime, attr.mtime);
        check_time(ctime, attr.ctime);
    }

    fn check_time(expected: nfstime3, actual: nfstime3) {
        assert_eq!(expected.seconds, actual.seconds);
        assert_eq!(expected.nseconds, actual.nseconds);
    }
}
