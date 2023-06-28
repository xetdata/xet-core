use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::Metadata;
use std::ops::Bound;
use std::path::Path;

use git2::Oid;
use intaglio::Symbol;
use itertools::Itertools;
use lazy_static::lazy_static;
use nfsserve::nfs::{fattr3, fileid3, filename3, nfs_fh3, nfsstat3};
use nfsserve::nfs::nfsstat3::{NFS3ERR_BAD_COOKIE, NFS3ERR_IO, NFS3ERR_NOENT, NFS3ERR_NOTDIR, NFS3ERR_STALE};
use nfsstat3::NFS3ERR_BADHANDLE;
use tracing::error;

use crate::log::ErrorPrinter;
use crate::xetmnt::watch::contents::{DirectoryMetadata, EntryContent};
use crate::xetmnt::watch::metadata::FSObject;
use crate::xetmnt::watch::metadata::symbol::Symbols;
use crate::xetmnt::watch::metrics::MOUNT_NUM_OBJECTS;

lazy_static! {
    /// Generation number to be used with the file handle. It is the unix time of
    /// when field is first accessed.
    pub static ref GENERATION_NUMBER: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
}

fn generation_number() -> u64 {
    *GENERATION_NUMBER
}

/// Contains metadata about the filesystem and its internal layout.
///
/// This struct is not thread-safe.
pub struct FileSystem {
    /// ID for the filesystem root node
    root_id: fileid3,
    /// Version of the filesystem.
    fs_version: u64,
    /// Filesystem-level metadata. Changes to this will update the fs_version.
    fs_metadata: Metadata,
    /// List of all objects in the file system, with the id of the object
    /// corresponding to its index in this list.
    fs: Vec<FSObject>,
}

/// File Handle Operations
impl FileSystem {

    /// Converts the id into a 24-byte file handle with the format:
    /// {generation_number, fs_version, id}
    pub fn id_to_fh(&self, id: fileid3) -> nfs_fh3 {
        let gennum = generation_number();
        let mut ret: Vec<u8> = Vec::new();
        ret.extend_from_slice(&gennum.to_le_bytes());
        ret.extend_from_slice(&self.fs_version.to_le_bytes());
        ret.extend_from_slice(&id.to_le_bytes());
        nfs_fh3 { data: ret }
    }

    /// Converts the NFS file handle to a fileid. If the generation number or fs_version
    /// in the handle is older than the current state, we return NFS3ERR_STALE.
    /// If they're larger then we return NFS3ERR_BADHANDLE.
    pub fn fh_to_id(&self, fh: &nfs_fh3) -> Result<fileid3, nfsstat3> {
        if fh.data.len() != 24 {
            return Err(NFS3ERR_BADHANDLE);
        }
        let gen = u64::from_le_bytes(fh.data[0..8].try_into().map_err(|_|NFS3ERR_IO)?);
        let fs_ver = u64::from_le_bytes(fh.data[8..16].try_into().map_err(|_|NFS3ERR_IO)?);
        let id = u64::from_le_bytes(fh.data[16..24].try_into().map_err(|_|NFS3ERR_IO)?);
        let gennum = generation_number();
        Self::check_valid(gen, gennum)?;
        Self::check_valid(fs_ver, self.fs_version)?;
        Ok(id)
    }

    /// Compares parsed to the current, ensuring that they're equal,
    /// returning specific errors if parsed is < or > current.
    fn check_valid(parsed: u64, current: u64) -> Result<(), nfsstat3> {
        match parsed.cmp(&current) {
            Ordering::Less => Err(NFS3ERR_STALE),
            Ordering::Greater => Err(NFS3ERR_BADHANDLE),
            Ordering::Equal => Ok(()),
        }
    }
}

impl FileSystem {
    /// Build a new FileSystem based off of the src_path and Oid of the root node.
    pub fn new(src_path: &Path, root_oid: Oid, root_sym: Symbol) -> Result<Self, nfsstat3> {
        let fs_metadata = Self::get_fs_metadata(src_path)?;
        let fs_version = 1;
        let (fs, root_id) = Self::init_root_nodes(src_path, root_oid, root_sym, fs_version);
        Ok(Self {
            root_id,
            fs_version,
            fs_metadata,
            fs,
        })
    }

    /// Initializes the filesystem root directory from the given path and git object id.
    /// This involves setting up 2 objects: a magic "0-id" node and the actual node for
    /// the root of the mount.
    ///
    /// Returns the (FSObject list, root node id).
    fn init_root_nodes(src_path: &Path, root_oid: Oid, sym: Symbol, version: u64) -> (Vec<FSObject>, fileid3) {
        let mut fs = Vec::new();
        let dir_meta = DirectoryMetadata {
            path: src_path.to_path_buf(),
        };
        // Add magic 0 object
        fs.push(FSObject {
            id: 0,
            oid: root_oid,
            parent: 0,
            name: sym,
            contents: EntryContent::Directory(dir_meta.clone()),
            children: BTreeMap::new(),
            expanded: false,
            version,
        });

        // Add the root node
        let rootid = fs.len() as fileid3;
        fs.push(FSObject {
            id: rootid,
            oid: root_oid,
            parent: rootid, // parent of root is root
            name: sym,
            contents: EntryContent::Directory(dir_meta),
            children: BTreeMap::new(),
            expanded: false,
            version,
        });
        (fs, rootid)
    }


    /// Get FileSystem metadata from the indicated src_path (typically the FS root).
    fn get_fs_metadata(src_path: &Path) -> Result<Metadata, nfsstat3> {
        src_path
            .metadata()
            .log_error(format!("Unable to get metadata for: {src_path:?}"))
            .map_err(|_| NFS3ERR_IO)
    }

    /// Updates the root oid
    pub fn update_root_oid(&mut self, src_path: &Path, root_oid: Oid, root_sym: Symbol) -> Result<fileid3, nfsstat3> {
        let metadata = Self::get_fs_metadata(src_path)?;
        self.fs_metadata = metadata;
        self.fs_version += 1;

        let (new_fs, new_root) = Self::init_root_nodes(src_path, root_oid, root_sym, self.fs_version);
        self.fs = new_fs;
        self.root_id = new_root;

        Ok(self.root_id)
    }

    pub fn get_root_id(&self) -> fileid3 {
        self.root_id
    }

    /// Checks the given fileId to see if it is expanded or not
    pub fn is_expanded(&self, id: fileid3) -> Result<bool, nfsstat3> {
       self.get_entry_ref(id)
            .map(|entry| entry.expanded)
    }

    /// Sets the expanded field for the indicated file.
    pub fn set_expanded(&mut self, id: fileid3) -> Result<(), nfsstat3> {
        self.get_entry_ref_mut(id)
            .map(|entry| entry.expanded = true)
    }

    pub fn get_entry_ref(&self, id: fileid3) -> Result<&FSObject, nfsstat3> {
        self.try_get_entry(id)
            .ok_or(NFS3ERR_NOENT)
    }

    fn try_get_entry(&self, id: fileid3) -> Option<&FSObject> {
        self.fs.get(id as usize)
    }

    fn get_entry_ref_mut(&mut self, id: fileid3) -> Result<&mut FSObject, nfsstat3> {
        self.fs.get_mut(id as usize)
            .ok_or(NFS3ERR_NOENT)
    }

    /// Insert a new object in to the filesystem with the indicated parent id and metadata.
    /// The file id for the new object will be returned.
    pub fn insert(&mut self, parent_id: fileid3, name: Symbol, oid: Oid, contents: EntryContent) -> Result<fileid3, nfsstat3> {
        let id = self.fs.len() as fileid3;
        let parent = self.get_entry_ref_mut(parent_id)?;
        let version = parent.version;
        parent.children.insert(name, (id, oid));
        let is_dir = matches!(contents, EntryContent::Directory(_));
        self.fs.push(FSObject {
            id,
            oid,
            parent: parent_id,
            name,
            contents,
            children: BTreeMap::new(),
            expanded: !is_dir, // if this is a directory it is not expanded
            version,
        });
        MOUNT_NUM_OBJECTS.inc();
        Ok(id)
    }

    /// Get file attributes for the indicated file.
    pub fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        let entry = self.get_entry_ref(id)?;
        entry.contents
            .getattr(&self.fs_metadata, entry.id)
    }

    pub fn lookup(&self, dirid: fileid3, lookup: LookupType) -> Result<fileid3, nfsstat3> {
        let entry = self.get_entry_ref(dirid)?;
        if !entry.expanded {
            error!("BUG: directory: {dirid:?} not expanded before calling `lookup_child_id()`");
            return Err(NFS3ERR_IO);
        }
        if !matches!(entry.contents, EntryContent::Directory(_)) {
            return Err(NFS3ERR_NOTDIR);
        }
        lookup.lookup(entry)
    }

    pub fn list_children(&self, dir_id: fileid3, start_after: fileid3, max_entries: usize) -> Result<(Vec<fileid3>, bool), nfsstat3> {
        let entry = self.get_entry_ref(dir_id)?;
        if !entry.expanded {
            error!("BUG: directory: {dir_id:?} not expanded before calling `list_children()`");
            return Err(NFS3ERR_IO);
        }
        if !matches!(entry.contents, EntryContent::Directory(_)) {
            return Err(NFS3ERR_NOTDIR);
        }

        let range_start = if start_after > 0 {
            Bound::Excluded(self.get_child_symbol_from_entry(&entry, start_after)?)
        } else {
            Bound::Unbounded
        };

        let remaining_length = entry
            .children
            .range((range_start, Bound::Unbounded))
            .count();

        let entries = entry
            .children
            .range((range_start, Bound::Unbounded))
            .take(max_entries)
            .map(|(_, (id, _))| *id)
            .collect_vec();

        let end = entries.len() == remaining_length;
        Ok((entries, end))
    }

    fn get_child_symbol_from_entry(
        &self,
        entry: &FSObject,
        id: fileid3,
    ) -> Result<Symbol, nfsstat3> {
        self.try_get_entry(id)
            .ok_or(NFS3ERR_BAD_COOKIE)
            .map(|file_entry| file_entry.name)
            .and_then(|name| {
                entry
                    .children
                    .get(&name)
                    .map(|_| name)
                    .ok_or(NFS3ERR_BAD_COOKIE)
            })
    }
}

pub enum LookupType {
    CurrentDir,
    ParentDir,
    Child(Symbol),
}

impl LookupType {
    pub fn from_filename(symbol_table: &Symbols, filename: &filename3) -> Result<LookupType, nfsstat3> {
        Ok(match filename[..] {
            [b'.'] => Self::CurrentDir,              // '.' => current directory
            [b'.', b'.'] => Self::ParentDir, // '..' => parent directory
            _ => Self::Child(symbol_table.get_symbol(filename)?
                .ok_or(NFS3ERR_NOENT)?)
        })
    }

    fn lookup(&self, entry: &FSObject) -> Result<fileid3, nfsstat3> {
        Ok(match self {
            LookupType::CurrentDir => entry.id,
            LookupType::ParentDir => entry.parent,
            LookupType::Child(sym) => entry.children.get(sym)
                .map(|(fid, _)| *fid)
                .ok_or(NFS3ERR_NOENT)?
        })
    }
}


#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    const ROOT_OID: &str = "d0b22188428e4098f5036f7940ebadb27d161f4c";

    fn get_root_oid() -> Oid {
        Oid::from_str(ROOT_OID).unwrap()
    }

    fn get_test_fs() -> FileSystem {
        let root_dir = TempDir::new().unwrap();
        let oid = get_root_oid();
        FileSystem::new(root_dir.path(), oid, Symbol::new(1)).unwrap()
    }

    #[test]
    fn test_fs_new() {
        let fs = get_test_fs();
        let obj_list = &fs.fs;
        assert_eq!(1, fs.get_root_id());
        assert_eq!(2, obj_list.len());
        for (i, obj) in obj_list.iter().enumerate() {
            assert_eq!(i, obj.id as usize);
            assert!(matches!(obj.contents, EntryContent::Directory(_)));
            assert_eq!(get_root_oid(), obj.oid);
            assert!(!obj.expanded);
        }
    }
}
