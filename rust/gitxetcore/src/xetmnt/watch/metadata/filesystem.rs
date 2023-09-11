use std::collections::BTreeMap;
use std::fs::Metadata;
use std::ops::Bound;
use std::path::Path;

use git2::Oid;
use intaglio::Symbol;
use itertools::Itertools;
use nfsserve::nfs::nfsstat3::{NFS3ERR_BAD_COOKIE, NFS3ERR_IO, NFS3ERR_NOENT, NFS3ERR_NOTDIR};
use nfsserve::nfs::{fattr3, fileid3, filename3, nfsstat3};
use tracing::error;

use crate::log::ErrorPrinter;
use crate::xetmnt::watch::contents::{DirectoryMetadata, EntryContent};
use crate::xetmnt::watch::metadata::symbol::Symbols;
use crate::xetmnt::watch::metadata::FSObject;
use crate::xetmnt::watch::metrics::MOUNT_NUM_OBJECTS;

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
    fn init_root_nodes(
        src_path: &Path,
        root_oid: Oid,
        sym: Symbol,
        version: u64,
    ) -> (Vec<FSObject>, fileid3) {
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
    pub fn update_root_oid(&mut self, src_path: &Path, root_oid: Oid) -> Result<fileid3, nfsstat3> {
        let metadata = Self::get_fs_metadata(src_path)?;
        self.fs_metadata = metadata;
        let new_version = self.fs_version + 1;
        self.fs_version = new_version;

        self.get_entry_ref_mut(self.root_id)
            .log_error("BUG: root node not found")
            .map(|root| {
                root.oid = root_oid;
                root.children.clear();
                root.expanded = false;
                root.version = new_version;
            })?;

        Ok(self.root_id)
    }

    pub fn get_root_id(&self) -> fileid3 {
        self.root_id
    }

    /// Checks the given fileId to see if it is expanded or not
    pub fn is_expanded(&self, id: fileid3) -> Result<bool, nfsstat3> {
        self.get_entry_ref(id).map(|entry| entry.expanded)
    }

    /// Sets the expanded field for the indicated file.
    pub fn set_expanded(&mut self, id: fileid3) -> Result<(), nfsstat3> {
        self.get_entry_ref_mut(id)
            .map(|entry| entry.expanded = true)
    }

    pub fn get_entry_ref(&self, id: fileid3) -> Result<&FSObject, nfsstat3> {
        self.try_get_entry(id).ok_or(NFS3ERR_NOENT)
    }

    fn try_get_entry(&self, id: fileid3) -> Option<&FSObject> {
        self.fs.get(id as usize)
    }

    fn get_entry_ref_mut(&mut self, id: fileid3) -> Result<&mut FSObject, nfsstat3> {
        self.fs.get_mut(id as usize).ok_or(NFS3ERR_NOENT)
    }

    /// Insert a new object in to the filesystem with the indicated parent id and metadata.
    /// The file id for the new object will be returned.
    pub fn insert(
        &mut self,
        parent_id: fileid3,
        name: Symbol,
        oid: Oid,
        contents: EntryContent,
    ) -> Result<fileid3, nfsstat3> {
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
        entry.contents.getattr(&self.fs_metadata, entry.id)
    }

    pub fn lookup(&self, dirid: fileid3, strategy: LookupStrategy) -> Result<fileid3, nfsstat3> {
        let entry = self.get_entry_ref(dirid)?;
        if !entry.expanded {
            error!("BUG: directory: {dirid:?} not expanded before calling `lookup_child_id()`");
            return Err(NFS3ERR_IO);
        }
        if !matches!(entry.contents, EntryContent::Directory(_)) {
            return Err(NFS3ERR_NOTDIR);
        }
        strategy.lookup(entry)
    }

    pub fn list_children(
        &self,
        dir_id: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<(Vec<fileid3>, bool), nfsstat3> {
        let entry = self.get_entry_ref(dir_id)?;
        if !entry.expanded {
            error!("BUG: directory: {dir_id:?} not expanded before calling `list_children()`");
            return Err(NFS3ERR_IO);
        }
        if !matches!(entry.contents, EntryContent::Directory(_)) {
            return Err(NFS3ERR_NOTDIR);
        }

        let range_start = if start_after > 0 {
            Bound::Excluded(self.get_child_symbol_from_entry(entry, start_after)?)
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

/// LookupStrategy defines a strategy for looking up the id of a path from an [FSObject].
/// The strategy can be constructed using [from_filename] and executed on some
/// FSObject using [lookup].
#[derive(Debug, Copy, Clone)]
pub enum LookupStrategy {
    CurrentDir,
    ParentDir,
    Child(Symbol),
}

impl LookupStrategy {
    /// Builds a [LookupStrategy] from the filename. Since [FSObject] operates on [Symbol]s,
    /// we may need to translate the filename to a symbol using the [Symbols] table.
    /// If the filename cannot be translated, we return an error.
    pub fn from_filename(
        symbol_table: &Symbols,
        filename: &filename3,
    ) -> Result<LookupStrategy, nfsstat3> {
        Ok(match filename[..] {
            [b'.'] => Self::CurrentDir,      // '.' => current directory
            [b'.', b'.'] => Self::ParentDir, // '..' => parent directory
            _ => Self::Child(symbol_table.get_symbol(filename)?.ok_or(NFS3ERR_NOENT)?),
        })
    }

    /// Executes this strategy on the provided entry, returning an id if it exists, or
    /// else [NFS3ERR_NOENT].
    fn lookup(&self, entry: &FSObject) -> Result<fileid3, nfsstat3> {
        Ok(match self {
            LookupStrategy::CurrentDir => entry.id,
            LookupStrategy::ParentDir => entry.parent,
            LookupStrategy::Child(sym) => entry
                .children
                .get(sym)
                .map(|(fid, _)| *fid)
                .ok_or(NFS3ERR_NOENT)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    const ROOT_OID: &str = "d0b22188428e4098f5036f7940ebadb27d161f4c";

    pub fn get_root_oid() -> Oid {
        Oid::from_str(ROOT_OID).unwrap()
    }

    pub fn get_test_fs() -> FileSystem {
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

#[cfg(test)]
mod test_lookup_strategy {
    use super::*;
    use std::path::PathBuf;

    // TODO: allow filename3 to implement From<&str> (needs external repo/crate change)
    fn to_filename(s: &str) -> filename3 {
        s.as_bytes().into()
    }

    #[test]
    fn test_from_filename() {
        let sym = Symbols::new();

        let name = to_filename(".");
        let ltype = LookupStrategy::from_filename(&sym, &name).unwrap();
        assert!(matches!(ltype, LookupStrategy::CurrentDir));

        let name = to_filename("..");
        let ltype = LookupStrategy::from_filename(&sym, &name).unwrap();
        assert!(matches!(ltype, LookupStrategy::ParentDir));

        let name = to_filename("foo");
        let expected_sym = sym.encode_symbol(&name).unwrap();
        let ltype = LookupStrategy::from_filename(&sym, &name).unwrap();
        assert!(matches!(ltype, LookupStrategy::Child(x) if x == expected_sym));
    }

    #[test]
    fn test_from_filename_no_symbol() {
        let sym = Symbols::new();
        let name = to_filename("not_found");
        let err = LookupStrategy::from_filename(&sym, &name).unwrap_err();
        assert!(matches!(err, NFS3ERR_NOENT));
    }

    #[test]
    fn test_lookup() {
        let sym = Symbols::new();
        let entry_name = to_filename("f1");
        let cur_sym = sym.encode_symbol(&entry_name).unwrap();

        let mut children = BTreeMap::new();
        let c1_name = to_filename("c1");
        let c1_sym = sym.encode_symbol(&c1_name).unwrap();
        children.insert(c1_sym, (5, Oid::zero()));
        let c2_name = to_filename("c2");
        let c2_sym = sym.encode_symbol(&c2_name).unwrap();
        children.insert(c2_sym, (7, Oid::zero()));

        let entry = FSObject {
            id: 2,
            oid: Oid::zero(),
            parent: 1,
            name: cur_sym,
            contents: EntryContent::Directory(DirectoryMetadata {
                path: PathBuf::new(),
            }),
            children,
            expanded: true,
            version: 1,
        };

        let cur_id = LookupStrategy::CurrentDir.lookup(&entry).unwrap();
        assert_eq!(entry.id, cur_id);

        let parent_id = LookupStrategy::ParentDir.lookup(&entry).unwrap();
        assert_eq!(entry.parent, parent_id);

        let c1_lookup = LookupStrategy::from_filename(&sym, &c1_name).unwrap();
        let c1_id = c1_lookup.lookup(&entry).unwrap();
        assert_eq!(5, c1_id);

        let c2_lookup = LookupStrategy::from_filename(&sym, &c2_name).unwrap();
        let c2_id = c2_lookup.lookup(&entry).unwrap();
        assert_eq!(7, c2_id);
    }

    #[test]
    fn test_lookup_not_found() {
        let sym = Symbols::new();
        let entry_name = to_filename("f1");
        let cur_sym = sym.encode_symbol(&entry_name).unwrap();

        let children = BTreeMap::new();

        let entry = FSObject {
            id: 1,
            oid: Oid::zero(),
            parent: 1,
            name: cur_sym,
            contents: EntryContent::Directory(DirectoryMetadata {
                path: PathBuf::new(),
            }),
            children,
            expanded: true,
            version: 1,
        };

        let err = LookupStrategy::Child(Symbol::new(56))
            .lookup(&entry)
            .unwrap_err();
        assert!(matches!(err, NFS3ERR_NOENT));
    }
}
