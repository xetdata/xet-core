use std::collections::BTreeMap;
use std::fs::Metadata;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use anyhow::anyhow;
use git2::Oid;
use intaglio::Symbol;
use nfsserve::nfs::nfsstat3::{
    NFS3ERR_BAD_COOKIE, NFS3ERR_NOENT, NFS3ERR_NOTDIR, NFS3ERR_STALE,
};
use nfsserve::nfs::{fattr3, fileid3, filename3, nfsstat3};
use nfsserve::vfs::{DirEntry, ReadDirResult};
use nfsstat3::NFS3ERR_IO;
use tracing::{error, info};
use cache::StatCache;
use symbol::Symbols;

use crate::log::ErrorPrinter;
use crate::xetmnt::watch::contents::{DirectoryMetadata, EntryContent};
use crate::xetmnt::watch::metrics::MOUNT_NUM_OBJECTS;

mod cache;
mod symbol;

const STAT_CACHE_SIZE: usize = 65536;

/// Internal representation of an object in the filesystem.
#[derive(Debug, Clone)]
pub struct FSObject {
    pub id: fileid3,
    pub oid: Oid,
    pub parent: fileid3,
    pub name: Symbol,
    pub contents: EntryContent,
    pub children: BTreeMap<Symbol, (fileid3, Oid)>,
    pub expanded: bool,
    version: u64,
}

/// Manages metadata for the mounted filesystem. This struct is safe to be used concurrently.
///
/// Internally, objects in the filesystem are held in a flat list of FSObject (i.e. inode),
/// where each object's index in the list corresponds to its id.
pub struct FSMetadata {
    fs: RwLock<Vec<FSObject>>,
    symbol_table: Symbols,
    statcache: StatCache,
    fs_metadata: RwLock<Metadata>, // TODO: should be grouped with fs under a lock
    // TODO: we may want the fs_version to be part of the file handle to make staleness
    //       detection easier and allow reclaiming the fs vector.
    fs_version: AtomicU64,         // TODO: should be grouped with fs under a lock
    root_id: fileid3,
    srcpath: PathBuf,
}

impl FSMetadata {
    /// Constructs a new FSMetadata for the given filesystem path and git object id.
    pub fn new(src_path: &Path, root_oid: Oid) -> Result<Self, anyhow::Error> {
        info!("Opening FSTree at: {src_path:?}");
        let metadata = Self::get_fs_metadata(src_path)
            .map_err(|_| anyhow!("can't read metadata for {src_path:?}"))?;
        let symbol_table = Symbols::new();
        let default_sym = symbol_table.default_symbol().map_err(|_| anyhow!("unknown issue with Symbol Table"))?;
        let (fs_vec, root_id) = Self::init_root_nodes(src_path, root_oid, default_sym);
        Ok(Self {
            fs: RwLock::new(fs_vec),
            symbol_table,
            statcache: StatCache::new(STAT_CACHE_SIZE),
            fs_metadata: RwLock::new(metadata),
            fs_version: AtomicU64::new(0),
            root_id,
            srcpath: src_path.to_path_buf(),
        })
    }

    /// Initializes the filesystem root directory from the given path and git object id.
    /// This involves setting up 2 objects: a magic "0-id" node and the actual node for
    /// the root of the mount.
    ///
    /// Returns the (FSObject list, symbol table, root node id).
    fn init_root_nodes(src_path: &Path, root_oid: Oid, sym: Symbol) -> (Vec<FSObject>, fileid3) {
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
            version: 0,
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
            version: 0,
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

    pub fn get_root_id(&self) -> fileid3 {
        self.root_id
    }

    /// Updates the root oid
    pub fn update_root_oid(&self, oid: Oid) -> Result<fileid3, nfsstat3> {
        let (mut fs, mut fs_metadata) = self.lock_write_fs_and_fs_metadata()?;
        self.fs_version.fetch_add(1, Ordering::AcqRel);
        self.statcache.clear()?;
        fs.get_mut(self.root_id as usize)
            .ok_or(NFS3ERR_IO)
            .log_error("BUG: root node not found")
            .map(|root| {
                root.oid = oid;
                root.children = BTreeMap::new();
                root.expanded = false;
                root.version = self.fs_version.load(Ordering::Acquire);
            })?;
        let metadata = Self::get_fs_metadata(&self.srcpath)?;
        *fs_metadata = metadata;
        Ok(self.root_id)
    }

    /// Checks the given fileId to see if it is expanded or not
    pub fn is_expanded(&self, id: fileid3) -> Result<bool, nfsstat3> {
        let fs = self.lock_read_fs()?;
        fs.get(id as usize)
            .map(|entry| entry.expanded)
            .ok_or(NFS3ERR_NOENT)
    }

    pub fn set_expanded(&self, id: fileid3) -> Result<(), nfsstat3> {
        let mut fs = self.lock_write_fs()?;
        fs.get_mut(id as usize)
            .ok_or(NFS3ERR_NOENT)
            .map(|entry| entry.expanded = true)
    }

    pub fn get_entry(&self, id: fileid3) -> Result<FSObject, nfsstat3> {
        self.try_get_entry(id)
            .and_then(|maybe_object| maybe_object.ok_or(NFS3ERR_NOENT))
    }

    fn try_get_entry(&self, id: fileid3) -> Result<Option<FSObject>, nfsstat3> {
        let fs = self.lock_read_fs()?;
        let maybe_object = fs.get(id as usize);
        // check that object is not stale / part of an older commit.
        if let Some(f) = maybe_object {
            if f.version != self.fs_version.load(Ordering::Acquire) {
                return Err(NFS3ERR_STALE);
            }
        }
        // Not a fan of the `cloned` value, however, it is here to minimize scope of the fs lock.
        Ok(maybe_object.cloned())
    }

    pub fn insert_new_entry(
        &self,
        parent_id: fileid3,
        filename: &filename3,
        oid: Oid,
        contents: EntryContent,
    ) -> Result<fileid3, nfsstat3> {
        let sym = self.symbol_table.encode_symbol(filename)?;

        let mut fs = self.lock_write_fs()?;
        let id = fs.len() as fileid3;
        let parent = fs.get_mut(parent_id as usize).ok_or(NFS3ERR_NOENT)?;
        let version = parent.version;
        if version != self.fs_version.load(Ordering::Acquire) {
            return Err(NFS3ERR_STALE);
        }
        parent.children.insert(sym, (id, oid));
        let is_dir = matches!(contents, EntryContent::Directory(_));
        fs.push(FSObject {
            id,
            oid,
            parent: parent_id,
            name: sym,
            contents,
            children: BTreeMap::new(),
            expanded: !is_dir, // if this is a directory it is not expanded
            version,
        });
        MOUNT_NUM_OBJECTS.inc();
        Ok(id)
    }

    pub fn lookup_child_id(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        let entry = self.get_entry(dirid)?;
        if !entry.expanded {
            error!("BUG: directory: {dirid:?} not expanded before calling `lookup_child_id()`");
            return Err(NFS3ERR_IO);
        }
        if !matches!(entry.contents, EntryContent::Directory(_)) {
            return Err(NFS3ERR_NOTDIR);
        }
        match filename[..] {
            [b'.'] => Ok(dirid),              // '.' => current directory
            [b'.', b'.'] => Ok(entry.parent), // '..' => parent directory
            _ => self.symbol_table
                .get_symbol(filename)?
                .and_then(|sym| entry.children.get(&sym))
                .map(|(fid, _)| *fid)
                .ok_or(NFS3ERR_NOENT),
        }
    }

    pub fn list_children(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let entry = self.get_entry(dirid)?;
        if !entry.expanded {
            error!("BUG: directory: {dirid:?} not expanded before calling `list_children()`");
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
            .map(|(_, (id, _))| self.get_dir_entry(id))
            .collect::<Result<Vec<DirEntry>, nfsstat3>>()?;

        let end = entries.len() == remaining_length;
        Ok(ReadDirResult { entries, end })
    }

    fn get_dir_entry(&self, id: &fileid3) -> Result<DirEntry, nfsstat3> {
        let entry = self
            .try_get_entry(*id)
            .and_then(|maybe_entry| maybe_entry.ok_or(NFS3ERR_BAD_COOKIE))?;
        let name = self.symbol_table.decode_symbol(entry.name)?;
        let attr = self.getattr(*id)?;
        Ok(DirEntry {
            fileid: *id,
            name,
            attr,
        })
    }

    fn get_child_symbol_from_entry(
        &self,
        entry: &FSObject,
        id: fileid3,
    ) -> Result<Symbol, nfsstat3> {
        self.try_get_entry(id)
            .and_then(|maybe_entry| maybe_entry.ok_or(NFS3ERR_BAD_COOKIE))
            .map(|file_entry| file_entry.name)
            .and_then(|name| {
                entry
                    .children
                    .get(&name)
                    .map(|_| name)
                    .ok_or(NFS3ERR_BAD_COOKIE)
            })
    }

    pub fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        if let Some(stat) = self.statcache.get(id)? {
            return Ok(stat);
        }
        let entry = self.get_entry(id)?;
        info!("Getattr {:?}", entry);

        let attr = {
            let fs_meta = self.lock_read_fs_metadata()?;
            entry.contents.getattr(&fs_meta, entry.id)?
        };
        self.statcache.put(id, attr)?;
        Ok(attr)
    }
}

/// Lock acquisition methods.
/// Since we have multiple locks, we should take care to acquire them in a specific order to
/// avoid deadlocks. These methods also wrap error handling of a poisoned lock.
///
/// The order of lock acquisition is:
/// - fs
/// - fs_metadata
impl FSMetadata {
    /// Lock the fs for reads
    fn lock_read_fs(&self) -> Result<RwLockReadGuard<'_, Vec<FSObject>>, nfsstat3> {
        self.fs
            .read()
            .log_error("Couldn't open fs lock for read")
            .map_err(|_| NFS3ERR_IO)
    }

    /// Lock the fs for writes
    fn lock_write_fs(&self) -> Result<RwLockWriteGuard<'_, Vec<FSObject>>, nfsstat3> {
        self.fs
            .write()
            .log_error("Couldn't open fs lock for write")
            .map_err(|_| NFS3ERR_IO)
    }

    /// Lock the fs_metadata for reads
    fn lock_read_fs_metadata(&self) -> Result<RwLockReadGuard<'_, Metadata>, nfsstat3> {
        self.fs_metadata
            .read()
            .log_error("Couldn't open fs_metadata lock for read")
            .map_err(|_| NFS3ERR_IO)
    }

    /// Lock the fs_metadata for writes
    fn lock_write_fs_metadata(&self) -> Result<RwLockWriteGuard<'_, Metadata>, nfsstat3> {
        self.fs_metadata
            .write()
            .log_error("Couldn't open fs_metadata lock for write")
            .map_err(|_| NFS3ERR_IO)
    }

    /// Lock both the fs and fs_metadata for writes
    fn lock_write_fs_and_fs_metadata(
        &self,
    ) -> Result<
        (
            RwLockWriteGuard<'_, Vec<FSObject>>,
            RwLockWriteGuard<'_, Metadata>,
        ),
        nfsstat3,
    > {
        let fs_lock = self.lock_write_fs()?;
        let fs_metadata_lock = self.lock_write_fs_metadata()?;
        Ok((fs_lock, fs_metadata_lock))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::Rng;
    use tempfile::TempDir;

    use crate::xetmnt::watch::contents::EntryMetadata;

    use super::*;

    const ROOT_OID: &str = "d0b22188428e4098f5036f7940ebadb27d161f4c";

    fn get_root_oid() -> Oid {
        Oid::from_str(ROOT_OID).unwrap()
    }

    fn get_rand_oid() -> Oid {
        let mut rng = rand::thread_rng();
        let s: String = (0..20)
            .map(|_| rng.gen_range(0..=15))
            .map(|n| format!("{:x}", n))
            .collect();

        Oid::from_str(&s).unwrap()
    }

    fn to_filename(s: &str) -> filename3 {
        s.as_bytes().into()
    }

    fn get_test_fs() -> FSMetadata {
        let root_dir = TempDir::new().unwrap();
        let oid = get_root_oid();
        FSMetadata::new(root_dir.path(), oid).unwrap()
    }

    #[test]
    fn test_fs_new() {
        let fs = get_test_fs();
        let obj_list = fs.fs.read().unwrap();
        assert_eq!(1, fs.get_root_id());
        assert_eq!(2, obj_list.len());
        for (i, obj) in obj_list.iter().enumerate() {
            assert_eq!(i, obj.id as usize);
            assert!(matches!(obj.contents, EntryContent::Directory(_)));
            assert_eq!(get_root_oid(), obj.oid);
            assert!(!obj.expanded);
        }
    }

    #[test]
    fn test_fs_add_entry() {
        let fs = get_test_fs();
        let child_oid = get_rand_oid();
        let name = to_filename("file1.txt");
        let ch_id = fs
            .insert_new_entry(
                1,
                &name,
                child_oid,
                EntryContent::RegularFile(EntryMetadata {
                    size: 100,
                    mode: 0o0644,
                }),
            )
            .unwrap();

        let child_node = fs.get_entry(ch_id).unwrap();
        assert_eq!(ch_id, child_node.id);
        assert_eq!(child_oid, child_node.oid);
        assert_eq!(1, child_node.parent);
        assert!(matches!(child_node.contents, EntryContent::RegularFile(_)));
        assert!(child_node.expanded);

        let parent_node = fs.get_entry(1).unwrap();
        let sym = fs.symbol_table.get_symbol(&name).unwrap().unwrap();
        let (link_id, link_oid) = parent_node.children.get(&sym).unwrap();
        assert_eq!(ch_id, *link_id);
        assert_eq!(child_oid, *link_oid);
    }

    #[test]
    fn test_lookup_child_id() {
        let fs = get_test_fs();
        let child_oid = get_rand_oid();
        let child_name = to_filename("file1.txt");
        let ch_id = fs
            .insert_new_entry(
                1,
                &child_name,
                child_oid,
                EntryContent::RegularFile(EntryMetadata {
                    size: 100,
                    mode: 0o0644,
                }),
            )
            .unwrap();
        fs.set_expanded(1).unwrap();
        let actual_id = fs.lookup_child_id(1, &child_name).unwrap();
        assert_eq!(ch_id, actual_id);
    }

    #[test]
    fn test_lookup_current_dir() {
        let fs = get_test_fs();
        let cur_dir = to_filename(".");
        fs.set_expanded(1).unwrap();
        let actual_id = fs.lookup_child_id(1, &cur_dir).unwrap();
        assert_eq!(1, actual_id);
    }

    #[test]
    fn test_parent_dir() {
        let fs = get_test_fs();
        let child_oid = get_rand_oid();
        let child_name = to_filename("dir1");
        let ch_id = fs
            .insert_new_entry(
                1,
                &child_name,
                child_oid,
                EntryContent::Directory(DirectoryMetadata {
                    path: PathBuf::from("dir1"),
                }),
            )
            .unwrap();
        fs.set_expanded(ch_id).unwrap();
        let parent_dir = to_filename("..");
        let actual_id = fs.lookup_child_id(ch_id, &parent_dir).unwrap();
        assert_eq!(1, actual_id);
    }

    #[test]
    fn test_lookup_child_id_error() {
        let fs = get_test_fs();
        let child_name = to_filename("file1.txt");
        let err = fs.lookup_child_id(1, &child_name).unwrap_err();
        assert!(matches!(err, NFS3ERR_IO)); // directory not expanded
        fs.set_expanded(1).unwrap();
        let err = fs.lookup_child_id(1, &child_name).unwrap_err();
        assert!(matches!(err, NFS3ERR_NOENT)); // child not found
    }

    #[test]
    fn test_lookup_child_not_dir() {
        let fs = get_test_fs();
        let children = insert_random_files(&fs, 1, 1);
        let cur_dir = to_filename(".");
        let err = fs.lookup_child_id(children[0].0, &cur_dir).unwrap_err();
        assert!(matches!(err, NFS3ERR_NOTDIR)); // can't lookup children of file
    }

    #[test]
    fn test_list_children() {
        let fs = get_test_fs();
        let children = insert_random_files(&fs, 1, 2);
        let res = fs.list_children(1, 0, 10).unwrap();
        assert!(res.end);
        check_dir_entries(&children, &res.entries);
    }

    #[test]
    fn test_list_children_paginated() {
        let fs = get_test_fs();
        let children = insert_random_files(&fs, 1, 3);
        let page_size = 2;
        let res = fs.list_children(1, 0, page_size).unwrap();
        assert!(!res.end);
        check_dir_entries(&children[..page_size], &res.entries);
        // 2nd page should return last item
        let start_from_id = res.entries[1].fileid;
        let res = fs.list_children(1, start_from_id, page_size).unwrap();
        assert!(res.end);
        check_dir_entries(&children[page_size..], &res.entries);
        // check that trying to get "next" page returns nothing
        let res = fs
            .list_children(1, res.entries[0].fileid, page_size)
            .unwrap();
        assert!(res.end);
        assert_eq!(0, res.entries.len());

        // check ends on pagination boundary
        let res = fs.list_children(1, start_from_id, 1).unwrap();
        assert!(res.end);
        check_dir_entries(&children[page_size..], &res.entries);
    }

    #[test]
    fn test_list_children_basic_errors() {
        let fs = get_test_fs();
        let err = fs.list_children(1, 0, 2).unwrap_err();
        assert!(matches!(err, NFS3ERR_IO)); // not expanded

        let children = insert_random_files(&fs, 1, 1);
        let err = fs.list_children(children[0].0, 0, 2).unwrap_err();
        assert!(matches!(err, NFS3ERR_NOTDIR)); // can't list files

        let err = fs.list_children(1, 32, 2).unwrap_err();
        assert!(matches!(err, NFS3ERR_BAD_COOKIE)); // can't start from an unknown fileid
    }

    #[test]
    fn test_list_children_id_not_in_dir() {
        let fs = get_test_fs();
        let files = insert_random_files(&fs, 1, 1);

        // add a sub-dir
        let oid = get_root_oid();
        let name = to_filename("dir1");
        let dir_id = fs
            .insert_new_entry(
                1,
                &name,
                oid,
                EntryContent::Directory(DirectoryMetadata {
                    path: PathBuf::from("/foo"),
                }),
            )
            .unwrap();
        fs.set_expanded(dir_id).unwrap();

        let err = fs.list_children(dir_id, files[0].0, 2).unwrap_err();
        assert!(matches!(err, NFS3ERR_BAD_COOKIE)); // starting fileid is not a child of dir.
    }

    fn insert_random_files(
        fs: &FSMetadata,
        parent_id: fileid3,
        num_children: i32,
    ) -> Vec<(fileid3, filename3, u64)> {
        let vec = (0..num_children)
            .map(|i| {
                let oid = get_root_oid();
                let name = to_filename(&format!("file-{i:}.txt"));
                let size = 1024 * i as u64;
                let contents = EntryContent::RegularFile(EntryMetadata { size, mode: 0o0644 });
                let id = fs
                    .insert_new_entry(parent_id, &name, oid, contents)
                    .unwrap();
                (id, name, size)
            })
            .collect_vec();
        fs.set_expanded(parent_id).unwrap();
        vec
    }

    fn check_dir_entries(expected_values: &[(fileid3, filename3, u64)], entries: &[DirEntry]) {
        assert_eq!(expected_values.len(), entries.len());
        for (i, expected) in expected_values.iter().enumerate() {
            let actual = &entries[i];
            assert_eq!(actual.fileid, actual.attr.fileid);
            assert_eq!(expected.0, actual.fileid);
            assert_eq!(expected.1.as_ref(), actual.name.as_ref());
            assert_eq!(expected.2, actual.attr.size);
        }
    }
}
