use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use anyhow::anyhow;
use git2::Oid;
use intaglio::Symbol;
use nfsserve::nfs::{fattr3, fileid3, filename3, nfsstat3};
use nfsserve::vfs::{DirEntry, ReadDirResult};
use nfsstat3::NFS3ERR_IO;
use tracing::info;

use cache::StatCache;
use symbol::Symbols;

use error_printer::ErrorPrinter;
use crate::xetmnt::watch::contents::EntryContent;
use crate::xetmnt::watch::metadata::filesystem::{FileSystem, LookupStrategy};

mod cache;
mod filesystem;
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
    fs: RwLock<FileSystem>,
    symbol_table: Symbols,
    statcache: StatCache,
    srcpath: PathBuf,
}

impl FSMetadata {
    /// Constructs a new FSMetadata for the given filesystem path and git object id.
    pub fn new(src_path: &Path, root_oid: Oid) -> Result<Self, anyhow::Error> {
        info!("Opening FSTree at: {src_path:?}");
        let symbol_table = Symbols::new();
        let default_sym = symbol_table
            .default_symbol()
            .map_err(|_| anyhow!("unknown issue with Symbol Table"))?;
        let fs = FileSystem::new(src_path, root_oid, default_sym)
            .map_err(|_| anyhow!("couldn't build the internal filesystem"))?;
        Ok(Self {
            fs: RwLock::new(fs),
            symbol_table,
            statcache: StatCache::new(STAT_CACHE_SIZE),
            srcpath: src_path.to_path_buf(),
        })
    }

    pub fn get_root_id(&self) -> fileid3 {
        self.lock_read_fs().unwrap().get_root_id()
    }

    /// Updates the root oid
    pub fn update_root_oid(&self, oid: Oid) -> Result<fileid3, nfsstat3> {
        let mut fs = self.lock_write_fs()?;
        let root_id = fs.update_root_oid(&self.srcpath, oid)?;
        self.statcache.clear()?;
        Ok(root_id)
    }

    /// Checks the given fileId to see if it is expanded or not
    pub fn is_expanded(&self, id: fileid3) -> Result<bool, nfsstat3> {
        self.lock_read_fs().and_then(|fs| fs.is_expanded(id))
    }

    pub fn set_expanded(&self, id: fileid3) -> Result<(), nfsstat3> {
        self.lock_write_fs().and_then(|mut fs| fs.set_expanded(id))
    }

    pub fn get_entry(&self, id: fileid3) -> Result<FSObject, nfsstat3> {
        self.lock_read_fs()
            .and_then(|fs| fs.get_entry_ref(id).cloned())
    }

    pub fn insert_new_entry(
        &self,
        parent_id: fileid3,
        filename: &filename3,
        oid: Oid,
        contents: EntryContent,
    ) -> Result<fileid3, nfsstat3> {
        let sym = self.symbol_table.encode_symbol(filename)?;
        self.lock_write_fs()
            .and_then(|mut fs| fs.insert(parent_id, sym, oid, contents))
    }

    pub fn lookup_child_id(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        let lookup_type = LookupStrategy::from_filename(&self.symbol_table, filename)?;
        self.lock_read_fs()
            .and_then(|fs| fs.lookup(dirid, lookup_type))
    }

    pub fn list_children(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let fs = self.lock_read_fs()?;
        let (children, end) = fs.list_children(dirid, start_after, max_entries)?;
        // translate children ids to DirEntries
        let entries = children
            .iter()
            .map(|id| {
                let entry = fs.get_entry_ref(*id)?;
                let name = self.symbol_table.decode_symbol(entry.name)?;
                let attr = self.get_attr_with_lock(&fs, *id)?;
                Ok(DirEntry {
                    fileid: *id,
                    name,
                    attr,
                })
            })
            .collect::<Result<Vec<DirEntry>, nfsstat3>>()?;
        Ok(ReadDirResult { entries, end })
    }

    /// Gets file attributes for the indicated file, using an internal cache to store commonly
    /// fetched attributes.
    pub fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        if let Some(stat) = self.statcache.get(id)? {
            return Ok(stat);
        }
        let fs = self.lock_read_fs()?;
        self.get_attr_with_lock(&fs, id)
    }

    /// Like getattr, but also takes in a FileSystem object to fetch ids from. Needed to
    /// prevent re-entrancy within the fs-lock (e.g. if we've already acquired the lock.
    fn get_attr_with_lock(&self, fs: &FileSystem, id: fileid3) -> Result<fattr3, nfsstat3> {
        if let Some(stat) = self.statcache.get(id)? {
            return Ok(stat);
        }
        let attr = fs.getattr(id)?;
        self.statcache.put(id, attr)?;
        Ok(attr)
    }
}

/// Lock acquisition methods.
impl FSMetadata {
    /// Lock the fs for reads
    fn lock_read_fs(&self) -> Result<RwLockReadGuard<'_, FileSystem>, nfsstat3> {
        self.fs
            .read()
            .log_error("Couldn't open fs lock for read")
            .map_err(|_| NFS3ERR_IO)
    }

    /// Lock the fs for writes
    fn lock_write_fs(&self) -> Result<RwLockWriteGuard<'_, FileSystem>, nfsstat3> {
        self.fs
            .write()
            .log_error("Couldn't open fs lock for write")
            .map_err(|_| NFS3ERR_IO)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use nfsserve::nfs::nfsstat3::{NFS3ERR_BAD_COOKIE, NFS3ERR_NOENT, NFS3ERR_NOTDIR};
    use rand::Rng;
    use tempfile::TempDir;

    use crate::xetmnt::watch::contents::{DirectoryMetadata, EntryMetadata};

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

    // TODO: allow filename3 to implement From<&str> (needs external repo/crate change)
    fn to_filename(s: &str) -> filename3 {
        s.as_bytes().into()
    }

    fn get_test_fs() -> FSMetadata {
        let root_dir = TempDir::new().unwrap();
        let oid = get_root_oid();
        FSMetadata::new(root_dir.path(), oid).unwrap()
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
    fn test_lookup_curdir_not_expanded() {
        let fs = get_test_fs();
        let child_name = to_filename(".");
        let err = fs.lookup_child_id(1, &child_name).unwrap_err();
        assert!(matches!(err, NFS3ERR_IO)); // directory not expanded
    }

    #[test]
    fn test_lookup_child_id_error() {
        let fs = get_test_fs();
        let child_name = to_filename("file1.txt");
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
