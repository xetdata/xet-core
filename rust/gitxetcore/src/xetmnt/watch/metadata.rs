use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::fs::Permissions;
use std::ops::Bound;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use anyhow::anyhow;
use git2::Oid;
use intaglio::osstr::SymbolTable;
use intaglio::Symbol;
use lru::LruCache;
use nfsserve::nfs::nfsstat3::{NFS3ERR_BAD_COOKIE, NFS3ERR_INVAL, NFS3ERR_NOENT, NFS3ERR_NOTDIR};
use nfsserve::nfs::{fattr3, fileid3, filename3, ftype3, nfsstat3, nfstime3, specdata3};
use nfsserve::vfs::{DirEntry, ReadDirResult};
use nfsstat3::NFS3ERR_IO;
use tracing::{error, info};

use pointer_file::PointerFile;

use crate::log::ErrorPrinter;
use crate::xetmnt::watch::metrics::MOUNT_NUM_OBJECTS;

const STAT_CACHE_SIZE: usize = 65536;

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
pub enum FileObject {
    XetFile((EntryMetadata, PointerFile)),
    RegularFile(EntryMetadata),
    Directory(DirectoryMetadata),
}

impl FileObject {
    fn getattr(&self, fs_metadata: &fs::Metadata, fid: fileid3) -> Result<fattr3, nfsstat3> {
        let (entrymeta, ftype, nlink) = match self {
            FileObject::XetFile((m, _)) => (Cow::Borrowed(m), ftype3::NF3REG, 1),
            FileObject::RegularFile(m) => (Cow::Borrowed(m), ftype3::NF3REG, 1),
            FileObject::Directory(_) => (Cow::default(), ftype3::NF3DIR, 2),
        };
        Ok(self.attr_os(fs_metadata, fid, ftype, nlink, entrymeta))
    }
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
impl FileObject {
    fn attr_os(
        &self,
        _fs_metadata: &fs::Metadata,
        fid: fileid3,
        ftype: ftype3,
        nlink: u32,
        entrymeta: Cow<EntryMetadata>,
    ) -> fattr3 {
        let mode = match self {
            FileObject::Directory(_) => 0o0511,
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

#[derive(Debug, Clone)]
pub struct FSObject {
    pub id: fileid3,
    pub oid: Oid,
    pub parent: fileid3,
    pub name: Symbol,
    pub contents: FileObject,
    pub children: BTreeMap<Symbol, (fileid3, Oid)>,
    pub expanded: bool,
}

/// Manages metadata for the mounted filesystem. This struct is safe to be used concurrently.
///
/// Internally, objects in the filesystem are held in a flat list of FSObject (i.e. inode),
/// where each object's index in the list corresponds to its id.
pub struct FSMetadata {
    fs: RwLock<Vec<FSObject>>,
    intern: RwLock<SymbolTable>,
    statcache: RwLock<LruCache<fileid3, fattr3>>,
    fs_metadata: fs::Metadata,
    root_id: fileid3,
    srcpath: PathBuf,
}

impl FSMetadata {
    /// Constructs a new FSMetadata for the given filesystem path and git object id.
    pub fn new(src_path: &Path, root_oid: Oid) -> Result<Self, anyhow::Error> {
        info!("Opening FSTree at: {src_path:?}");
        let metadata = src_path
            .metadata()
            .log_error(format!("Unable to get metadata for: {src_path:?}"))
            .map_err(|_| anyhow!("unable to get metadata for directory at: {src_path:?}"))?;
        let (fs_vec, intern, root_id) = Self::init_root_nodes(src_path, root_oid);
        Ok(Self {
            fs: RwLock::new(fs_vec),
            intern: RwLock::new(intern),
            statcache: RwLock::new(LruCache::new(STAT_CACHE_SIZE)),
            fs_metadata: metadata,
            root_id,
            srcpath: src_path.to_path_buf(),
        })
    }

    /// Initializes the filesystem root directory from the given path and git object id.
    /// This involves setting up 2 objects: a magic "0-id" node and the actual node for
    /// the root of the mount.
    ///
    /// Returns the (FSObject list, symbol table, root node id).
    fn init_root_nodes(src_path: &Path, root_oid: Oid) -> (Vec<FSObject>, SymbolTable, fileid3) {
        let mut fs = Vec::new();
        let mut intern = SymbolTable::new();
        let dir_meta = DirectoryMetadata {
            path: src_path.to_path_buf(),
        };
        // Panic safety: since intern() only returns an error if the symbol table
        // is full (i.e. u32::MAX) and we just created a new symbol table, this unwrap()
        // will not panic.
        let sym = intern.intern(OsString::default()).unwrap();
        // Add magic 0 object
        fs.push(FSObject {
            id: 0,
            oid: root_oid,
            parent: 0,
            name: sym,
            contents: FileObject::Directory(dir_meta.clone()),
            children: BTreeMap::new(),
            expanded: false,
        });

        // Add the root node
        let rootid = fs.len() as fileid3;
        fs.push(FSObject {
            id: rootid,
            oid: root_oid,
            parent: rootid, // parent of root is root
            name: sym,
            contents: FileObject::Directory(dir_meta),
            children: BTreeMap::new(),
            expanded: false,
        });
        (fs, intern, rootid)
    }

    pub fn get_root_id(&self) -> fileid3 {
        self.root_id
    }

    /// Checks the given fileId to see if it is expanded or not
    pub fn is_expanded(&self, id: fileid3) -> Result<bool, nfsstat3> {
        let fs = self.lock_read_fs()?;
        fs.get(id as usize)
            .map(|entry| entry.expanded)
            .ok_or(NFS3ERR_NOENT)
    }

    pub fn get_entry(&self, id: fileid3) -> Result<FSObject, nfsstat3> {
        self.try_get_entry(id)
            .and_then(|maybe_object| maybe_object.ok_or(NFS3ERR_NOENT))
    }

    fn try_get_entry(&self, id: fileid3) -> Result<Option<FSObject>, nfsstat3> {
        // Not a fan of the `cloned` value, however, it is here to minimize scope of the fs read lock.
        self.lock_read_fs().map(|fs| fs.get(id as usize).cloned())
    }

    pub fn set_expanded(&self, id: fileid3) -> Result<(), nfsstat3> {
        let mut fs = self.lock_write_fs()?;
        fs.get_mut(id as usize)
            .ok_or(NFS3ERR_NOENT)
            .map(|entry| entry.expanded = true)
    }

    pub fn insert_new_entry(
        &self,
        sym: Symbol,
        parent_id: fileid3,
        oid: Oid,
        contents: FileObject,
    ) -> Result<fileid3, nfsstat3> {
        let mut fs = self.lock_write_fs()?;
        let id = fs.len() as fileid3;
        let parent = fs.get_mut(parent_id as usize).ok_or(NFS3ERR_NOENT)?;
        parent.children.insert(sym, (id, oid));
        let is_dir = matches!(contents, FileObject::Directory(_));
        fs.push(FSObject {
            id,
            oid,
            parent: parent_id,
            name: sym,
            contents,
            children: BTreeMap::new(),
            expanded: !is_dir, // if this is a directory it is not expanded
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
        if !matches!(entry.contents, FileObject::Directory(_)) {
            return Err(NFS3ERR_NOTDIR);
        }
        match filename[..] {
            [b'.'] => Ok(dirid),              // '.' => current directory
            [b'.', b'.'] => Ok(entry.parent), // '..' => parent directory
            _ => self
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
        if !matches!(entry.contents, FileObject::Directory(_)) {
            return Err(NFS3ERR_NOTDIR);
        }
        let mut ret = ReadDirResult {
            entries: Vec::new(),
            end: false,
        };
        let range_start = if start_after > 0 {
            Bound::Excluded(self.get_child_symbol_from_entry(&entry, start_after)?)
        } else {
            Bound::Unbounded
        };

        let remaining_length = entry
            .children
            .range((range_start, Bound::Unbounded))
            .count();

        // Note: we might want to lock the fs/intern/statcache outside the loop to
        // possibly save on lock/unlock time, but that complicates the implementation
        // as we need to deal with the lack of re-entrant locks:
        // "lock.read() might panic when called if the lock is already held by the current thread"
        ret.entries = entry
            .children
            .range((range_start, Bound::Unbounded))
            .take(max_entries)
            .map(|(_, (id, _))| self.get_dir_entry(id))
            .collect::<Result<Vec<DirEntry>, nfsstat3>>()?;

        if ret.entries.len() == remaining_length {
            ret.end = true;
        }

        Ok(ret)
    }

    fn get_dir_entry(&self, id: &fileid3) -> Result<DirEntry, nfsstat3> {
        let entry = self
            .try_get_entry(*id)
            .and_then(|maybe_entry| maybe_entry.ok_or(NFS3ERR_BAD_COOKIE))?;
        let attr = self.getattr(*id)?;
        let name = self.decode_symbol(entry.name)?;
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

    pub fn encode_symbol(&self, name: &filename3) -> Result<Symbol, nfsstat3> {
        let os_str = Self::filename_to_os_string(name)?;
        self.lock_write_intern()
            .and_then(|mut intern| intern.intern(os_str).map_err(|_| NFS3ERR_IO))
    }

    fn decode_symbol(&self, sym: Symbol) -> Result<filename3, nfsstat3> {
        self.lock_read_intern()?
            .get(sym)
            .and_then(OsStr::to_str)
            .map(str::as_bytes)
            .map(filename3::from)
            .ok_or(NFS3ERR_IO)
    }

    pub fn get_symbol(&self, name: &filename3) -> Result<Option<Symbol>, nfsstat3> {
        let os_str = Self::filename_to_os_string(name)?;
        self.lock_read_intern()
            .map(|intern| intern.check_interned(&os_str))
    }

    fn filename_to_os_string(name: &filename3) -> Result<OsString, nfsstat3> {
        std::str::from_utf8(name)
            .map_err(|_| NFS3ERR_INVAL)
            .and_then(|s| OsString::from_str(s).map_err(|_| NFS3ERR_INVAL))
    }

    pub fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        if let Some(stat) = self.cache_getattr(id)? {
            return Ok(stat);
        }
        let entry = self.get_entry(id)?;
        info!("Getattr {:?}", entry);
        let attr = entry.contents.getattr(&self.fs_metadata, entry.id)?;
        self.cache_setattr(id, attr)?;
        Ok(attr)
    }

    fn cache_getattr(&self, id: fileid3) -> Result<Option<fattr3>, nfsstat3> {
        // annoyingly this LRU cache implementation is not thread-safe and thus, requires mut
        // on a read. Ostensibly to update the read count.
        self.lock_write_statcache()
            .map(|mut cache| cache.get(&id).cloned())
    }

    fn cache_setattr(&self, id: fileid3, attr: fattr3) -> Result<(), nfsstat3> {
        self.lock_write_statcache().map(|mut cache| {
            cache.put(id, attr);
        })
    }
}

/// Lock acquisition methods.
/// Since we have multiple locks, we should take care to acquire them in a specific order to
/// avoid deadlocks. These methods also wrap error handling of a poisoned lock.
///
/// The order of lock acquisition is:
/// - fs
/// - intern
/// - statcache
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

    /// Lock the intern for reads
    fn lock_read_intern(&self) -> Result<RwLockReadGuard<'_, SymbolTable>, nfsstat3> {
        self.intern
            .read()
            .log_error("Couldn't open intern lock for read")
            .map_err(|_| NFS3ERR_IO)
    }

    /// Lock the intern for writes
    fn lock_write_intern(&self) -> Result<RwLockWriteGuard<'_, SymbolTable>, nfsstat3> {
        self.intern
            .write()
            .log_error("Couldn't open intern lock for write")
            .map_err(|_| NFS3ERR_IO)
    }

    /// Lock the statcache for writes
    fn lock_write_statcache(
        &self,
    ) -> Result<RwLockWriteGuard<'_, LruCache<fileid3, fattr3>>, nfsstat3> {
        self.statcache
            .write()
            .log_error("Couldn't open statcache lock for write")
            .map_err(|_| NFS3ERR_IO)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::Rng;
    use tempfile::TempDir;

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
            assert!(matches!(obj.contents, FileObject::Directory(_)));
            assert_eq!(get_root_oid(), obj.oid);
            assert!(!obj.expanded);
        }
    }

    #[test]
    fn test_fs_add_entry() {
        let fs = get_test_fs();
        let child_oid = get_rand_oid();
        let sym = fs.encode_symbol(&to_filename("file1.txt")).unwrap();
        let ch_id = fs
            .insert_new_entry(
                sym,
                1,
                child_oid,
                FileObject::RegularFile(EntryMetadata {
                    size: 100,
                    mode: 0o0644,
                }),
            )
            .unwrap();

        let child_node = fs.get_entry(ch_id).unwrap();
        assert_eq!(ch_id, child_node.id);
        assert_eq!(child_oid, child_node.oid);
        assert_eq!(1, child_node.parent);
        assert!(matches!(child_node.contents, FileObject::RegularFile(_)));
        assert!(child_node.expanded);

        let parent_node = fs.get_entry(1).unwrap();
        let (link_id, link_oid) = parent_node.children.get(&sym).unwrap();
        assert_eq!(ch_id, *link_id);
        assert_eq!(child_oid, *link_oid);
    }

    #[test]
    fn test_symbol_encoding() {
        let fs = get_test_fs();

        let w1 = to_filename("file1.txt");
        let w2 = to_filename("dir-2");
        let w3 = to_filename("file1.txt");
        let s1 = fs.encode_symbol(&w1).unwrap();
        let s2 = fs.encode_symbol(&w2).unwrap();
        let s3 = fs.encode_symbol(&w3).unwrap();

        assert_eq!(s1, s3); // symbol table should reduce duplicates.

        // check symbols are present
        let f1 = fs.get_symbol(&w1).unwrap().unwrap();
        assert_eq!(s1, f1);
        let f2 = fs.get_symbol(&w2).unwrap().unwrap();
        assert_eq!(s2, f2);

        // check decoding of symbols
        let f1 = fs.decode_symbol(s1).unwrap();
        assert_eq!(w1.as_ref(), f1.as_ref());
        let f2 = fs.decode_symbol(s2).unwrap();
        assert_eq!(w2.as_ref(), f2.as_ref());
    }

    #[test]
    fn test_symbol_error() {
        let fs = get_test_fs();
        let filename = vec![0, 159, 146, 150].into();
        let err = fs.encode_symbol(&filename).unwrap_err();
        assert!(matches!(err, NFS3ERR_INVAL));
        let err = fs.get_symbol(&filename).unwrap_err();
        assert!(matches!(err, NFS3ERR_INVAL));

        let err = fs.decode_symbol(Symbol::new(5372)).unwrap_err();
        assert!(matches!(err, NFS3ERR_IO));
    }

    #[test]
    fn test_symbol_not_found() {
        let fs = get_test_fs();
        let filename = to_filename("not_found");
        let maybe_symbol = fs.get_symbol(&filename).unwrap();
        assert!(maybe_symbol.is_none());
    }

    #[test]
    fn test_lookup_child_id() {
        let fs = get_test_fs();
        let child_oid = get_rand_oid();
        let child_name = to_filename("file1.txt");
        let sym = fs.encode_symbol(&child_name).unwrap();
        let ch_id = fs
            .insert_new_entry(
                sym,
                1,
                child_oid,
                FileObject::RegularFile(EntryMetadata {
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
        let sym = fs.encode_symbol(&child_name).unwrap();
        let ch_id = fs
            .insert_new_entry(
                sym,
                1,
                child_oid,
                FileObject::Directory(DirectoryMetadata {
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
        let name = to_filename(&format!("dir1"));
        let sym = fs.encode_symbol(&name).unwrap();
        let dir_id = fs
            .insert_new_entry(
                sym,
                1,
                oid,
                FileObject::Directory(DirectoryMetadata {
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
                let sym = fs.encode_symbol(&name).unwrap();
                let size = 1024 * i as u64;
                let contents = FileObject::RegularFile(EntryMetadata { size, mode: 0o0644 });
                let id = fs.insert_new_entry(sym, parent_id, oid, contents).unwrap();
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
