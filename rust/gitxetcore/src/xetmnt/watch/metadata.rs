use crate::log::ErrorPrinter;
use crate::xetmnt::watch::metrics::MOUNT_NUM_OBJECTS;
use anyhow::anyhow;
use git2::Oid;
use intaglio::osstr::SymbolTable;
use intaglio::Symbol;
use lru::LruCache;
use nfsserve::nfs::nfsstat3::{NFS3ERR_BAD_COOKIE, NFS3ERR_INVAL, NFS3ERR_NOENT, NFS3ERR_NOTDIR};
use nfsserve::nfs::{fattr3, fileid3, filename3, ftype3, nfsstat3, nfstime3, specdata3};
use nfsserve::vfs::{DirEntry, ReadDirResult};
use nfsstat3::NFS3ERR_IO;
use pointer_file::PointerFile;
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::Permissions;
use std::ops::Bound;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{error, info};

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

/// Manages the filesystem object tree and metadata. This struct is safe to be used concurrently.
///
/// Internally, objects in the filesystem are held in a flat list of FSObject (i.e. inode),
/// where each object's index in the list corresponds to its id.
pub struct FSMetadata {
    fs: RwLock<Vec<FSObject>>,
    intern: RwLock<SymbolTable>,
    statcache: RwLock<LruCache<fileid3, fattr3>>,
    fs_metadata: std::fs::Metadata,
    root_id: fileid3,
    srcpath: PathBuf,
}

impl FSMetadata {
    /// Constructs a new FSTree for the given filesystem path and git object id.
    pub fn new(src_path: &Path, root_oid: Oid) -> Result<Self, anyhow::Error> {
        info!("Opening FSTree at: {src_path:?}");
        let metadata = src_path
            .metadata()
            .log_error("Unable to get metadata for srcpath")
            .map_err(|_| anyhow!("unable to get metadata for directory"))?;
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
    ) -> Result<(), nfsstat3> {
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
        Ok(())
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

    pub fn encode_symbol(&self, name: &str) -> Result<Symbol, nfsstat3> {
        let os_str = OsString::from_str(&name).map_err(|_| NFS3ERR_INVAL)?;
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
        let os_str = std::str::from_utf8(name)
            .map_err(|_| NFS3ERR_INVAL)
            .and_then(|s| OsString::from_str(s).map_err(|_| NFS3ERR_INVAL))?;
        self.lock_read_intern()
            .map(|intern| intern.check_interned(&os_str))
    }

    pub fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        if let Some(stat) = self.cache_getattr(id)? {
            return Ok(stat);
        }
        let entry = self.get_entry(id)?;
        info!("Getattr {:?}", entry);
        let attr = self.entry_to_attr(&entry)?;
        self.cache_setattr(id, attr)?;
        Ok(attr)
    }

    fn entry_to_attr(&self, entry: &FSObject) -> Result<fattr3, nfsstat3> {
        let attr = match &entry.contents {
            FileObject::XetFile((meta, _)) => self.path_to_fattr3(entry.id, meta.clone(), true),
            FileObject::RegularFile(meta) => self.path_to_fattr3(entry.id, meta.clone(), true),
            FileObject::Directory(_) => {
                self.path_to_fattr3(entry.id, EntryMetadata::default(), false)
            }
        }?;
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

/// OS-specific methods
impl FSMetadata {
    #[cfg(unix)]
    fn path_to_fattr3(
        &self,
        fid: fileid3,
        entrymeta: EntryMetadata,
        is_file: bool,
    ) -> Result<fattr3, nfsstat3> {
        let size = entrymeta.size;
        let mode = Self::mode_unmask_write(entrymeta.mode);
        let (ftype, nlink) = if is_file {
            (ftype3::NF3REG, 1)
        } else {
            (ftype3::NF3DIR, 2)
        };
        Ok(fattr3 {
            ftype,
            mode,
            nlink,
            uid: self.fs_metadata.uid(),
            gid: self.fs_metadata.gid(),
            size,
            used: size,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: fid,
            atime: nfstime3 {
                seconds: self.fs_metadata.atime() as u32,
                nseconds: self.fs_metadata.atime_nsec() as u32,
            },
            mtime: nfstime3 {
                seconds: self.fs_metadata.mtime() as u32,
                nseconds: self.fs_metadata.mtime_nsec() as u32,
            },
            ctime: nfstime3 {
                seconds: self.fs_metadata.ctime() as u32,
                nseconds: self.fs_metadata.ctime_nsec() as u32,
            },
        })
    }

    #[cfg(windows)]
    fn path_to_fattr3(
        &self,
        fid: fileid3,
        entrymeta: EntryMetadata,
        is_file: bool,
    ) -> Result<fattr3, nfsstat3> {
        let size = entrymeta.size;
        let (ftype, nlink, mode) = if is_file {
            (ftype3::NF3REG, 1, 0o0555)
        } else {
            (ftype3::NF3DIR, 2, 0o0511)
        };
        Ok(fattr3 {
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
        })
    }

    #[cfg(unix)]
    fn mode_unmask_write(mode: u32) -> u32 {
        let mut mode = Permissions::from_mode(mode);
        mode.set_readonly(true);
        mode.mode()
    }
}
