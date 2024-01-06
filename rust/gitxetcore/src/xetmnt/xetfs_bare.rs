use crate::config::XetConfig;
use crate::constants as gitxet_constants;
use crate::constants::POINTER_FILE_LIMIT;
use crate::data_processing::PointerFileTranslator;
use async_trait::async_trait;
use git2;
use intaglio::osstr::SymbolTable;
use intaglio::Symbol;
use lru::LruCache;
use nfsserve::nfs::*;
use nfsserve::vfs::*;
use pointer_file::PointerFile;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt::Debug;
#[cfg(unix)]
use std::fs::Permissions;
use std::ops::Bound;
use std::str::FromStr;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

use std::path;
use std::sync::RwLock;
use tracing::{debug, error, info};

use lazy_static::lazy_static;
use prometheus::{register_int_counter, register_int_gauge, IntCounter, IntGauge};

lazy_static! {
    pub static ref MOUNT_POINTER_BYTES_READ: IntCounter = register_int_counter!(
        "mount_pointer_bytes_read",
        "Number of bytes read that originate from a pointer file",
    )
    .unwrap();
    pub static ref MOUNT_PASSTHROUGH_BYTES_READ: IntCounter = register_int_counter!(
        "mount_passthrough_bytes_read",
        "Number of bytes read that originate from a passthrough file",
    )
    .unwrap();
    pub static ref MOUNT_NUM_OBJECTS: IntGauge =
        register_int_gauge!("mount_num_fs_objects", "Number of filesystem objects").unwrap();
}

const STAT_CACHE_SIZE: usize = 65536;
const PREFETCH_LOOKAHEAD: usize = gitxet_constants::PREFETCH_WINDOW_SIZE_BYTES as usize;

fn mode_unmask_write(mode: u32) -> u32 {
    #[cfg(unix)]
    {
        let mut mode = Permissions::from_mode(mode);
        mode.set_readonly(true);
        mode.mode()
    }

    #[cfg(windows)]
    {
        mode
    }
}

#[derive(Default, Debug, Clone)]
struct EntryMetadata {
    size: u64,
    mode: u32,
}

#[derive(Debug, Clone)]
struct DirectoryMetadata {
    path: path::PathBuf,
    oid: git2::Oid,
}

#[derive(Debug, Clone)]
enum FileObject {
    XetFile((EntryMetadata, PointerFile)),
    RegularFile((EntryMetadata, git2::Oid)),
    Directory(DirectoryMetadata),
}
#[derive(Debug, Clone)]
struct FSObject {
    #[allow(dead_code)]
    id: fileid3,
    parent: fileid3,
    name: Symbol,
    contents: FileObject,
    children: BTreeMap<Symbol, fileid3>,
    expanded: bool,
}

pub struct XetFSBare {
    fs: RwLock<Vec<FSObject>>,
    intern: RwLock<SymbolTable>,
    rootdir: fileid3,
    srcpath: path::PathBuf,
    pfilereader: PointerFileTranslator,
    statcache: RwLock<lru::LruCache<fileid3, fattr3>>,
    repo: tokio::sync::Mutex<git2::Repository>,
    gitref: String,
    metadata: std::fs::Metadata, // the metadata used to fill uid, gid, and times from
    prefetch: usize,
}

impl Debug for XetFSBare {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XetFSBare")
            .field("rootdir", &self.rootdir)
            .field("fs", &self.fs)
            .finish()
    }
}

impl XetFSBare {
    fn add_root(&mut self, tree_oid: git2::Oid) {
        let mut fs = self.fs.write().unwrap();
        let dirmeta = DirectoryMetadata {
            path: self.srcpath.to_path_buf(),
            oid: tree_oid,
        };
        // insert the magic 0 object
        let mut intern = self.intern.write().unwrap();
        assert_eq!(fs.len(), 0);
        fs.push(FSObject {
            id: 0,
            parent: 0,
            name: intern.intern(OsString::default()).unwrap(),
            contents: FileObject::Directory(dirmeta.clone()),
            children: BTreeMap::new(),
            expanded: false,
        });

        // insert the root path and set the rootid
        let sym = intern.intern(OsString::default()).unwrap();
        let rootid = fs.len() as fileid3;
        fs.push(FSObject {
            id: rootid,
            parent: rootid, // parent of root is root
            name: sym,
            contents: FileObject::Directory(dirmeta),
            children: BTreeMap::new(),
            expanded: false,
        });
        self.rootdir = rootid;
    }
    async fn expand_directory(&self, curid: fileid3) -> Result<(), anyhow::Error> {
        {
            // quick expanded check
            let fs = self.fs.read().unwrap();
            if fs[curid as usize].expanded {
                return Ok(());
            }
        }
        let repo = self.repo.lock().await;
        let mut fs = self.fs.write().unwrap();
        let mut intern = self.intern.write().unwrap();
        let cur_path;
        let oid_to_expand;
        if let FileObject::Directory(ref dirmeta) = fs[curid as usize].contents {
            oid_to_expand = dirmeta.oid;
            cur_path = dirmeta.path.clone();
        } else {
            return Err(anyhow::anyhow!("Attempting to expand non-directory object"));
        }

        let tree = repo.find_tree(oid_to_expand)?;
        for tree_ent in tree.iter() {
            let filename = String::from_utf8_lossy(tree_ent.name_bytes()).to_string();
            let filename_osstr = OsString::from_str(&filename).unwrap();
            let sym = intern.intern(filename_osstr).unwrap();
            let ent_path = cur_path.join(&filename);
            let is_directory = matches!(tree_ent.kind(), Some(git2::ObjectType::Tree));
            let maybe_contents = XetFSBare::oid_to_contents(
                &repo,
                &ent_path,
                tree_ent.filemode() as u32,
                tree_ent.id(),
                tree_ent.kind().unwrap(),
            );
            if let Ok(Some(contents)) = maybe_contents {
                let new_id = fs.len() as fileid3;
                fs[curid as usize].children.insert(sym, new_id);
                fs.push(FSObject {
                    id: new_id,
                    parent: curid,
                    name: sym,
                    contents,
                    children: BTreeMap::new(),
                    expanded: !is_directory, // if this is a directory it is not exanded
                });
            }
        }

        // update expanded flag
        fs[curid as usize].expanded = true;
        MOUNT_NUM_OBJECTS.set(fs.len() as i64);

        Ok(())
    }

    // The git tree provides a list of mode and oid
    fn oid_to_contents(
        repo: &git2::Repository,
        path: &path::Path,
        mode: u32,
        oid: git2::Oid,
        kind: git2::ObjectType,
    ) -> Result<Option<FileObject>, anyhow::Error> {
        match kind {
            git2::ObjectType::Any => {
                error!("Oid {:?} type Any not supported", oid);
                Ok(None)
            }
            git2::ObjectType::Commit => {
                error!("Oid {:?} type Commit not supported", oid);
                Ok(None)
            }
            git2::ObjectType::Tag => {
                error!("Oid {:?} type Tag not supported", oid);
                Ok(None)
            }
            git2::ObjectType::Tree => Ok(Some(FileObject::Directory(DirectoryMetadata {
                path: path.to_path_buf(),
                oid,
            }))),
            git2::ObjectType::Blob => Ok(Some(XetFSBare::blob_to_contents(repo, mode, oid)?)),
        }
    }
    fn blob_to_contents(
        repo: &git2::Repository,
        mode: u32,
        oid: git2::Oid,
    ) -> Result<FileObject, anyhow::Error> {
        let blob = repo.find_blob(oid)?;
        let blob_size = blob.size();
        if blob_size <= POINTER_FILE_LIMIT {
            let pointer = PointerFile::init_from_string(std::str::from_utf8(blob.content())?, "");
            if pointer.is_valid() {
                return Ok(FileObject::XetFile((
                    EntryMetadata {
                        size: pointer.filesize(),
                        mode,
                    },
                    pointer,
                )));
            }
        }
        // all fall through has it turn into a regular file
        Ok(FileObject::RegularFile((
            EntryMetadata {
                size: blob_size as u64,
                mode,
            },
            oid,
        )))
    }

    #[cfg(unix)]
    fn path_to_fattr3(
        &self,
        fid: fileid3,
        entrymeta: EntryMetadata,
        is_file: bool,
    ) -> Result<fattr3, nfsstat3> {
        let size = entrymeta.size;
        let file_mode = mode_unmask_write(entrymeta.mode);
        if is_file {
            Ok(fattr3 {
                ftype: ftype3::NF3REG,
                mode: file_mode,
                nlink: 1,
                uid: self.metadata.uid(),
                gid: self.metadata.gid(),
                size,
                used: size,
                rdev: specdata3::default(),
                fsid: 0,
                fileid: fid,
                atime: nfstime3 {
                    seconds: self.metadata.atime() as u32,
                    nseconds: self.metadata.atime_nsec() as u32,
                },
                mtime: nfstime3 {
                    seconds: self.metadata.mtime() as u32,
                    nseconds: self.metadata.mtime_nsec() as u32,
                },
                ctime: nfstime3 {
                    seconds: self.metadata.ctime() as u32,
                    nseconds: self.metadata.ctime_nsec() as u32,
                },
            })
        } else {
            Ok(fattr3 {
                ftype: ftype3::NF3DIR,
                mode: file_mode,
                nlink: 2,
                uid: self.metadata.uid(),
                gid: self.metadata.gid(),
                size: 0,
                used: 0,
                rdev: specdata3::default(),
                fsid: 0,
                fileid: fid,
                atime: nfstime3 {
                    seconds: self.metadata.atime() as u32,
                    nseconds: self.metadata.atime_nsec() as u32,
                },
                mtime: nfstime3 {
                    seconds: self.metadata.mtime() as u32,
                    nseconds: self.metadata.mtime_nsec() as u32,
                },
                ctime: nfstime3 {
                    seconds: self.metadata.ctime() as u32,
                    nseconds: self.metadata.ctime_nsec() as u32,
                },
            })
        }
    }

    #[cfg(windows)]
    fn path_to_fattr3(
        &self,
        fid: fileid3,
        entrymeta: EntryMetadata,
        is_file: bool,
    ) -> Result<fattr3, nfsstat3> {
        let size = entrymeta.size;
        let file_mode = mode_unmask_write(entrymeta.mode);
        if is_file {
            Ok(fattr3 {
                ftype: ftype3::NF3REG,
                mode: 0555,
                nlink: 1,
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
        } else {
            Ok(fattr3 {
                ftype: ftype3::NF3DIR,
                mode: 0511,
                nlink: 2,
                uid: 507,
                gid: 507,
                size: 0,
                used: 0,
                rdev: specdata3::default(),
                fsid: 0,
                fileid: fid,
                atime: nfstime3::default(),
                mtime: nfstime3::default(),
                ctime: nfstime3::default(),
            })
        }
    }

    pub async fn new(
        srcpath: &path::Path,
        cfg: &XetConfig,
        reference: &str,
        prefetch: usize,
    ) -> Result<XetFSBare, anyhow::Error> {
        debug!("Opening XetFS ReadOnly at {:?} {:?}", srcpath, reference);
        let pfile = PointerFileTranslator::from_config(cfg).await?;

        let repo = git2::Repository::discover(srcpath)?;

        // check that reference is a commit
        let oid = repo
            .revparse_single(reference)
            .map_err(|_| anyhow::anyhow!("Unable to resolve reference {}", reference))?
            .id();

        let commit_ = repo
            .find_commit(oid)
            .map_err(|_| anyhow::anyhow!("reference is not a commit"))?;

        let metadata = srcpath
            .metadata()
            .map_err(|_| anyhow::anyhow!("Unable to get directory metadata"))?;

        drop(commit_);
        let mut ret = XetFSBare {
            fs: RwLock::new(Vec::new()),
            intern: RwLock::new(SymbolTable::new()),
            rootdir: 0,
            srcpath: srcpath.to_path_buf(),
            pfilereader: pfile,
            statcache: RwLock::new(LruCache::new(STAT_CACHE_SIZE)),
            repo: tokio::sync::Mutex::new(repo),
            gitref: reference.into(),
            metadata,
            prefetch,
        };
        ret.init().await?;
        Ok(ret)
    }

    async fn find_root_tree_oid(&mut self) -> Result<git2::Oid, anyhow::Error> {
        let repo = self.repo.lock().await;

        // find the root ref
        let rev = repo.revparse_single(&self.gitref)?;
        if rev.kind().unwrap() != git2::ObjectType::Commit {
            return Err(anyhow::anyhow!(
                "Expecting reference {:?} to point to a commit",
                self.gitref
            ));
        }
        Ok(rev.as_commit().unwrap().tree_id())
    }

    pub async fn init(&mut self) -> Result<(), anyhow::Error> {
        let tree_oid = self.find_root_tree_oid().await?;
        self.add_root(tree_oid);
        Ok(())
    }

    pub fn num_objects(&self) -> usize {
        self.fs.read().unwrap().len()
    }
    pub fn total_object_size(&self) -> u64 {
        let objects = self.fs.read().unwrap();
        let mut sum: u64 = 0;
        for f in objects.iter() {
            sum += match &f.contents {
                FileObject::XetFile((meta, _)) => meta.size,
                FileObject::RegularFile((meta, _)) => meta.size,
                FileObject::Directory(_) => 0,
            };
        }
        sum
    }

    pub fn getattr_sync(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        {
            // annoyingly this LRU cache implementation requires mut on a read.
            // Ostensibly to update the read count
            let mut statread = self.statcache.write().unwrap();
            if let Some(stat) = statread.get(&id) {
                return Ok(*stat);
            }
        }
        let fs = self.fs.read().unwrap();
        let entry = fs.get(id as usize).ok_or(nfsstat3::NFS3ERR_NOENT)?;
        info!("Getattr {:?}", entry);
        let attr = match &entry.contents {
            FileObject::XetFile((meta, _)) => self.path_to_fattr3(id, meta.clone(), true),
            FileObject::RegularFile((meta, _)) => self.path_to_fattr3(id, meta.clone(), true),
            FileObject::Directory(_) => self.path_to_fattr3(id, EntryMetadata::default(), false),
        };
        if let Ok(stat) = &attr {
            self.statcache.write().unwrap().put(id, *stat);
        }
        attr
    }
}

// For this demo file system we let the handle just be the file
// there is only 1 file. a.txt.
#[async_trait]
impl NFSFileSystem for XetFSBare {
    fn root_dir(&self) -> fileid3 {
        self.rootdir
    }
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadOnly
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        self.expand_directory(dirid).await.map_err(|e| {
            error!("Error expanding directory {:?} {:?}", dirid, e);
            nfsstat3::NFS3ERR_IO
        })?;
        let fs = self.fs.read().unwrap();
        let entry = fs.get(dirid as usize).ok_or(nfsstat3::NFS3ERR_NOENT)?;
        info!("Lookup {:?}", entry);

        if let FileObject::Directory(_) = entry.contents {
            // if looking for dir/. its the current directory
            if filename[..] == [b'.'] {
                return Ok(dirid);
            }
            // if looking for dir/.. its the parent directory
            if filename[..] == [b'.', b'.'] {
                return Ok(entry.parent);
            }
            if let Some(fileid) = self
                .intern
                .read()
                .unwrap()
                .check_interned(
                    &OsString::from_str(
                        std::str::from_utf8(filename).map_err(|_| nfsstat3::NFS3ERR_INVAL)?,
                    )
                    .map_err(|_| nfsstat3::NFS3ERR_INVAL)?,
                )
                .and_then(|x| entry.children.get(&x))
            {
                Ok(*fileid)
            } else {
                Err(nfsstat3::NFS3ERR_NOENT)
            }
        } else {
            Err(nfsstat3::NFS3ERR_NOTDIR)
        }
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        self.getattr_sync(id)
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let entry = {
            let fs = self.fs.read().unwrap();
            fs.get(id as usize).ok_or(nfsstat3::NFS3ERR_NOENT)?.clone()
        };
        //eprintln!("Read {:?} {}, {}", entry, offset, count);
        match entry.contents {
            FileObject::Directory(_) => Err(nfsstat3::NFS3ERR_ISDIR),
            FileObject::XetFile((_, pointer)) => {
                let mut start = offset as usize;
                let mut end = offset as usize + count as usize;
                let len = pointer.filesize() as usize;
                let eof = end >= len;
                if start >= len {
                    start = len;
                }
                if end > len {
                    end = len;
                }
                MOUNT_POINTER_BYTES_READ.inc_by((end - start) as u64);

                let mut output: Vec<u8> = Vec::new();
                for ctr in 1..(self.prefetch + 1) {
                    if start + ctr * PREFETCH_LOOKAHEAD >= len {
                        break;
                    }
                    if self
                        .pfilereader
                        .prefetch(&pointer, (start + ctr * PREFETCH_LOOKAHEAD) as u64)
                        .await
                        .unwrap()
                    {
                        break;
                    }
                }
                self.pfilereader
                    .smudge_file_from_pointer(
                        &path::PathBuf::new(),
                        &pointer,
                        &mut output,
                        Some((start, end)),
                    )
                    .await
                    .or(Err(nfsstat3::NFS3ERR_IO))?;
                Ok((output, eof))
            }
            FileObject::RegularFile((_, oid)) => {
                let repo = self.repo.lock().await;
                let blob = repo.find_blob(oid).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                let len = blob.size();
                let mut start = offset as usize;
                let mut end = start + (count as usize);
                let eof = end >= len;
                if start >= len {
                    start = len;
                }
                if end > len {
                    end = len;
                }
                MOUNT_PASSTHROUGH_BYTES_READ.inc_by((end - start) as u64);
                let contents = blob.content();
                let ret: Vec<u8> = contents[start..end].to_vec();
                Ok((ret, eof))
            }
        }
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        self.expand_directory(dirid).await.map_err(|e| {
            error!("Error expanding directory {:?} {:?}", dirid, e);
            nfsstat3::NFS3ERR_IO
        })?;
        let fs = self.fs.read().unwrap();
        let entry = fs.get(dirid as usize).ok_or(nfsstat3::NFS3ERR_NOENT)?;
        info!("Readdir {:?}", entry);
        if let FileObject::Directory(_) = &entry.contents {
            let mut ret = ReadDirResult {
                entries: Vec::new(),
                end: false,
            };

            let range_start = if start_after > 0 {
                // look for the start_after file in the entry
                // However, the directory list is by symbol
                // So we convert the fileid back to a symbol (start_after_name)
                // Then lookup the name in the children.
                let start_after_name = fs
                    .get(start_after as usize)
                    .ok_or(nfsstat3::NFS3ERR_BAD_COOKIE)?
                    .name;
                if entry.children.get(&start_after_name).is_none() {
                    return Err(nfsstat3::NFS3ERR_BAD_COOKIE);
                }
                Bound::Excluded(start_after_name)
            } else {
                Bound::Unbounded
            };

            let remaining_length = entry
                .children
                .range((range_start, Bound::Unbounded))
                .count();

            let intern = self.intern.read().unwrap();

            for i in entry.children.range((range_start, Bound::Unbounded)) {
                let attr = self.getattr_sync(*i.1)?;
                ret.entries.push(DirEntry {
                    fileid: *i.1,
                    name: intern
                        .get(fs[*i.1 as usize].name)
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .as_bytes()
                        .into(),
                    attr,
                });
                if ret.entries.len() >= max_entries {
                    break;
                }
            }
            if ret.entries.len() == remaining_length {
                ret.end = true;
            }
            Ok(ret)
        } else {
            Err(nfsstat3::NFS3ERR_NOTDIR)
        }
    }
    async fn setattr(&self, _id: fileid3, _setattr: sattr3) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }
    async fn write(&self, _id: fileid3, _offset: u64, _data: &[u8]) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn create(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
        _attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn create_exclusive(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn remove(&self, _dirid: fileid3, _filename: &filename3) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn rename(
        &self,
        _from_dirid: fileid3,
        _from_filename: &filename3,
        _to_dirid: fileid3,
        _to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }
    async fn mkdir(
        &self,
        _dirid: fileid3,
        _dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn symlink(
        &self,
        _dirid: fileid3,
        _linkname: &filename3,
        _symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }
    async fn readlink(&self, _id: fileid3) -> Result<nfspath3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
}
