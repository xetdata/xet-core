use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use git2;
use git2::{Commit, Oid};
use nfsserve::nfs::*;
use nfsserve::vfs::*;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::config::XetConfig;
use crate::constants as gitxet_constants;
use crate::data_processing::PointerFileTranslator;
use crate::log::ErrorPrinter;
use crate::xetmnt::watch::contents::EntryContent;
use crate::xetmnt::watch::metadata::FSMetadata;
use crate::xetmnt::watch::metrics::{MOUNT_PASSTHROUGH_BYTES_READ, MOUNT_POINTER_BYTES_READ};
use crate::xetmnt::watch::watcher::RepoWatcher;

const PREFETCH_LOOKAHEAD: usize = gitxet_constants::PREFETCH_WINDOW_SIZE_BYTES as usize;

/// A Read-only FS implementation that will watch for updates to a git reference.
pub struct XetFSWatch {
    fs: Arc<FSMetadata>,
    pfilereader: Arc<PointerFileTranslator>,
    repo: Arc<Mutex<git2::Repository>>,
    watcher: Arc<RepoWatcher>,
    prefetch: usize,
}

impl Debug for XetFSWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XetFSWatch")
            // .field("rootdir", &self.rootdir)
            // .field("fs", &self.fs)
            .finish()
    }
}

impl XetFSWatch {
    pub async fn new(
        srcpath: &Path,
        cfg: &XetConfig,
        reference: &str,
        prefetch: usize,
        autowatch_interval: Option<Duration>,
    ) -> Result<XetFSWatch, anyhow::Error> {
        debug!("Opening XetFS ReadOnly at {:?} {:?}", srcpath, reference);
        let pfile = Arc::new(PointerFileTranslator::from_config(cfg).await?);

        let repo = git2::Repository::discover(srcpath)?;
        let root_tree_oid = Self::get_root_tree_oid(&repo, reference)?;
        let should_auto_watch = Self::should_watch_ref(&repo, reference);
        let repo = Arc::new(Mutex::new(repo));
        let fs = Arc::new(FSMetadata::new(srcpath, root_tree_oid)?);
        let watcher = Arc::new(RepoWatcher::new(
            fs.clone(),
            repo.clone(),
            pfile.clone(),
            reference.to_string(),
            srcpath.to_path_buf(),
        ));
        if let Some(interval) = autowatch_interval {
            if should_auto_watch {
                info!("Starting watcher task for ref: {reference}");
                let w = watcher.clone();
                tokio::spawn(async move { w.run(interval).await }); // TODO: add shutdown hook.
            } else {
                info!("Ref: {reference} is not a branch, not periodically watching the repo")
            }
        }

        Ok(XetFSWatch {
            fs,
            pfilereader: pfile,
            repo,
            watcher,
            prefetch,
        })
    }

    pub fn get_root_tree_oid(repo: &git2::Repository, gitref: &str) -> Result<Oid, anyhow::Error> {
        let rev = repo.revparse_single(gitref)?;
        rev.as_commit()
            .map(Commit::tree_id)
            .ok_or(anyhow!("expecting reference {gitref} to point to a commit"))
    }

    #[allow(unused)]
    pub async fn refresh(&self) -> Result<(), anyhow::Error> {
        self.watcher.refresh().await
    }

    fn should_watch_ref(repo: &git2::Repository, gitref: &str) -> bool {
        if gitref.is_empty() {
            return false;
        }
        let branch = repo.find_branch(gitref, git2::BranchType::Local);
        branch.is_ok()
    }

    async fn expand_directory(&self, dir_id: fileid3) -> Result<(), nfsstat3> {
        if self.fs.is_expanded(dir_id)? {
            return Ok(());
        }
        let repo = self.repo.lock().await;
        // double check that someone else hasn't already expanded the directory
        if self.fs.is_expanded(dir_id)? {
            return Ok(());
        }
        info!("Expand dir: {dir_id:?}");
        let entry = self.fs.get_entry(dir_id)?;
        let parent_path = if let EntryContent::Directory(ref dirmeta) = entry.contents {
            &dirmeta.path
        } else {
            return Err(nfsstat3::NFS3ERR_NOTDIR);
        };

        let entry_oid = entry.oid;
        let tree = repo
            .find_tree(entry_oid)
            .log_error(format!("Couldn't find oid: {entry_oid:} in repo"))
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        for tree_ent in tree.iter() {
            let filename: filename3 = tree_ent.name_bytes().into();
            let ent_path = parent_path.join(String::from_utf8_lossy(&filename).to_string());
            let oid = tree_ent.id();
            let maybe_contents = EntryContent::from_repo_tree_entry(&repo, &tree_ent, &ent_path)
                .log_error(format!("couldn't detect content type for: {filename:?}"));
            match maybe_contents {
                Err(e) => {
                    error!("Couldn't construct EntryContent for: {oid:?} ({filename:?}) due to error: {e:}");
                }
                Ok(None) => {
                    let entry_type = tree_ent
                        .kind()
                        .ok_or(nfsstat3::NFS3ERR_IO)
                        .log_error(format!("tree entry for: {filename:?} has no type"))?;
                    error!("Oid: {oid:?} ({filename:?}) type: {entry_type:?} isn't supported");
                }
                Ok(Some(contents)) => {
                    let id = self.fs.insert_new_entry(dir_id, &filename, oid, contents)?;
                    info!("Added new entry for: {ent_path:?} into fs with id: {id:?}");
                }
            }
        }
        self.fs.set_expanded(dir_id)?;
        Ok(())
    }
}

#[async_trait]
impl NFSFileSystem for XetFSWatch {
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadOnly
    }
    fn root_dir(&self) -> fileid3 {
        self.fs.get_root_id()
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        self.expand_directory(dirid).await?;
        info!("Lookup {dirid:?}/{filename:?}");
        self.fs.lookup_child_id(dirid, filename)
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        info!("Getattr: {id:?}");
        self.fs.getattr(id)
    }

    async fn setattr(&self, _id: fileid3, _setattr: sattr3) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let entry = self.fs.get_entry(id)?;
        info!("Read {:?} {}, {}", entry, offset, count);
        match &entry.contents {
            EntryContent::Directory(_) => Err(nfsstat3::NFS3ERR_ISDIR),
            EntryContent::XetFile((_, pointer)) => {
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
                        .prefetch(pointer, (start + ctr * PREFETCH_LOOKAHEAD) as u64)
                        .await
                        .unwrap()
                    {
                        break;
                    }
                }
                self.pfilereader
                    .smudge_file_from_pointer(
                        &PathBuf::new(),
                        pointer,
                        &mut output,
                        Some((start, end)),
                    )
                    .await
                    .or(Err(nfsstat3::NFS3ERR_IO))?;
                Ok((output, eof))
            }
            EntryContent::RegularFile(_) => {
                let repo = self.repo.lock().await;
                let blob = repo
                    .find_blob(entry.oid)
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
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

    async fn mkdir(
        &self,
        _dirid: fileid3,
        _dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
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

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        info!("Readdir {dirid:?}, start_after: {start_after:?}, max_entries: {max_entries:?}");
        self.expand_directory(dirid).await?;
        self.fs.list_children(dirid, start_after, max_entries)
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
