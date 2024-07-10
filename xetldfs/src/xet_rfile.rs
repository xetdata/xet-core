use crate::runtime::activate_fd_runtime;
use errno::{set_errno, Errno};
use lazy_static::lazy_static;
use libc::*;
use libxet::data::PointerFile;
use libxet::errors::Result;
use libxet::ErrorPrinter;
use std::collections::HashMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex as TMutex;

use crate::ld_trace;
use crate::runtime;
use crate::ENABLE_CALL_TRACING;

// size of buffer used by setbuf, copied from stdio.h
const BUFSIZ: c_int = 1024;

// Copied from fread.c
// The maximum amount to read to avoid integer overflow.  INT_MAX is odd,
// so it make sense to make it even.  We subtract (BUFSIZ - 1) to get a
// whole number of BUFSIZ chunks.
const MAXREAD: c_int = c_int::MAX - (BUFSIZ - 1);

use crate::xet_interface::{get_repo_context, XetFSRepoWrapper};

pub struct XetFdReadHandle {
    xet_fsw: Arc<XetFSRepoWrapper>,
    pos: Arc<TMutex<usize>>,
    path: PathBuf,
    fd: c_int,
    pointer_file: Arc<PointerFile>, // All non pointer files just get passed directly through
}

lazy_static! {
    static ref FD_LOOKUP: std::sync::RwLock<HashMap<c_int, Arc<XetFdReadHandle>>> =
        std::sync::RwLock::new(HashMap::new());
}

pub fn set_fd_read_interpose(fd: c_int, fd_info: Arc<XetFdReadHandle>) {
    // We now have one thing to track, so go ahead and activate all the read and fstat commands.
    activate_fd_runtime();

    FD_LOOKUP.write().unwrap().insert(fd, fd_info);
}

fn register_read_fd_impl(path: &str, fd: c_int) -> Result<()> {
    ld_trace!("register_read_fd_impl: {path} for {fd}");
    if let Some((maybe_xet_wrapper, norm_path)) = get_repo_context(path)? {
        ld_trace!("file norm_path: {norm_path:?} for {path}");

        if let Some(mut fd_info) = runtime::tokio_run(async move {
            maybe_xet_wrapper
                .open_path_for_read_if_pointer(norm_path.clone())
                .await
                .map_err(|e| {
                    ld_trace!("open for read if pointer failed: {e:?}");
                    e
                })
                .log_error(format!("Opening path {norm_path:?}."))
        })? {
            fd_info.fd = fd;

            set_fd_read_interpose(fd, Arc::new(fd_info));

            ld_trace!("{path} registered to {fd}");
        } else {
            ld_trace!("open for read if pointer failed");
        }
    } else {
        ld_trace!("get repo context failed for : {path}");
    }
    Ok(())
}

pub fn register_interposed_read_fd(path: &str, fd: c_int) {
    // Possibly register the read fd.
    let _ = register_read_fd_impl(path, fd).map_err(|e| {
        ld_trace!("Error in register_read_fd with {path}: {e:?}");
        e
    });
}

pub fn maybe_fd_read_managed(fd: c_int) -> Option<Arc<XetFdReadHandle>> {
    FD_LOOKUP.read().unwrap().get(&fd).cloned()
}

pub fn close_fd_if_registered(fd: c_int) -> bool {
    FD_LOOKUP.write().unwrap().remove_entry(&fd).is_some()
}

impl XetFdReadHandle {
    pub fn new(xet_fsw: Arc<XetFSRepoWrapper>, pointer_file: PointerFile) -> Self {
        Self {
            xet_fsw,
            pos: Arc::new(tokio::sync::Mutex::new(0)),
            path: PathBuf::from_str(pointer_file.path()).unwrap(),
            pointer_file: Arc::new(pointer_file),
            fd: 0,
        }
    }

    pub fn filesize(&self) -> u64 {
        self.pointer_file.filesize()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn dup(self: Arc<Self>, new_fd: c_int) -> Arc<Self> {
        Arc::new(Self {
            xet_fsw: self.xet_fsw.clone(),
            pos: self.pos.clone(),
            path: self.path.clone(),
            fd: new_fd,
            pointer_file: self.pointer_file.clone(),
        })
    }

    async fn read_impl(self: &Arc<Self>, buf: *mut c_void, n_bytes: size_t) -> ssize_t {
        let slice = unsafe { std::slice::from_raw_parts_mut(buf as *mut u8, n_bytes) };

        let mut out = Cursor::new(slice);

        let mut pos_lg = self.pos.lock().await;
        let pos = *pos_lg;

        let end = (pos + n_bytes).min(self.pointer_file.filesize() as usize);

        let smudge_ok = self
            .xet_fsw
            .pft
            .smudge_file_from_pointer(
                self.path(),
                &self.pointer_file,
                &mut out,
                Some((pos as usize, end as usize)),
            )
            .await
            .log_error(format!(
                "Smudging pointer file in range = ({pos},{end}); pointer file: \n{:?}",
                &self.pointer_file
            ))
            .is_ok();

        if smudge_ok {
            *pos_lg = end;
            (end - pos) as isize
        } else {
            0
        }
    }

    pub fn read(self: &Arc<Self>, buf: *mut c_void, n_bytes: size_t) -> ssize_t {
        let s = self.clone();
        runtime::tokio_run(async move { s.read_impl(buf, n_bytes).await })
    }

    pub fn fread(self: &Arc<Self>, buf: *mut c_void, size: size_t, count: size_t) -> size_t {
        let s = self.clone();

        runtime::tokio_run(async move {
            // adapted from fread.c
            let mut resid = count * size;

            if resid == 0 {
                return 0;
            }

            let total = resid;
            let mut ptr = buf;

            while resid > 0 {
                let r: size_t = if resid > c_int::MAX as size_t {
                    MAXREAD as size_t
                } else {
                    resid
                };

                let ret = s.read_impl(ptr, r).await;

                if ret == -1 {
                    // error occurred
                    todo!()
                }

                let ret: size_t = ret.try_into().unwrap_or_default();

                if ret != r {
                    return (total - resid + ret) / size;
                }

                ptr = unsafe { ptr.byte_add(r) };
                resid -= r;
            }

            // full read
            count
        })
    }

    pub fn update_stat(self: &Arc<Self>, buf: *mut libc::stat) {
        unsafe {
            (*buf).st_size = self.filesize() as i64; /* file size, in bytes */
            (*buf).st_blocks = 0; // todo!() /* blocks allocated for file */
            (*buf).st_blksize = libxet::merkledb::constants::IDEAL_CAS_BLOCK_SIZE
                .try_into()
                .unwrap()
            /* optimal blocksize for I/O */
        }
    }

    #[cfg(target_os = "linux")]
    pub fn update_statx(self: &Arc<Self>, buf: *mut libc::statx) {
        unsafe {
            (*buf).stx_size = self.filesize(); /* file size, in bytes */
            (*buf).stx_blocks = 0; // todo!() /* blocks allocated for file */
            (*buf).stx_blksize = libxet::merkledb::constants::IDEAL_CAS_BLOCK_SIZE
                .try_into()
                .unwrap()
            /* optimal blocksize for I/O */
        }
    }

    pub fn lseek(self: &Arc<Self>, offset: libc::off_t, whence: libc::c_int) -> libc::off_t {
        let s = self.clone();

        runtime::tokio_run(async move {
            //  whence is not valid?
            if !matches!(
                whence,
                SEEK_SET | SEEK_CUR | SEEK_END | SEEK_DATA | SEEK_HOLE
            ) {
                set_errno(Errno(libc::EINVAL));
                return EOF.into();
            }

            let fsize = s.pointer_file.filesize();

            // lock because it's difficult to implement with pure atomic variable.
            let mut pos_lock = s.pos.lock().await;

            let cur_pos = *pos_lock as u64;

            // The seek location (calculated from offset and whence) is negative?
            let seek_to_negtive_location = match whence {
                SEEK_SET => offset.is_negative(),
                SEEK_CUR => offset.is_negative() && cur_pos < offset.abs().try_into().unwrap(),
                SEEK_END => offset.is_negative() && fsize < offset.abs().try_into().unwrap(),
                _ => false, // noop
            };

            if seek_to_negtive_location {
                set_errno(Errno(libc::EINVAL));
                return EOF.into();
            }

            // The seek location is too large to be stored in an object of type off_t?
            let seek_overflow = match whence {
                SEEK_SET => false,
                SEEK_CUR => libc::off_t::MAX.saturating_sub_unsigned(cur_pos) < offset,
                SEEK_END => libc::off_t::MAX.saturating_sub_unsigned(fsize) < offset,
                _ => false, // noop
            };

            if seek_overflow {
                set_errno(Errno(libc::EOVERFLOW));
                return EOF.into();
            }

            // whence is SEEK_DATA or SEEK_HOLE, and offset is beyond the end of the file?
            if !matches!(whence, SEEK_DATA | SEEK_HOLE)
                && offset.is_positive()
                && offset as u64 == fsize
            {
                set_errno(Errno(libc::ENXIO));
                return EOF.into();
            }

            let new_pos = match whence {
                SEEK_SET => offset.try_into().unwrap(),
                SEEK_CUR => cur_pos.saturating_add_signed(offset),
                SEEK_END => fsize.saturating_add_signed(offset),
                SEEK_DATA => offset.try_into().unwrap(), // always data
                SEEK_HOLE => fsize,                      // no hole
                _ => unreachable!(),
            };

            *pos_lock = new_pos as usize;

            new_pos as libc::off_t
        })
    }

    pub fn ftell(self: &Arc<Self>) -> libc::c_long {
        let s = self.clone();
        runtime::tokio_run(async move { *s.pos.lock().await as libc::c_long })
    }
}
