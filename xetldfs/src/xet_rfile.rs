use crate::real_fstat;
use crate::runtime::{activate_fd_runtime, TOKIO_RUNTIME};
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex as TMutex;

// size of buffer used by setbuf, copied from stdio.h
const BUFSIZ: c_int = 1024;

// Copied from fread.c
// The maximum amount to read to avoid integer overflow.  INT_MAX is odd,
// so it make sense to make it even.  We subtract (BUFSIZ - 1) to get a
// whole number of BUFSIZ chunks.
const MAXREAD: c_int = c_int::MAX - (BUFSIZ - 1);

use crate::{
    c_to_str,
    xet_interface::{get_repo_context, XetFSRepoWrapper},
};

pub struct XetFdReadHandle {
    xet_fsw: Arc<XetFSRepoWrapper>,
    pos: TMutex<usize>,
    path: PathBuf,
    fd: c_int,
    pointer_file: PointerFile, // All non pointer files just get passed directly through
}

lazy_static! {
    static ref FD_LOOKUP: std::sync::RwLock<HashMap<c_int, Arc<XetFdReadHandle>>> =
        std::sync::RwLock::new(HashMap::new());
}

fn register_read_fd_impl(path: &str, fd: c_int) -> Result<()> {
    if let Some((maybe_xet_wrapper, norm_path)) = get_repo_context(path)? {
        if let Some(mut fd_info) = TOKIO_RUNTIME.handle().block_on(async move {
            maybe_xet_wrapper
                .open_path_for_read_if_pointer(norm_path.clone())
                .await
                .log_error(format!("Opening path {norm_path:?}."))
        })? {
            fd_info.fd = fd;

            // We now have one thing to track, so go ahead and activate all the read and fstat commands.
            activate_fd_runtime();

            FD_LOOKUP.write().unwrap().insert(fd, Arc::new(fd_info));
        }
    }
    Ok(())
}

pub fn register_interposed_read_fd(pathname: *const c_char, fd: c_int) {
    let path = unsafe { c_to_str(pathname) };

    // Possibly register the read fd.
    let _ = register_read_fd_impl(path, fd).map_err(|e| {
        eprintln!("Error in register_read_fd with {path}: {e:?}");
        e
    });
}

pub fn maybe_fd_read_managed(fd: c_int) -> Option<Arc<XetFdReadHandle>> {
    FD_LOOKUP.read().unwrap().get(&fd).map(|c| c.clone())
}

pub fn close_fd_if_registered(fd: c_int) {
    FD_LOOKUP.write().unwrap().remove_entry(&fd);
}

impl XetFdReadHandle {
    pub fn new(xet_fsw: Arc<XetFSRepoWrapper>, pointer_file: PointerFile) -> Self {
        Self {
            xet_fsw,
            pos: tokio::sync::Mutex::new(0),
            path: PathBuf::from_str(pointer_file.path()).unwrap(),
            pointer_file,
            fd: 0,
        }
    }

    pub fn filesize(&self) -> u64 {
        self.pointer_file.filesize()
    }

    pub fn path(&self) -> &Path {
        &self.path
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
        TOKIO_RUNTIME.block_on(async move { s.read_impl(buf, n_bytes).await })
    }

    pub fn fread(self: &Arc<Self>, buf: *mut c_void, size: size_t, count: size_t) -> size_t {
        let s = self.clone();

        TOKIO_RUNTIME.block_on(async move {
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

    pub fn fstat(self: &Arc<Self>, buf: *mut libc::stat) -> c_int {
        unsafe {
            // get the stat of the file on disk
            if real_fstat(self.fd, buf) == EOF {
                return EOF;
            };

            (*buf).st_size = self.pointer_file.filesize() as i64; /* file size, in bytes */
            (*buf).st_blocks = 0; // todo!() /* blocks allocated for file */
            (*buf).st_blksize = libxet::merkledb::constants::IDEAL_CAS_BLOCK_SIZE as i32;
            /* optimal blocksize for I/O */
        }
        0
    }

    pub fn lseek(self: &Arc<Self>, offset: libc::off_t, whence: libc::c_int) -> libc::off_t {
        let s = self.clone();

        TOKIO_RUNTIME.block_on(async move {
            //  whence is not valid?
            if !matches!(
                whence,
                SEEK_SET | SEEK_CUR | SEEK_END | SEEK_DATA | SEEK_HOLE
            ) {
                set_errno(Errno(libc::EINVAL));
                return EOF.try_into().unwrap();
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
                return EOF.try_into().unwrap();
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
                return EOF.try_into().unwrap();
            }

            // whence is SEEK_DATA or SEEK_HOLE, and offset is beyond the end of the file?
            if !matches!(whence, SEEK_DATA | SEEK_HOLE)
                && offset.is_positive()
                && offset as u64 == fsize
            {
                set_errno(Errno(libc::ENXIO));
                return EOF.try_into().unwrap();
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
        TOKIO_RUNTIME.block_on(async move { *s.pos.lock().await as libc::c_long })
    }

    pub fn close(fd: libc::c_int) {
        FD_LOOKUP.write().unwrap().remove(&fd);
    }
}
