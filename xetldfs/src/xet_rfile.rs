use std::{
    collections::HashMap, path::PathBuf, sync::{atomic::AtomicU64, Arc, RwLock}
};

use crate::tokio_runtime::TOKIO_RUNTIME;
use lazy_static::lazy_static;
use libc::*;
use libxet::errors::Result;
use libxet::{data::PointerFile, xetblob::XetRFileObject};
use libxet::ErrorPrinter;

use crate::{
    c_to_str,
    xet_interface::{get_repo_context, XetFSRepoWrapper},
};

pub struct XetFdReadHandle {
    xet_fsw: Arc<XetFSRepoWrapper>,
    path : PathBuf,

    pub fd: c_int,
    pos: AtomicU64,

    // we don't keep size here because the underlying file may
    // change by other threads. e.g. linux fs keep size in
    // vnode table.
    pub lock: tokio::sync::Mutex<()>, // synchronization on file operations

    inner: XetRFileObject,
}

lazy_static! {
    static ref FD_LOOKUP: RwLock<HashMap<c_int, Arc<XetFdReadHandle>>> = RwLock::new(HashMap::new());
}


fn register_read_fd_impl(path: &str, fd: c_int) -> Result<()> {
    if let Some((maybe_xet_wrapper, norm_path)) = get_repo_context(path)? {
        assert!(!FD_LOOKUP.read().unwrap().contains_key(&fd));

        if let Some(fd_info) = TOKIO_RUNTIME.handle().block_on(async move {
            maybe_xet_wrapper
                .open_path_for_read_if_pointer(norm_path)
                .await
                .log_error()
        })? {
            let fdl = FD_LOOKUP.write().unwrap();
            fdl.insert(fd, fd_info);
        }
    }

    Ok(())
}

pub fn register_read_fd(pathname: *const c_char, fd: c_int) {
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

impl XetFdReadHandle {

    pub fn len() -> usize {
        todo!()
    } 

    pub fn read(self: &Arc<Self>, buf: *mut c_void, nbyte: size_t) -> ssize_t {
        let s = self.clone();
        TOKIO_RUNTIME.block_on(async move {

            let fsize = match metadata {
                FileType::Regular(size) => size,
                FileType::Pointer(pf) => pf.filesize(),
            };

            // read syscall is thread-safe
            let _flock = s.lock.lock();

            if fd_info.pos.load(Ordering::Relaxed) >= fsize {
                eprintln!("returning eof for {fd}");
                return 0;
            }

            // let bytes = unsafe { real_read(fd, buf, nbyte) };
            // bytes

            let bytes = "aaaaaa";

            unsafe {
                libc::memcpy(
                    buf as *mut c_void,
                    bytes.as_ptr() as *const c_void,
                    bytes.len(),
                );
            }

            fd_info.pos.fetch_add(bytes.len() as u64, Ordering::Relaxed);

            bytes.len() as ssize_t
        })
    }

    pub fn fread(
        self: &Arc<Self>, buf: *mut c_void, size: size_t, count: size_t, stream: *mut libc::FILE) -> size_t


}
