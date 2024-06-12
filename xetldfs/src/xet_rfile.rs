use std::{
    collections::HashMap,
    io::Cursor,
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc},
};

use crate::tokio_runtime::TOKIO_RUNTIME;
use lazy_static::lazy_static;
use libc::*;
use libxet::ErrorPrinter;
use libxet::{data::pointer_file, errors::Result};
use libxet::{data::PointerFile, xetblob::XetRFileObject};

use crate::{
    c_to_str,
    xet_interface::{get_repo_context, XetFSRepoWrapper},
};

pub struct XetFdReadHandle {
    xet_fsw: Arc<XetFSRepoWrapper>,
    pos: tokio::sync::Mutex<ssize_t>,
    pointer_file: PointerFile, // All non pointer files just get passed directly through
}

lazy_static! {
    static ref FD_LOOKUP: std::sync::RwLock<HashMap<c_int, Arc<XetFdReadHandle>>> =
        RwLock::new(HashMap::new());
}

fn register_read_fd_impl(path: &str, fd: c_int) -> Result<()> {
    if let Some((maybe_xet_wrapper, norm_path)) = get_repo_context(path)? {
        assert!(!FD_LOOKUP.read().unwrap().contains_key(&fd));

        if let Some(fd_info) = TOKIO_RUNTIME.handle().block_on(async move {
            maybe_xet_wrapper
                .open_path_for_read_if_pointer(norm_path)
                .await
                .log_error(format!("Opening path {norm_path:?}."))
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
    pub fn new(xet_fsw: Arc<XetFSRepoWrapper>, pointer_file: PointerFile) -> Self {
        Self {
            xet_fsw,
            pos: <_>::new(0),
            pointer_file,
        }
    }

    pub fn filesize(&self) -> usize {
        self.pointer_file.filesize()
    }

    pub fn read(self: &Arc<Self>, buf: *mut c_void, n_bytes: size_t) -> ssize_t {
        let s = self.clone();
        TOKIO_RUNTIME.block_on(async move {
            let n_bytes = n_bytes as usize;
            let slice = slice::from_raw_parts_mut(buf as *mut u8, n_bytes);
            let mut out = Cursor::new(slice);

            let mut pos_lg = s.pos.lock().await;
            let pos = *pos_lg as usize;

            let end = (pos + n_bytes).min(s.pointer_file.filesize());

            let smudge_ok = s
                .xet_fsw
                .pft
                .smudge_file_from_pointer(&s.path, &s.pointer_file, &mut out, Some((pos, end)))
                .await
                .log_error(format!(
                    "Smudging pointer file in range = ({pos},{end}); pointer file: \n{}",
                    &s.pointer_file
                ))
                .is_ok();

            if smudge_ok {
                *pos_lg = end;
                end - pos
            } else {
                0
            }
        })
    }

    pub fn fread(
        self: &Arc<Self>,
        buf: *mut c_void,
        size: size_t,
        count: size_t,
        stream: *mut libc::FILE,
    ) -> size_t {
    }
}
