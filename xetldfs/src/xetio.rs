use lazy_static::lazy_static;
use libc::{c_char, c_int, size_t, O_RDONLY, O_RDWR};
use std::collections::HashMap;
use std::{
    ffi::{CStr, CString},
    sync::Arc,
    sync::RwLock,
};

use crate::utils::*;
use crate::xet_interface::get_xet_instance;

#[derive(Debug)]
struct FdInfo {
    fd: c_int,
    size: size_t,
    pos: size_t,
}

lazy_static! {
    static ref FD_LOOKUP: RwLock<HashMap<c_int, Arc<FdInfo>>> = RwLock::new(HashMap::new());
}

pub fn register_interposed_fd(fd: c_int, pathname: *const c_char, flags: c_int) {
    if is_managed(pathname, flags) {
        FD_LOOKUP.write().unwrap().insert(
            fd,
            Arc::new(FdInfo {
                fd,
                size: 0, // todo!()
                pos: 0,
            }),
        );
        eprintln!("XetLDFS: registered {fd} to {}", unsafe {
            CStr::from_ptr(pathname).to_str().unwrap()
        });
    }
}

pub fn is_registered(fd: c_int) -> bool {
    FD_LOOKUP.read().unwrap().contains_key(&fd)
}

pub fn internal_read(fd: c_int, buf: *mut c_void, nbyte: size_t) -> size_t {
    todo!()
}

fn is_managed(pathname: *const c_char, flags: c_int) -> bool {
    if flags == O_RDONLY || (flags & O_RDWR) > 0 {
        is_read_managed(pathname)
    } else {
        is_write_managed(pathname)
    }
}

// Do we interpose reading from this file
fn is_read_managed(pathname: *const c_char) -> bool {
    todo!()
}

// Do we interpose writing into this file
fn is_write_managed(pathname: *const c_char) -> bool {
    return false;
}

fn rust_open(pathname: *const c_char, open_flags: c_int, filemode: Option<c_int>) -> Option<c_int> {
    eprintln!("XetLDFS: rust_open called");
    unsafe {
        let Ok(path) = CStr::from_ptr(pathname).to_str() else {
            register_io_error(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid pathname (UTF8 Error)",
            ));
            return Some(-1);
        };

        eprintln!("XetLDFS: rust_open: path set to {path}.");

        // See if there's a xet wrapper with which to convert this all.
        let Ok(maybe_xet_wrapper) = get_xet_instance(path).map_err(|e| {
            eprintln!("ERROR: Error opening Xet Instance from {path:?}: {e:?}");
            e
        }) else {
            eprintln!("XetLDFS: rust_open: no xet fs instance given.");
            // Fall back to raw
            return None;
        };

        #[cfg(test)]
        {
            use std::io::Write;
            if path.ends_with("_TEST_FILE") && ((open_flags & libc::O_RDONLY) != 0) {
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(path)
                    .map_err(|e| {
                        register_io_error_with_context(e, &format!("Opening test file {path:?}"))
                    })
                    .and_then(|mut f| f.write(b" :SUCCESSFUL:"))
                    .unwrap();
            }
        }

        let Some(_xet_wrapper) = maybe_xet_wrapper else {
            return None;
        };

        // TODO: intercept it
        None

        // return xet_wrapper.get_fd(path, mode);
    }
}
