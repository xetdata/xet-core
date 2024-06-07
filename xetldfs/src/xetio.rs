use errno::{set_errno, Errno};
use lazy_static::lazy_static;
use libc::{
    c_char, c_int, c_void, fileno, size_t, ssize_t, EOF, O_ACCMODE, O_RDONLY, O_RDWR, SEEK_SET,
};
use libxet::constants::POINTER_FILE_LIMIT;
use libxet::data::PointerFile;
use std::collections::HashMap;
use std::mem::{size_of, MaybeUninit};
use std::{
    ffi::CStr,
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
    sync::RwLock,
};

use crate::filter::is_managed;
use crate::lseek::lseek;
use crate::xet_interface::get_xet_instance;
use crate::{real_fstat, real_lseek, real_read, utils::*};

#[derive(Debug)]
struct FdInfo {
    fd: c_int,
    size: size_t,
    pos: AtomicUsize,
}

lazy_static! {
    static ref FD_LOOKUP: RwLock<HashMap<c_int, Arc<FdInfo>>> = RwLock::new(HashMap::new());
}

// size of buffer used by setbuf, copied from stdio.h
const BUFSIZ: c_int = 1024;

// Copied from fread.c
// The maximum amount to read to avoid integer overflow.  INT_MAX is odd,
// so it make sense to make it even.  We subtract (BUFSIZ - 1) to get a
// whole number of BUFSIZ chunks.
const MAXREAD: c_int = c_int::MAX - (BUFSIZ - 1);

pub fn register_interposed_fd(fd: c_int, pathname: *const c_char, flags: c_int) {
    let path = unsafe { CStr::from_ptr(pathname).to_str().unwrap() };
    if is_managed(pathname, flags) {
        FD_LOOKUP.write().unwrap().insert(
            fd,
            Arc::new(FdInfo {
                fd,
                size: 6, // todo!()
                pos: 0.into(),
            }),
        );
        eprintln!("XetLDFS: registered {fd} to {path}");
    } else {
        eprintln!("XetLDFS: {path} not registered to {fd}");
    }
}

pub fn is_registered(fd: c_int) -> bool {
    FD_LOOKUP.read().unwrap().contains_key(&fd)
}

pub fn internal_fread(
    buf: *mut c_void,
    size: size_t,
    count: size_t,
    stream: *mut libc::FILE,
) -> size_t {
    let fd = unsafe { fileno(stream) };

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

        let ret = internal_read(fd, ptr, r);

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
}

pub fn internal_read(fd: c_int, buf: *mut c_void, nbyte: size_t) -> ssize_t {
    let Some(fd_info) = get_fd_info(fd) else {
        return EOF.try_into().unwrap();
    };

    if fd_info.pos.load(Ordering::Relaxed) >= fd_info.size {
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

    fd_info.pos.fetch_add(bytes.len(), Ordering::Relaxed);

    bytes.len() as ssize_t
}

pub fn internal_fstat(fd: c_int, buf: *mut libc::stat) -> c_int {
    let Some(fd_info) = get_fd_info(fd) else {
        return EOF.try_into().unwrap();
    };

    let mut stat = [0u8; size_of::<libc::stat>()];
    unsafe {
        if real_fstat(fd, stat.as_mut_ptr() as *mut libc::stat) == EOF {
            return EOF;
        }
    }

    // the file on disk stat
    let stat = stat.as_mut_ptr() as *mut libc::stat;

    unsafe {
        let disk_size = (*stat).st_size as usize;
        // may be a pointer file
        if disk_size < POINTER_FILE_LIMIT {
            // move pos to begining of file
            if real_lseek(fd, 0, SEEK_SET) == -1 {
                return EOF;
            }

            // load all bytes from disk
            let mut file_contents = vec![0u8; disk_size];
            if real_read(fd, file_contents.as_mut_ptr() as *mut c_void, disk_size) == -1 {
                return EOF;
            }

            // check pointer file validity
            if let Ok(utf8_contents) = std::str::from_utf8(&file_contents) {
                let pf = PointerFile::init_from_string(utf8_contents, "" /* not important */);

                if pf.is_valid() {
                    // reset size & ...
                    (*stat).st_size = pf.filesize() as i64;
                }
            }
        }
    }

    unsafe {
        libc::memcpy(
            buf as *mut c_void,
            stat as *const c_void,
            size_of::<libc::stat>(),
        );
    }

    return 0;
}

fn get_fd_info(fd: c_int) -> Option<Arc<FdInfo>> {
    let readhandle = FD_LOOKUP.read().unwrap();
    let maybe_fd_info = readhandle.get(&fd);
    if maybe_fd_info.is_none() {
        eprintln!("I/O interposed for unregistered file descriptor {fd}\n");
        set_errno(Errno(libc::EIO));
    }

    maybe_fd_info.cloned()
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
