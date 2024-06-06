use errno::{set_errno, Errno};
use lazy_static::lazy_static;
use libc::{c_char, c_int, c_void, fileno, size_t, ssize_t, EOF, O_ACCMODE, O_RDONLY, O_RDWR};
use std::collections::HashMap;
use std::{
    ffi::CStr,
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
    sync::RwLock,
};

use crate::xet_interface::get_xet_instance;
use crate::{real_read, utils::*};

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
    let readhandle = FD_LOOKUP.read().unwrap();
    let Some(fd_info) = readhandle.get(&fd) else {
        eprintln!("Read interposed for unregistered file descriptor {fd}\n");
        set_errno(Errno(libc::EIO));
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

fn is_managed(pathname: *const c_char, flags: c_int) -> bool {
    eprintln!("flags: {flags}");
    let flags = flags & O_ACCMODE; // only check the access mode bits
    if flags == O_RDONLY || (flags & O_RDWR) > 0 {
        eprintln!("is_read_managed?");
        is_read_managed(pathname)
    } else {
        eprintln!("is_write_managed?");
        is_write_managed(pathname)
    }
}

// Do we interpose reading from this file
fn is_read_managed(pathname: *const c_char) -> bool {
    unsafe {
        let Ok(path) = CStr::from_ptr(pathname).to_str() else {
            set_errno(Errno(libc::ENOENT));
            return false;
        };

        if path == "hello.txt" {
            return true;
        }
    }
    false
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

use redhook::hook;

hook! {
    unsafe fn lseek(fd: libc::c_int, offset: libc::off_t, whence: libc::c_int) -> libc::off_t => my_lseek {
        let result = real!(lseek)(fd, offset, whence);
        eprintln!("XetLDFS: lseek called, result = {result}");
        result
    }
}

hook! {
    unsafe fn readdir(dirp: *mut libc::DIR) -> *mut libc::dirent => my_readdir {
        let result = real!(readdir)(dirp);
        eprintln!("XetLDFS: readdir called");
        result
    }
}

hook! {
    unsafe fn fopen(path: *const libc::c_char, mode: *const libc::c_char) -> *mut libc::FILE => my_fopen {
        let file = real!(fopen)(path, mode);
        eprintln!("XetLDFS: fopen called, file = {:?}", file);
        file
    }
}

hook! {
    unsafe fn fread(ptr: *mut libc::c_void, size: libc::size_t, nmemb: libc::size_t, stream: *mut libc::FILE) -> libc::size_t => my_fread {
        let result = real!(fread)(ptr, size, nmemb, stream);
        eprintln!("XetLDFS: fread called, result = {result}");
        result
    }
}

hook! {
    unsafe fn fseek(stream: *mut libc::FILE, offset: libc::c_long, whence: libc::c_int) -> libc::c_int => my_fseek {
        let result = real!(fseek)(stream, offset, whence);
        eprintln!("XetLDFS: fseek called, result = {result}");
        result
    }
}

hook! {
    unsafe fn fclose(stream: *mut libc::FILE) -> libc::c_int => my_fclose {
        let result = real!(fclose)(stream);
        eprintln!("XetLDFS: fclose called, result = {result}");
        result
    }
}

hook! {
    unsafe fn mmap(addr: *mut libc::c_void, length: libc::size_t, prot: libc::c_int, flags: libc::c_int, fd: libc::c_int, offset: libc::off_t) -> *mut libc::c_void => my_mmap {
        let result = real!(mmap)(addr, length, prot, flags, fd, offset);
        eprintln!("XetLDFS: mmap called, result = {:?}", result);
        result
    }
}

hook! {
    unsafe fn readv(fd: libc::c_int, iov: *const libc::iovec, iovcnt: libc::c_int) -> libc::ssize_t => my_readv {
        let result = real!(readv)(fd, iov, iovcnt);
        eprintln!("XetLDFS: readv called, result = {result}");
        result
    }
}

hook! {
    unsafe fn writev(fd: libc::c_int, iov: *const libc::iovec, iovcnt: libc::c_int) -> libc::ssize_t => my_writev {
        let result = real!(writev)(fd, iov, iovcnt);
        eprintln!("XetLDFS: writev called, result = {result}");
        result
    }
}

/*
hook! {
    unsafe fn execle(path: *const libc::c_char, arg0: *const libc::c_char, ... /*, envp: *const *const libc::c_char */) -> libc::c_int => my_execle {
        let result = real!(execle)(path, arg0);
        eprintln!("XetLDFS: execle called, result = {result}");
        result
    }
}
*/

hook! {
    unsafe fn execve(path: *const libc::c_char, argv: *const *const libc::c_char, envp: *const *const libc::c_char) -> libc::c_int => my_execve {
        let result = real!(execve)(path, argv, envp);
        eprintln!("XetLDFS: execve called, result = {result}");
        result
    }
}

hook! {
    unsafe fn sendfile(out_fd: libc::c_int, in_fd: libc::c_int, offset: *mut libc::off_t, count: libc::size_t) -> libc::ssize_t => my_sendfile {
        let result = real!(sendfile)(out_fd, in_fd, offset, count);
        eprintln!("XetLDFS: sendfile called, result = {result}");
        result
    }
}

hook! {
    unsafe fn chmod(path: *const libc::c_char, mode: libc::mode_t) -> libc::c_int => my_chmod {
        let result = real!(chmod)(path, mode);
        eprintln!("XetLDFS: chmod called, result = {result}");
        result
    }
}

hook! {
    unsafe fn umask(mask: libc::mode_t) -> libc::mode_t => my_umask {
        let result = real!(umask)(mask);
        eprintln!("XetLDFS: umask called, result = {result}");
        result
    }
}

hook! {
    unsafe fn dup(oldfd: libc::c_int) -> libc::c_int => my_dup {
        let result = real!(dup)(oldfd);
        eprintln!("XetLDFS: dup called, result = {result}");
        result
    }
}

hook! {
    unsafe fn dup2(oldfd: libc::c_int, newfd: libc::c_int) -> libc::c_int => my_dup2 {
        let result = real!(dup2)(oldfd, newfd);
        eprintln!("XetLDFS: dup2 called, result = {result}");
        result
    }
}

hook! {
    unsafe fn freopen(path: *const libc::c_char, mode: *const libc::c_char, stream: *mut libc::FILE) -> *mut libc::FILE => my_freopen {
        let file = real!(freopen)(path, mode, stream);
        eprintln!("XetLDFS: freopen called, file = {:?}", file);
        file
    }
}

hook! {
    unsafe fn select(nfds: libc::c_int, readfds: *mut libc::fd_set, writefds: *mut libc::fd_set, exceptfds: *mut libc::fd_set, timeout: *mut libc::timeval) -> libc::c_int => my_select {
        let result = real!(select)(nfds, readfds, writefds, exceptfds, timeout);
        eprintln!("XetLDFS: select called, result = {result}");
        result
    }
}

hook! {
    unsafe fn poll(fds: *mut libc::pollfd, nfds: libc::nfds_t, timeout: libc::c_int) -> libc::c_int => my_poll {
        let result = real!(poll)(fds, nfds, timeout);
        eprintln!("XetLDFS: poll called, result = {result}");
        result
    }
}

/*
hook! {
    unsafe fn epoll_wait(epfd: libc::c_int, events: *mut libc::epoll_event, maxevents: libc::c_int, timeout: libc::c_int) -> libc::c_int => my_epoll_wait {
        let result = real!(epoll_wait)(epfd, events, maxevents, timeout);
        eprintln!("XetLDFS: epoll_wait called, result = {result}");
        result
    }
}
*/

/*
hook! {
    unsafe fn fcntl(fd: libc::c_int, cmd: libc::c_int, ...) -> libc::c_int => my_fcntl {
        let result = real!(fcntl)(fd, cmd);
        eprintln!("XetLDFS: fcntl called, result = {result}");
        result
    }
}
*/

hook! {
    unsafe fn kqueue() -> libc::c_int => my_kqueue {
        let result = real!(kqueue)();
        eprintln!("XetLDFS: kqueue called, result = {result}");
        result
    }
}
