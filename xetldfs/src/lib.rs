mod utils;
mod xet_interface;
mod xet_rfile;

#[macro_use]
extern crate redhook;

use crate::utils::*;
use ctor;
use libc::*;
mod runtime;
use runtime::{activate_fd_runtime, interposing_disabled, with_interposing_disabled};
use xet_interface::materialize_rw_file_if_needed;
use xet_rfile::{close_fd_if_registered, maybe_fd_read_managed, register_interposed_read_fd};

use std::{
    ffi::CStr,
    ptr::null_mut,
    sync::atomic::{AtomicBool, Ordering},
};
#[ctor::ctor]
fn print_open() {
    eprintln!("XetLDFS interposing library loaded.");
    std::env::set_var("LD_PRELOAD", "");
    std::env::set_var("DYLD_INSERT_LIBRARIES", "");
}

// 0666, copied from sys/stat.h
const DEFFILEMODE: mode_t = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;

static FORCE_ALL_PASSTHROUGH: AtomicBool = AtomicBool::new(false);

pub fn force_all_passthrough(state: bool) {
    // Disables all the interposing, so all functions just pass through to the underlying function.
    FORCE_ALL_PASSTHROUGH.store(state, Ordering::Relaxed);
}

#[inline]
unsafe fn fopen_impl(
    pathname: *const c_char,
    mode: *const c_char,
    callback: unsafe extern "C" fn(*const c_char, *const c_char) -> *mut libc::FILE,
) -> *mut libc::FILE {
    // Convert fopen mode to OpenOptions
    let mode_str = CStr::from_ptr(mode).to_str().unwrap();
    let Some(open_flags) = open_flags_from_mode_string(mode_str) else {
        eprintln!("Bad open flags: {mode_str}");
        return null_mut();
    };

    if file_needs_materialization(open_flags) {
        materialize_rw_file_if_needed(pathname);
        // no need to interpose a regular file
        return callback(pathname, mode);
    }

    // only interpose read
    if open_flags & O_ACCMODE == O_RDONLY {
        let ret = callback(pathname, mode);
        if ret == null_mut() {
            return null_mut();
        }

        let fd = fileno(ret);
        register_interposed_read_fd(pathname, fd);
        ret
    } else {
        callback(pathname, mode)
    }
}

hook! {
    unsafe fn fopen(pathname: *const c_char, mode: *const c_char) -> *mut libc::FILE => my_fopen {
        if interposing_disabled() { return real!(fopen)(pathname, mode); }

        let _ig = with_interposing_disabled();

        eprintln!("XetLDFS: fopen called");

        fopen_impl(pathname, mode, real!(fopen))
    }
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn fopen64(pathname: *const c_char, mode: *const c_char) -> *mut libc::FILE => my_fopen64 {
        if interposing_disabled() { return real!(fopen)(pathname, mode); }

        let _ig = with_interposing_disabled();

        eprintln!("XetLDFS: fopen64 called");

        fopen_impl(pathname, mode, real!(fopen))
    }
}

#[inline]
unsafe fn open_impl(
    pathname: *const c_char,
    open_flags: c_int,
    filemode: mode_t,
    callback: unsafe extern "C" fn(*const c_char, flags: c_int, filemode: mode_t) -> c_int,
) -> c_int {
    if file_needs_materialization(open_flags) {
        materialize_rw_file_if_needed(pathname);
        // no need to interpose a regular file
        return callback(pathname, open_flags, filemode);
    }

    // only interpose read
    if open_flags & O_ACCMODE == O_RDONLY {
        let fd = callback(pathname, open_flags, filemode);
        register_interposed_read_fd(pathname, fd);
        fd
    } else {
        callback(pathname, open_flags, filemode)
    }
}

// Hook for open
hook! {
    unsafe fn open(pathname: *const c_char, flags: c_int, filemode: mode_t) -> c_int => my_open {
        activate_fd_runtime();

        let fname = unsafe {c_to_str(pathname)};
        if interposing_disabled() {
            return real!(open)(pathname, flags, filemode);
        }

        let _ig = with_interposing_disabled();

        eprintln!("XetLDFS: open called on {fname}");

        open_impl(pathname,flags, filemode, real!(open))
    }
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn open64(pathname: *const c_char, flags: c_int, filemode: mode_t) -> c_int => my_open64 {
        activate_fd_runtime();

        let fname = unsafe {c_to_str(pathname)};
        if interposing_disabled() {
            return real!(open64)(pathname, flags, filemode);
        }

        let _ig = with_interposing_disabled();

        eprintln!("XetLDFS: open64 called on {fname}");

        open_impl(pathname,flags, filemode, real!(open))
    }
}

hook! {
    unsafe fn read(fd: c_int, buf: *mut c_void, nbyte: size_t) -> ssize_t => my_read {
        if fd <= 2 || interposing_disabled() { return real!(read)(fd, buf, nbyte); }
        let _ig = with_interposing_disabled();

        eprintln!("XetLDFS: read called on {fd} for {nbyte} bytes");

        if let Some(fd_info) = maybe_fd_read_managed(fd) {
            fd_info.read(buf, nbyte)
        } else {
            real!(read)(fd, buf, nbyte)
        }
    }
}

hook! {
    unsafe fn fread(buf: *mut c_void, size: size_t, count: size_t, stream: *mut libc::FILE) -> size_t => my_fread {
        if interposing_disabled() { return real!(fread)(buf, size, count, stream); }
        let _ig = with_interposing_disabled();

        let fd = fileno(stream);

        eprintln!("XetLDFS: fread called on {fd}");

        if let Some(fd_info) = maybe_fd_read_managed(fd) {
            fd_info.fread(buf, size, count)
        } else {
            real!(fread)(buf, size, count, stream)
        }
    }
}

hook! {
    unsafe fn fstat(fd: c_int, buf: *mut libc::stat) -> c_int => my_fstat {
        if fd <= 2 || interposing_disabled() { return real!(fstat)(fd, buf); }
        let _ig = with_interposing_disabled();

        eprintln!("XetLDFS: fstat called on {fd}");

        if let Some(fd_info) = maybe_fd_read_managed(fd) {
            eprintln!("XetLDFS: fstat called on {fd} is managed");
            fd_info.fstat(buf)
        } else {
            real!(fstat)(fd, buf)
        }
    }
}

unsafe fn real_fstat(fd: c_int, buf: *mut libc::stat) -> c_int {
    real!(fstat)(fd, buf)
}

hook! {
    unsafe fn stat(pathname: *const libc::c_char, buf: *mut libc::stat) -> c_int => my_stat {
        let fd = my_open(pathname, O_RDONLY, DEFFILEMODE);
        my_fstat(fd, buf)
    }
}

hook! {
    unsafe fn lseek(fd: libc::c_int, offset: libc::off_t, whence: libc::c_int) -> libc::off_t => my_lseek {
        if fd <= 2 || interposing_disabled() { return real!(lseek)(fd, offset, whence); }
        let _ig = with_interposing_disabled();

        let result = {
            if let Some(fd_info) = maybe_fd_read_managed(fd) {
            fd_info.lseek(offset, whence)
        } else {
            real!(lseek)(fd, offset, whence)
        }};

        eprintln!("XetLDFS: lseek called, result = {result}");
        result
    }
}

hook! {
    unsafe fn readdir(dirp: *mut libc::DIR) -> *mut libc::dirent => my_readdir {
        if interposing_disabled() { return real!(readdir)(dirp); }
        let _ig = with_interposing_disabled();

        let result = real!(readdir)(dirp);
        eprintln!("XetLDFS: readdir called");
        result
    }
}

hook! {
    unsafe fn fseek(stream: *mut libc::FILE, offset: libc::c_long, whence: libc::c_int) -> libc::c_long => my_fseek {
        if interposing_disabled() { return real!(fseek)(stream, offset, whence); }
        let _ig = with_interposing_disabled();

        if stream == null_mut() { return EOF.try_into().unwrap(); }

        let fd = fileno(stream);

        let result = {
            if let Some(fd_info) = maybe_fd_read_managed(fd) {
            fd_info.lseek(offset, whence) as libc::c_long
        } else {
            real!(fseek)(stream, offset, whence)
        }
    };

        eprintln!("XetLDFS: fseek called, result = {result}");
        result
    }
}

hook! {
    unsafe fn close(fd: libc::c_int) => my_close {
        if fd <= 2 || interposing_disabled() { return real!(close)(fd); }
        let _ig = with_interposing_disabled();

        eprintln!("XetLDFS: close called on {fd}");

        close_fd_if_registered(fd);

        real!(close)(fd);
    }
}

hook! {
    unsafe fn fclose(stream: *mut libc::FILE) -> libc::c_int => my_fclose {
        if stream == null_mut() { return EOF.try_into().unwrap(); }

        let fd = fileno(stream);

        close_fd_if_registered(fd);

        let result = real!(fclose)(stream);
        eprintln!("XetLDFS: fclose called for fd={fd}, result = {result}");
        result
    }
}

hook! {
    unsafe fn ftell(stream: *mut libc::FILE) -> libc::c_long => my_ftell {
        if stream == null_mut() { return EOF.try_into().unwrap(); }
        let fd = fileno(stream);

        let result = {
            if let Some(fd_info) = maybe_fd_read_managed(fd) {
            fd_info.ftell() as libc::c_long
        } else {
            real!(ftell)(stream)
        }};
        eprintln!("XetLDFS: ftell called for fd={fd}, result = {result}");
        result
    }
}

hook! {
    unsafe fn mmap(addr: *mut libc::c_void, length: libc::size_t, prot: libc::c_int, flags: libc::c_int, fd: libc::c_int, offset: libc::off_t) -> *mut libc::c_void => my_mmap {
        let result = real!(mmap)(addr, length, prot, flags, fd, offset);
        // eprintln!("XetLDFS: mmap called, result = {:?}", result);
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
