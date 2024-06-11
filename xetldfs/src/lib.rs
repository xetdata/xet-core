mod filter;
mod utils;
mod xet_interface;
mod xetio;

#[macro_use]
extern crate redhook;

use crate::{utils::*, xetio::*};
use ctor;
use libc::*;

use std::{ffi::CStr, ptr::null_mut};

#[ctor::ctor]
fn on_load() {
    eprintln!("{} loaded successfully.", env!("CARGO_PKG_NAME"));
}

// 0666, copied from sys/stat.h
const DEFFILEMODE: mode_t = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;

hook! {
    unsafe fn fopen(pathname: *const c_char, mode: *const c_char) -> *mut libc::FILE => my_fopen {
        eprintln!("XetLDFS: fopen called");
        // Convert fopen mode to OpenOptions
        let mode_str = CStr::from_ptr(mode).to_str().unwrap();
        let maybe_open_flags = open_flags_from_mode_string(mode_str);

        // if let Some(option_flags) = maybe_open_flags {
        //     if let Some(out) = rust_open(pathname, option_flags, None) {
        //         // Convert the file descriptor to FILE* using fdopen
        //         let file_mode = CString::new(mode_str).unwrap();
        //         return libc::fdopen(out, file_mode.as_ptr());
        //     }
        // }

        // real!(fopen)(pathname, mode)

        let Some(flags) = maybe_open_flags else {
            return null_mut();
        };

        let file = real!(fopen)(pathname, mode);

        if file != null_mut() {
            let fd = fileno(file);
            register_interposed_fd(fd, pathname, flags);
        }

        file
    }
}

#[cfg(target_os = "linux")]
hook! {
        unsafe fn fopen64(pathname: *const c_char, mode: *const c_char) -> *mut libc::FILE => my_fopen64 {
         eprintln!("XetLDFS: fopen64 called");
        // Convert fopen mode to OpenOptions
        let mode_str = CStr::from_ptr(mode).to_str().unwrap();
        let maybe_open_flags = open_flags_from_mode_string(mode_str);

        if let Some(option_flags) = maybe_open_flags {
            if let Some(out) = rust_open(pathname, option_flags, None) {
                // Convert the file descriptor to FILE* using fdopen
                let file_mode = CString::new(mode_str).unwrap();
                return libc::fdopen(out, file_mode.as_ptr());
            }
        }

        real!(fopen64)(pathname, mode)
    }
}

// Hook for open
hook! {
    unsafe fn open(pathname: *const c_char, flags: c_int, filemode: mode_t) -> c_int => my_open {
    eprintln!("XetLDFS: open called {flags:?} {filemode:?}");

        // Check if the path is for a file that exists
        // let path = CStr::from_ptr(pathname).to_str().unwrap();
        // if std::path::Path::new(path).is_file() {
        //     if let Some(out) = rust_open(pathname, flags, Some(filemode)) {
        //         return out;
        //     }
        // }


        let fd = real!(open)(pathname, flags, filemode);

        register_interposed_fd(fd, pathname, flags as c_int);

        fd
    }
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn open64(pathname: *const c_char, flags: c_int, filemode: c_int) -> c_int => my_open64 {
    eprintln!("XetLDFS: open64 called");
        // Check if the path is for a file that exists
        let path = CStr::from_ptr(pathname).to_str().unwrap();
        if std::path::Path::new(path).is_file() {
            if let Some(out) = rust_open(pathname, flags, Some(filemode)) {
                return out;
            }
        }

        eprintln!("XetLDFS open64: rust_open completed");

        real!(open64)(pathname, flags, filemode)
    }
}

hook! {
    unsafe fn read(fd: c_int, buf: *mut c_void, nbyte: size_t) -> ssize_t => my_read {
        eprintln!("XetLDFS: read called on {fd} for {nbyte} bytes");

        if is_registered(fd) {
            let r = internal_read(fd, buf, nbyte);
            eprintln!("read {r} bytes from internal_read");
            r
        } else {
            let r = real!(read)(fd, buf, nbyte);
            eprintln!("read {r} bytes from real_read");
            r
        }
    }
}

pub unsafe fn real_read(fd: c_int, buf: *mut c_void, nbyte: size_t) -> ssize_t {
    real!(read)(fd, buf, nbyte)
}

hook! {
    unsafe fn fread(buf: *mut c_void, size: size_t, count: size_t, stream: *mut libc::FILE) -> size_t => my_fread {
        let fd = fileno(stream);

        eprintln!("XetLDFS: fread called on {fd}");

        if is_registered(fd) {
            internal_fread(buf, size, count, stream)
        } else {
            real!(fread)(buf, size, count, stream)
        }
    }
}

hook! {
    unsafe fn fstat(fd: c_int, buf: *mut libc::stat) -> c_int => my_fstat {
        eprintln!("XetLDFS: fstat called on {fd}");

        if is_registered(fd) {
            internal_fstat(fd, buf)
        } else {
            real!(fstat)(fd, buf)
        }
    }
}

pub unsafe fn real_fstat(fd: c_int, buf: *mut libc::stat) -> c_int {
    real!(fstat)(fd, buf)
}

hook! {
    unsafe fn lseek(fd: libc::c_int, offset: libc::off_t, whence: libc::c_int) -> libc::off_t => my_lseek {
        let result = if is_registered(fd) {
            internal_lseek(fd, offset, whence)
        } else {
            real!(lseek)(fd, offset, whence)
        };

        eprintln!("XetLDFS: lseek called, result = {result}");
        result
    }
}

pub unsafe fn real_lseek(fd: libc::c_int, offset: libc::off_t, whence: libc::c_int) -> libc::off_t {
    real!(lseek)(fd, offset, whence)
}

hook! {
    unsafe fn readdir(dirp: *mut libc::DIR) -> *mut libc::dirent => my_readdir {
        let result = real!(readdir)(dirp);
        eprintln!("XetLDFS: readdir called");
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
    unsafe fn close(fd: libc::c_int) => my_close {
        eprintln!("XetLDFS: close called on {fd}");

        if is_registered(fd) {
            internal_close(fd);
        }

        real!(close)(fd);
    }
}

hook! {
    unsafe fn fclose(stream: *mut libc::FILE) -> libc::c_int => my_fclose {
        if stream != null_mut() {
            let fd = fileno(stream);
            internal_close(fd);
        }

        let result = real!(fclose)(stream);
        eprintln!("XetLDFS: fclose called, result = {result}");
        result
    }
}

// hook! {
//     unsafe fn mmap(addr: *mut libc::c_void, length: libc::size_t, prot: libc::c_int, flags: libc::c_int, fd: libc::c_int, offset: libc::off_t) -> *mut libc::c_void => my_mmap {
//         let result = real!(mmap)(addr, length, prot, flags, fd, offset);
//         eprintln!("XetLDFS: mmap called, result = {:?}", result);
//         result
//     }
// }

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
