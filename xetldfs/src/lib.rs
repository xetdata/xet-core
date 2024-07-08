#[allow(unused_imports)]
mod path_utils;
mod utils;
mod xet_interface;
mod xet_rfile;

#[macro_use]
extern crate redhook;
extern crate libc;

use crate::utils::*;
use ctor;
use libc::*;
mod runtime;
use path_utils::{is_regular_file, path_of_fd, resolve_path_from_fd};
use runtime::{activate_fd_runtime, interposing_disabled, with_interposing_disabled};

#[allow(unused)]
use utils::C_EMPTY_STR;

use xet_interface::{materialize_file_under_fd_if_needed, materialize_rw_file_if_needed};
use xet_rfile::{
    close_fd_if_registered, maybe_fd_read_managed, register_interposed_read_fd,
    set_fd_read_interpose,
};

use std::ffi::CStr;
use std::ptr::{null, null_mut};

#[ctor::ctor]
fn print_open() {
    eprintln!("XetLDFS interposing library loaded.");
}

pub const ENABLE_CALL_TRACING: bool = true;

#[macro_export]
macro_rules! ld_trace {
    ($($arg:tt)*) => {
        if ENABLE_CALL_TRACING {
            if runtime::runtime_activated() {
                let text = format!($($arg)*);
                eprintln!("XetLDFS: {text}");
            }
        }
    };
}

#[macro_export]
macro_rules! ld_warn {
    ($($arg:tt)*) => {
        if runtime::runtime_activated() {
            let text = format!($($arg)*);
            eprintln!("XetLDFS WARNING: {text}");
        }
    };
}

// 0666, copied from sys/stat.h
const DEFFILEMODE: mode_t = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;

#[inline]
unsafe fn fopen_impl(
    pathname: &str,
    mode: *const c_char,
    callback: impl Fn() -> *mut libc::FILE,
) -> *mut libc::FILE {
    // Convert fopen mode to OpenOptions
    let mode_str = CStr::from_ptr(mode).to_str().unwrap();
    let Some(open_flags) = open_flags_from_mode_string(mode_str) else {
        ld_warn!("Bad open flags? {mode_str}");
        return callback();
    };

    if file_needs_materialization(open_flags) {
        ld_trace!("fopen_impl: Materializing path {pathname}.");
        materialize_rw_file_if_needed(pathname);
        // no need to interpose a non-pointer file
        return callback();
    }

    // only interpose read
    if open_flags & O_ACCMODE == O_RDONLY {
        let ret = callback();
        if ret == null_mut() {
            ld_trace!("fopen_impl: callback returned null.");
            return null_mut();
        }

        let fd = fileno(ret);
        register_interposed_read_fd(pathname, fd);
        ld_trace!("fopen_impl: Registered {pathname} as read interposed with fd = {fd}.");
        ret
    } else {
        ld_trace!("fopen_impl: passing through.");
        callback()
    }
}

hook! {
    unsafe fn fopen(pathname: *const c_char, mode: *const c_char) -> *mut libc::FILE => my_fopen {
        if interposing_disabled() { return real!(fopen)(pathname, mode); }

        let _ig = with_interposing_disabled();

        if !is_regular_file(pathname) {
            // We only interpose for regular files (not for socket, symlink, block dev, directory, character device (/dev/tty), fifo).
            return real!(fopen)(pathname, mode);
        }

        let path = unsafe { c_to_str(pathname) };
        fopen_impl(path, mode, || real!(fopen)(pathname, mode))
    }
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn fopen64(pathname: *const c_char, mode: *const c_char) -> *mut libc::FILE => my_fopen64 {
        if interposing_disabled() { return real!(fopen64)(pathname, mode); }

        let _ig = with_interposing_disabled();

        let path = unsafe { c_to_str(pathname) };
        fopen_impl(path, mode, || real!(fopen64)(pathname, mode))
    }
}

hook! {
    unsafe fn freopen(path: *const libc::c_char, mode: *const libc::c_char, stream: *mut libc::FILE) -> *mut libc::FILE => my_freopen {
        if interposing_disabled() { return real!(freopen)(path, mode, stream); }

        let _ig = with_interposing_disabled();

        let fd = fileno(stream);

        // Close this if registered.
        if close_fd_if_registered(fd) {
            my_fopen(path, mode)
        } else {
            real!(freopen)(path, mode, stream)
        }
    }
}

#[inline]
unsafe fn open_impl(pathname: &str, open_flags: c_int, callback: impl Fn() -> c_int) -> c_int {
    if file_needs_materialization(open_flags) {
        materialize_rw_file_if_needed(pathname);
        // no need to interpose a non-pointer file
        return callback();
    }

    // only interpose read
    if open_flags & O_ACCMODE == O_RDONLY {
        ld_trace!("file {pathname} is RDONLY");
        let fd = callback();
        ld_trace!("file {pathname} is RDONLY, opened as {fd}");
        if fd != -1 {
            register_interposed_read_fd(pathname, fd);
        }
        fd
    } else {
        callback()
    }
}

// Hook for open
hook! {
    unsafe fn open(pathname: *const c_char, flags: c_int, filemode: mode_t) -> c_int => my_open {
        activate_fd_runtime();

        if interposing_disabled() {
            return real!(open)(pathname, flags, filemode);
        }

        let _ig = with_interposing_disabled();

        if !is_regular_file(pathname) {
            // We only interpose for regular files (not for socket, symlink, block dev, directory, character device (/dev/tty), fifo).
            return real!(open)(pathname, flags, filemode);
        }

        let path = unsafe { c_to_str(pathname) };
        open_impl(path ,flags,  || real!(open)(pathname, flags, filemode))
    }
}

#[inline]
unsafe fn real_open(pathname: *const c_char, flags: c_int, filemode: mode_t) -> c_int {
    real!(open)(pathname, flags, filemode)
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn open64(pathname: *const c_char, flags: c_int, filemode: mode_t) -> c_int => my_open64 {
        activate_fd_runtime();

        if interposing_disabled() {
            return real!(open64)(pathname, flags, filemode);
        }

        let _ig = with_interposing_disabled();

        // We only interpose for regular files (not for socket, symlink, block dev, directory, character device (/dev/tty), fifo).
        if !is_regular_file(pathname) {
            return real!(open64)(pathname, flags, filemode);
        }

        let path = unsafe { c_to_str(pathname) };
        open_impl(path, flags, || real!(open64)(pathname, flags, filemode))
    }
}

hook! {
    unsafe fn openat(dirfd: libc::c_int, pathname: *const libc::c_char, flags: libc::c_int, filemode : mode_t) -> libc::c_int => my_openat {
        activate_fd_runtime();

        if !interposing_disabled() {

            let _ig = with_interposing_disabled();

            if let Some(path) = resolve_path_from_fd(dirfd, pathname) {

                // We only interpose for regular files (not for socket, symlink, block dev, directory, character device (/dev/tty), fifo).
                if is_regular_file(path.as_ptr()) {
                    ld_trace!("openat: Path {} is regular file, calling open_impl (dirfd={dirfd}, pathname={})", path.to_str().unwrap(), c_to_str(pathname));
                    return open_impl(cstring_to_str(&path), flags, || real!(openat)(dirfd, pathname, flags, filemode));
                } else {
                    ld_trace!("openat: Path {} is not a regular file, bypassing (dirfd={dirfd}, pathname={}).", path.to_str().unwrap(), c_to_str(pathname));
                }
            } else {
                ld_trace!("openat: Error resolving path {} from dirfd={dirfd} regular file, bypassing.", c_to_str(pathname));
            }
        } else {
            ld_trace!("openat: Interposing disabled; passing path={} from dirfd={dirfd} regular file, bypassing.", c_to_str(pathname));
        }

        real!(openat)(dirfd, pathname, flags, filemode)
    }
}

hook! {
    unsafe fn read(fd: c_int, buf: *mut c_void, nbyte: size_t) -> ssize_t => my_read {
        if interposing_disabled() { return real!(read)(fd, buf, nbyte); }
        let _ig = with_interposing_disabled();

        if let Some(fd_info) = maybe_fd_read_managed(fd) {
            ld_trace!("read: Interposed read called with {nbyte} bytes on fd = {fd}");
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

        if let Some(fd_info) = maybe_fd_read_managed(fd) {
            ld_trace!("fread: Interposed read called with size={size}, count={count}, fd = {fd}");
            fd_info.fread(buf, size, count)
        } else {
            real!(fread)(buf, size, count, stream)
        }
    }
}

unsafe fn stat_impl(fd: c_int, buf: *mut libc::stat) -> c_int {
    let r = real!(fstat)(fd, buf);

    if r == EOF {
        return EOF;
    }

    if let Some(fd_info) = maybe_fd_read_managed(fd) {
        ld_trace!("XetLDFS: fstat called on {fd} is managed");
        fd_info.update_stat(buf);
    }

    r
}

hook! {
    unsafe fn fstat(fd: c_int, buf: *mut libc::stat) -> c_int => my_fstat {
        if interposing_disabled() { return real!(fstat)(fd, buf); }
        let _ig = with_interposing_disabled();

        stat_impl(fd, buf)
    }
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn fstat64(fd: c_int, buf: *mut libc::stat) -> c_int => my_fstat64 {
        if interposing_disabled() { return real!(fstat64)(fd, buf); }
        let _ig = with_interposing_disabled();

        stat_impl(fd, buf)
    }
}

hook! {
    unsafe fn stat(pathname: *const libc::c_char, buf: *mut libc::stat) -> c_int => my_stat {
        let fd = my_open(pathname, O_RDONLY, DEFFILEMODE);
        if fd == -1 {
            return -1;
        }

        stat_impl(fd, buf)
    }
}

unsafe fn real_stat(pathname: *const libc::c_char, buf: *mut libc::stat) -> c_int {
    real!(stat)(pathname, buf)
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn stat64(pathname: *const libc::c_char, buf: *mut libc::stat) -> c_int => my_stat64 {
        let fd = my_open(pathname, O_RDONLY, DEFFILEMODE);
        if fd == -1 {
            return -1;
        }

        stat_impl(fd, buf)
    }
}

hook! {
    unsafe fn fstatat(dirfd: libc::c_int, pathname: *const libc::c_char, buf: *mut libc::stat, flags: libc::c_int) -> libc::c_int => my_fstatat {
        let fd = {
            if pathname == null() || *pathname == (0 as c_char) {
                dirfd
            } else {
                my_openat(dirfd, pathname, flags, DEFFILEMODE)
            }
        };
        if fd == -1 {
            return -1;
        }

        stat_impl(fd, buf)
    }
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn fstatat64(dirfd: libc::c_int, pathname: *const libc::c_char, buf: *mut libc::stat, flags: libc::c_int) -> libc::c_int => my_fstatat64 {
        let fd = {
            if pathname == null() || *pathname == (0 as c_char) {
                dirfd
            } else {
                my_openat(dirfd, pathname, flags, DEFFILEMODE)
            }
        };
        if fd == -1 {
            return -1;
        }

        stat_impl(fd, buf)
    }
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn statx(dirfd: libc::c_int, pathname: *const libc::c_char, flags: libc::c_int, mask: libc::c_uint, statxbuf: *mut libc::statx) -> libc::c_int => my_statx {

        let fd = {
            if pathname == null() || *pathname == (0 as c_char) {
                dirfd
            } else {
                my_openat(dirfd, pathname, flags, DEFFILEMODE)
            }
        };

        if fd == -1 {
            return -1;
        }

        // If pathname is an empty string and the AT_EMPTY_PATH flag
        // is specified in flags (see below), then the target file is
        // the one referred to by the file descriptor dirfd.
        let ret = real!(statx)(fd, C_EMPTY_STR, AT_EMPTY_PATH | flags, mask, statxbuf);
        if ret == EOF {
            return EOF;
        }

        if let Some(fd_info) = maybe_fd_read_managed(fd) {
            ld_trace!("statx: update_statx called on {fd}, is managed");
            fd_info.update_statx(statxbuf);
        } else {
            ld_trace!("statx called; passed through.");
        }


        ret
    }
}

hook! {
    unsafe fn lseek(fd: libc::c_int, offset: libc::off_t, whence: libc::c_int) -> libc::off_t => my_lseek {
        if interposing_disabled() { return real!(lseek)(fd, offset, whence); }
        let _ig = with_interposing_disabled();

        let result = {
            if let Some(fd_info) = maybe_fd_read_managed(fd) {
            let ret = fd_info.lseek(offset, whence);
            ld_trace!("XetLDFS: lseek called, offset={offset}, whence={whence}, fd={fd}: ret={ret}");
            ret
        } else {
            real!(lseek)(fd, offset, whence)
        }};

        result
    }
}

hook! {
    unsafe fn readdir(dirp: *mut libc::DIR) -> *mut libc::dirent => my_readdir {
        if interposing_disabled() { return real!(readdir)(dirp); }
        let _ig = with_interposing_disabled();

        let result = real!(readdir)(dirp);
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
            let ret = fd_info.lseek(offset, whence) as libc::c_long;
            ld_trace!("XetLDFS: lseek called, offset={offset}, whence={whence}, fd={fd}: ret={ret}");
            ret
        } else {
            real!(fseek)(stream, offset, whence)
        }
    };

        result
    }
}

#[inline]
pub unsafe fn interposed_close(fd: libc::c_int) {
    ld_trace!("close called on {fd}");
    close_fd_if_registered(fd);
    real!(close)(fd);
}

#[inline]
pub unsafe fn real_close(fd: libc::c_int) {
    real!(close)(fd)
}

hook! {
    unsafe fn close(fd: libc::c_int) => my_close {
        if interposing_disabled() { return real!(close)(fd); }
        let _ig = with_interposing_disabled();
        interposed_close(fd)
    }
}

hook! {
    unsafe fn fclose(stream: *mut libc::FILE) -> libc::c_int => my_fclose {
        if stream == null_mut() { return EOF.try_into().unwrap(); }
        if interposing_disabled() { return real!(fclose)(stream); }
        let _ig = with_interposing_disabled();

        let fd = fileno(stream);

        ld_trace!("fclose called on {fd}");

        close_fd_if_registered(fd);

        let result = real!(fclose)(stream);
        result
    }
}

hook! {
    unsafe fn ftell(stream: *mut libc::FILE) -> libc::c_long => my_ftell {
        if stream == null_mut() { return EOF.try_into().unwrap(); }
        if interposing_disabled() { return real!(ftell)(stream); }
        let _ig = with_interposing_disabled();

        let fd = fileno(stream);

        let result = {
            if let Some(fd_info) = maybe_fd_read_managed(fd) {
            let ret = fd_info.ftell() as libc::c_long;
            ld_trace!("ftell: called on {fd}; interposed, ret = {ret}");
            ret
        } else {
            real!(ftell)(stream)
        }};
        result
    }
}

hook! {
    unsafe fn dup(old_fd: libc::c_int) -> libc::c_int => my_dup {
        if interposing_disabled() { return real!(dup)(old_fd); }
        let _ig = with_interposing_disabled();

        let new_fd = real!(dup)(old_fd);

        if new_fd == -1 {
            return new_fd;
        }

        if let Some(fd_info) = maybe_fd_read_managed(old_fd) {
            ld_trace!("dup: fd={new_fd} to point to same file as {old_fd}, path={:?}", fd_info.path());
            set_fd_read_interpose(new_fd, fd_info.dup(new_fd));
        }

        new_fd
    }
}

hook! {
    unsafe fn dup2(old_fd: libc::c_int, new_fd: libc::c_int) -> libc::c_int => my_dup2 {
        if interposing_disabled() { return real!(dup2)(old_fd, new_fd); }
        let _ig = with_interposing_disabled();

        let result = real!(dup2)(old_fd, new_fd);
        if result == -1 {
            return -1;
        }

        // If old_fd and new_fd are equal, then dup2() just returns new_fd;
        // no other changes are made to the existing descriptor.
        if old_fd != new_fd {
            close_fd_if_registered(new_fd);

            if let Some(fd_info) = maybe_fd_read_managed(old_fd) {
                ld_trace!("dup2: fd={new_fd} to point to same file as {old_fd}, path={:?}", fd_info.path());
                set_fd_read_interpose(new_fd, fd_info.dup(new_fd));
            }
        }

        result
    }
}

#[inline]
unsafe fn real_dup2(old_fd: libc::c_int, new_fd: libc::c_int) -> libc::c_int {
    real!(dup2)(old_fd, new_fd)
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn dup3(old_fd: libc::c_int, new_fd: libc::c_int, flags : libc::c_int) -> libc::c_int => my_dup3 {
        if interposing_disabled() { return real!(dup3)(old_fd, new_fd, flags); }
        let _ig = with_interposing_disabled();

        let result = real!(dup3)(old_fd, new_fd, flags);

        if result == -1 {
            return -1;
        }

        close_fd_if_registered(new_fd);

        if let Some(fd_info) = maybe_fd_read_managed(old_fd) {
            ld_trace!("dup3: fd={new_fd} to point to same file as {old_fd}, path={:?}", fd_info.path());
            set_fd_read_interpose(new_fd, fd_info.dup(new_fd));
        }

        result
    }
}

hook! {
    unsafe fn mmap(addr: *mut libc::c_void, length: libc::size_t, prot: libc::c_int, flags: libc::c_int, fd: libc::c_int, offset: libc::off_t) -> *mut libc::c_void => my_mmap {
        if !interposing_disabled() {
            if materialize_file_under_fd_if_needed(fd) {
                ld_trace!("mmap: Materialized pointer file under descriptor {fd}.");
            }
        }

        // Call the original mmap function
        real!(mmap)(addr, length, prot, flags, fd, offset)
    }
}

hook! {
    unsafe fn readv(fd: libc::c_int, iov: *const libc::iovec, iovcnt: libc::c_int) -> libc::ssize_t => my_readv {
        let result = real!(readv)(fd, iov, iovcnt);
        ld_trace!("XetLDFS: readv called, result = {result}");
        result
    }
}

hook! {
    unsafe fn writev(fd: libc::c_int, iov: *const libc::iovec, iovcnt: libc::c_int) -> libc::ssize_t => my_writev {
        let result = real!(writev)(fd, iov, iovcnt);
        ld_trace!("XetLDFS: writev called, result = {result}");
        result
    }
}

hook! {
    unsafe fn sendfile(out_fd: libc::c_int, in_fd: libc::c_int, offset: *mut libc::off_t, count: libc::size_t) -> libc::ssize_t => my_sendfile {
        let result = real!(sendfile)(out_fd, in_fd, offset, count);
        ld_trace!("XetLDFS: sendfile called, result = {result}");
        result
    }
}

/*
hook! {
    unsafe fn select(nfds: libc::c_int, readfds: *mut libc::fd_set, writefds: *mut libc::fd_set, exceptfds: *mut libc::fd_set, timeout: *mut libc::timeval) -> libc::c_int => my_select {
        let result = real!(select)(nfds, readfds, writefds, exceptfds, timeout);
        // eprintln!("XetLDFS: select called, result = {result}");
        result
    }
}

hook! {
    unsafe fn poll(fds: *mut libc::pollfd, nfds: libc::nfds_t, timeout: libc::c_int) -> libc::c_int => my_poll {
        let result = real!(poll)(fds, nfds, timeout);
        // eprintln!("XetLDFS: poll called, result = {result}");
        result
    }
}
*/

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
        ld_trace!("XetLDFS: kqueue called, result = {result}");
        result
    }
}
