use libc::*;
use std::ffi::CStr;
use std::ptr::null_mut;

#[macro_use]
extern crate redhook;
extern crate libc;

#[macro_use]
mod reporting;

mod path_utils;
mod repo_path_utils;
mod runtime;
mod utils;
mod xet_interface;
mod xet_repo_wrapper;
mod xet_rfile;

use crate::path_utils::{is_regular_fd, is_regular_file, resolve_path_from_fd};
use crate::runtime::{
    activate_fd_runtime, interposing_disabled, process_in_interposable_state,
    with_interposing_disabled,
};

#[allow(unused)]
use crate::utils::{c_to_str, open_flags_from_mode_string, C_EMPTY_STR};

use xet_interface as xet;
// 0666, copied from sys/stat.h
const DEFFILEMODE: mode_t = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;

#[inline]
unsafe fn fopen_impl(
    pathname: *const c_char,
    mode: *const c_char,
    callback: impl Fn() -> *mut libc::FILE,
) -> *mut libc::FILE {
    ld_func_trace!("fopen_impl", pathname, mode);

    // Convert fopen mode to OpenOptions
    let mode_str = CStr::from_ptr(mode).to_str().unwrap();
    let Some(open_flags) = open_flags_from_mode_string(mode_str) else {
        ld_warn!("Bad open flags? {mode_str}");
        return callback();
    };

    if xet::file_needs_materialization(open_flags) {
        xet::ensure_file_materialized_c(pathname);
        // no need to interpose a non-pointer file
        return callback();
    }

    // only interpose read
    if open_flags & O_ACCMODE == O_RDONLY {
        let ret = callback();
        if ret.is_null() {
            ld_trace!("fopen_impl: callback returned null.");
            return null_mut();
        }

        let fd = fileno(ret);

        xet::register_read_fd_c(pathname, fd);

        ret
    } else {
        ld_trace!("fopen_impl: passing through.");
        callback()
    }
}

hook! {
    unsafe fn fopen(pathname: *const c_char, mode: *const c_char) -> *mut libc::FILE => my_fopen {
        ld_func_trace!("fopen", pathname, mode);

        if interposing_disabled() { return real!(fopen)(pathname, mode); }

        let _ig = with_interposing_disabled();

        if !is_regular_file(pathname) {
            // We only interpose for regular files (not for socket, symlink, block dev, directory, character device (/dev/tty), fifo).
            return real!(fopen)(pathname, mode);
        }

        fopen_impl(pathname, mode, || real!(fopen)(pathname, mode))
    }
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn fopen64(pathname: *const c_char, mode: *const c_char) -> *mut libc::FILE => my_fopen64 {
        ld_func_trace!("fopen64", pathname, mode);
        if interposing_disabled() { return real!(fopen64)(pathname, mode); }

        let _ig = with_interposing_disabled();

        let path = unsafe { c_to_str(pathname) };
        fopen_impl(path, mode, || real!(fopen64)(pathname, mode))
    }
}

hook! {
    unsafe fn freopen(pathname: *const libc::c_char, mode: *const libc::c_char, stream: *mut libc::FILE) -> *mut libc::FILE => my_freopen {
        let path_s = c_to_str(pathname);
        ld_func_trace!("freopen", path_s, mode);
        if interposing_disabled() { return real!(freopen)(pathname, mode, stream); }

        let _ig = with_interposing_disabled();

        let fd = fileno(stream);

        // Close this if registered.
        if xet::close_fd_if_registered(fd) {
            my_fopen(pathname, mode)
        } else {
            real!(freopen)(pathname, mode, stream)
        }
    }
}

#[inline]
unsafe fn open_impl(
    pathname: *const c_char,
    open_flags: c_int,
    callback: impl Fn() -> c_int,
) -> c_int {
    if xet::file_needs_materialization(open_flags) {
        xet::ensure_file_materialized_c(pathname);
        // no need to interpose a non-pointer file
        return callback();
    }

    // only interpose read
    if open_flags & O_ACCMODE == O_RDONLY {
        ld_trace!("file {} is RDONLY", c_to_str(pathname));
        let fd = callback();
        ld_trace!("file {} is RDONLY, opened as {fd}", c_to_str(pathname));
        if fd != -1 {
            xet::register_read_fd_c(pathname, fd);
        }
        fd
    } else {
        callback()
    }
}

// Hook for open
hook! {
    unsafe fn open(pathname: *const c_char, flags: c_int, filemode: mode_t) -> c_int => my_open {
        let path = c_to_str(pathname);
        ld_func_trace!("open", path, flags, filemode);

        activate_fd_runtime();

        if interposing_disabled() {
            return real!(open)(pathname, flags, filemode);
        }

        let _ig = with_interposing_disabled();

        if !is_regular_file(pathname) {
            // We only interpose for regular files (not for socket, symlink, block dev, directory, character device (/dev/tty), fifo).
            return real!(open)(pathname, flags, filemode);
        }

        open_impl(pathname, flags,  || real!(open)(pathname, flags, filemode))
    }
}

#[inline]
unsafe fn real_open(pathname: *const c_char, flags: c_int, filemode: mode_t) -> c_int {
    real!(open)(pathname, flags, filemode)
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn open64(pathname: *const c_char, flags: c_int, filemode: mode_t) -> c_int => my_open64 {
        let path = c_to_str(pathname);
        ld_func_trace!("open64", path, flags, filemode);
        activate_fd_runtime();

        if interposing_disabled() {
            return real!(open64)(pathname, flags, filemode);
        }

        let _ig = with_interposing_disabled();

        // We only interpose for regular files (not for socket, symlink, block dev, directory, character device (/dev/tty), fifo).
        if !is_regular_file(pathname) {
            return real!(open64)(pathname, flags, filemode);
        }

        open_impl(pathname, flags, || real!(open64)(pathname, flags, filemode))
    }
}

hook! {
    unsafe fn openat(dirfd: libc::c_int, pathname: *const libc::c_char, flags: libc::c_int, filemode : mode_t) -> libc::c_int => my_openat {
        let path_s = c_to_str(pathname);
        ld_func_trace!("openat", dirfd, path_s, filemode);
        activate_fd_runtime();

        if !interposing_disabled() {

            let _ig = with_interposing_disabled();

            if let Some(path) = resolve_path_from_fd(dirfd, pathname) {

                // We only interpose for regular files (not for socket, symlink, block dev, directory, character device (/dev/tty), fifo).
                if is_regular_file(path.as_ptr()) {
                    ld_trace!("openat: Path {} is regular file, calling open_impl (dirfd={dirfd}, pathname={})", path.to_str().unwrap(), c_to_str(pathname));
                    return open_impl(path.as_ptr(), flags, || real!(openat)(dirfd, pathname, flags, filemode));
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
        ld_func_trace!("read", fd, nbyte);
        if interposing_disabled() || fd <= 2 { return real!(read)(fd, buf, nbyte); }
        let _ig = with_interposing_disabled();

        if let Some(fd_info) = xet::maybe_fd_read_managed(fd) {
            ld_trace!("read: Interposed read called with {nbyte} bytes on fd = {fd}");
            fd_info.read(buf, nbyte)
        } else {
            ld_trace!("read: non-interposed read called with {nbyte} bytes on fd = {fd}");
            real!(read)(fd, buf, nbyte)
        }
    }
}

hook! {
    unsafe fn fread(buf: *mut c_void, size: size_t, count: size_t, stream: *mut libc::FILE) -> size_t => my_fread {
        ld_func_trace!("fread", size, count);
        if interposing_disabled() { return real!(fread)(buf, size, count, stream); }
        let _ig = with_interposing_disabled();

        let fd = fileno(stream);

        if let Some(fd_info) = xet::maybe_fd_read_managed(fd) {
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

    if let Some(fd_info) = xet::maybe_fd_read_managed(fd) {
        ld_trace!("XetLDFS: fstat called on {fd} is managed");
        fd_info.update_stat(buf);
    }

    r
}

hook! {
    unsafe fn fstat(fd: c_int, buf: *mut libc::stat) -> c_int => my_fstat {
        ld_func_trace!("fstat", fd);
        if interposing_disabled() { return real!(fstat)(fd, buf); }
        let _ig = with_interposing_disabled();

        stat_impl(fd, buf)
    }
}

unsafe fn real_fstat(fd: c_int, buf: *mut libc::stat) -> c_int {
    real!(fstat)(fd, buf)
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn fstat64(fd: c_int, buf: *mut libc::stat) -> c_int => my_fstat64 {
        ld_func_trace!("fstat64", fd);
        if interposing_disabled() { return real!(fstat64)(fd, buf); }
        let _ig = with_interposing_disabled();

        stat_impl(fd, buf)
    }
}

hook! {
    unsafe fn stat(pathname: *const libc::c_char, buf: *mut libc::stat) -> c_int => my_stat {
        ld_func_trace!("stat", pathname);
        if !process_in_interposable_state() { return real!(stat)(pathname, buf); }

        let fd = my_open(pathname, O_RDONLY, DEFFILEMODE);
        if fd == -1 {
            return real!(stat)(pathname, buf);
        }

        stat_impl(fd, buf)
    }
}

#[allow(unused)]
unsafe fn real_stat(pathname: *const libc::c_char, buf: *mut libc::stat) -> c_int {
    real!(stat)(pathname, buf)
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn stat64(pathname: *const libc::c_char, buf: *mut libc::stat) -> c_int => my_stat64 {
        ld_func_trace!("stat64", pathname);
        if !process_in_interposable_state() { return real!(stat64)(pathname, buf); }

        let fd = my_open(pathname, O_RDONLY, DEFFILEMODE);
        if fd == -1 {
            return real!(stat64)(pathname, buf);
        }

        stat_impl(fd, buf)
    }
}

hook! {
    unsafe fn fstatat(dirfd: libc::c_int, pathname: *const libc::c_char, buf: *mut libc::stat, flags: libc::c_int) -> libc::c_int => my_fstatat {
        ld_func_trace!("fstatat", dirfd, pathname);
        if !process_in_interposable_state() { return real!(fstatat)(dirfd, pathname, buf, flags); }

        let fd = {
            if pathname.is_null() || *pathname == (0 as c_char) {
                dirfd
            } else {
                my_openat(dirfd, pathname, flags, DEFFILEMODE)
            }
        };
        if fd == -1 {
            return real!(fstatat)(dirfd, pathname, buf, flags);
        }

        stat_impl(fd, buf)
    }
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn fstatat64(dirfd: libc::c_int, pathname: *const libc::c_char, buf: *mut libc::stat, flags: libc::c_int) -> libc::c_int => my_fstatat64 {
        ld_func_trace!("fstatat64", dirfd, pathname);
        if !process_in_interposable_state() { return real!(fstatat)(dirfd, pathname, buf, flags); }

        let fd = {
            if pathname.is_null() || *pathname == (0 as c_char) {
                dirfd
            } else {
                my_openat(dirfd, pathname, flags, DEFFILEMODE)
            }
        };
        if fd == -1 {
            return real!(fstatat64)(dirfd, pathname, buf, flags);
        }

        stat_impl(fd, buf)
    }
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn statx(dirfd: libc::c_int, pathname: *const libc::c_char, flags: libc::c_int, mask: libc::c_uint, statxbuf: *mut libc::statx) -> libc::c_int => my_statx {
        ld_func_trace!("statx", dirfd, pathname, flags, mask);
        if !process_in_interposable_state() { return real!(statx)(dirfd, pathname, flags, mask, statxbuf); }

        let fd = {
            if pathname.is_null() || *pathname == (0 as c_char) {
                dirfd
            } else {
                ld_trace!("statx: attempting to open path on {dirfd}, pathname = {:?}", c_to_str(pathname));
                my_openat(dirfd, pathname, flags, DEFFILEMODE)
            }
        };

        if fd == -1 {
            ld_trace!("statx: openat returned -1, passing through.");
            return real!(statx)(dirfd, pathname, flags, mask, statxbuf);
        }

        // If pathname is an empty string and the AT_EMPTY_PATH flag
        // is specified in flags (see below), then the target file is
        // the one referred to by the file descriptor dirfd.
        let ret = real!(statx)(fd, C_EMPTY_STR, AT_EMPTY_PATH | flags, mask, statxbuf);
        if ret == EOF {
            return real!(statx)(dirfd, pathname, flags, mask, statxbuf);
        }

        if let Some(fd_info) = maybe_fd_read_managed(fd) {
            ld_trace!("statx: update_statx called on {fd}, is managed");
            fd_info.update_statx(statxbuf);
        } else {
            ld_trace!("statx called on {fd}; passed through.");
        }


        ret
    }
}

hook! {
    unsafe fn lseek(fd: libc::c_int, offset: libc::off_t, whence: libc::c_int) -> libc::off_t => my_lseek {
        ld_func_trace!("lseek", fd, offset, whence);
        if interposing_disabled() { return real!(lseek)(fd, offset, whence); }
        let _ig = with_interposing_disabled();

        if let Some(fd_info) = xet::maybe_fd_read_managed(fd) {
            let ret = fd_info.lseek(offset, whence);
            ld_trace!("XetLDFS: lseek called, offset={offset}, whence={whence}, fd={fd}: ret={ret}");
            ret
        } else {
            real!(lseek)(fd, offset, whence)
        }
    }
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn lseek64(fd: libc::c_int, offset: libc::off64_t, whence: libc::c_int) -> libc::off64_t => my_lseek64 {
        ld_func_trace!("lseek64", fd, offset, whence);
        if interposing_disabled() { return real!(lseek)(fd, offset, whence); }
        let _ig = with_interposing_disabled();

        if let Some(fd_info) = maybe_fd_read_managed(fd) {
            let ret = fd_info.lseek(offset, whence);
            ld_trace!("XetLDFS: lseek64 called, offset={offset}, whence={whence}, fd={fd}: ret={ret}");
            ret
        } else {
            real!(lseek64)(fd, offset, whence)
        }
    }
}

hook! {
    unsafe fn fseek(stream: *mut libc::FILE, offset: libc::c_long, whence: libc::c_int) -> libc::c_long => my_fseek {
        ld_func_trace!("fseek", stream, offset, whence);
        if interposing_disabled() || stream.is_null() { return real!(fseek)(stream, offset, whence); }
        let _ig = with_interposing_disabled();

        let fd = fileno(stream);
        if fd < 0 {
            return real!(fseek)(stream, offset, whence);
        }

        if let Some(fd_info) = xet::maybe_fd_read_managed(fd) {
            let ret = fd_info.lseek(offset, whence) as libc::c_long;
            ld_trace!("XetLDFS: lseek called, offset={offset}, whence={whence}, fd={fd}: ret={ret}");
            ret
        } else {
            real!(fseek)(stream, offset, whence)
        }
    }
}

#[inline]
unsafe fn interposed_close(fd: libc::c_int) -> libc::c_int {
    ld_trace!("close called on {fd}");
    xet::close_fd_if_registered(fd);
    real!(close)(fd)
}

#[inline]
unsafe fn real_close(fd: libc::c_int) -> libc::c_int {
    real!(close)(fd)
}

hook! {
    unsafe fn close(fd: libc::c_int) -> libc::c_int => my_close {
        ld_func_trace!("close", fd);
        if interposing_disabled() { return real!(close)(fd); }
        let _ig = with_interposing_disabled();
        interposed_close(fd)
    }
}

hook! {
    unsafe fn fclose(stream: *mut libc::FILE) -> libc::c_int => my_fclose {
        ld_func_trace!("close", stream);
        if interposing_disabled() || stream.is_null() { return real!(fclose)(stream); }
        let _ig = with_interposing_disabled();

        let fd = fileno(stream);

        ld_trace!("fclose called on {fd}");

        xet::close_fd_if_registered(fd);

        real!(fclose)(stream)
    }
}

hook! {
    unsafe fn ftell(stream: *mut libc::FILE) -> libc::c_long => my_ftell {
        ld_func_trace!("ftell", stream);
        if interposing_disabled() || stream.is_null() { return real!(ftell)(stream); }
        let _ig = with_interposing_disabled();

        let fd = fileno(stream);

        if let Some(fd_info) = xet::maybe_fd_read_managed(fd) {
            let ret = fd_info.ftell() as libc::c_long;
            ld_trace!("ftell: called on {fd}; interposed, ret = {ret}");
            ret
        } else {
            real!(ftell)(stream)
        }
    }
}

hook! {
    unsafe fn dup(old_fd: libc::c_int) -> libc::c_int => my_dup {
        ld_func_trace!("dup", old_fd);
        if interposing_disabled() { return real!(dup)(old_fd); }
        let _ig = with_interposing_disabled();

        let new_fd = real!(dup)(old_fd);

        if new_fd < 0 {
            return new_fd;
        }

        if let Some(fd_info) = xet::maybe_fd_read_managed(old_fd) {
            ld_trace!("dup: fd={new_fd} to point to same file as {old_fd}, path={:?}", fd_info.path());
            xet::set_fd_read_interpose(new_fd, fd_info.dup(new_fd));
        }

        new_fd
    }
}

hook! {
    unsafe fn dup2(old_fd: libc::c_int, new_fd: libc::c_int) -> libc::c_int => my_dup2 {
        ld_func_trace!("dup2", old_fd, new_fd);
        if interposing_disabled() { return real!(dup2)(old_fd, new_fd); }
        let _ig = with_interposing_disabled();

        let result = real!(dup2)(old_fd, new_fd);
        if result < 0 {
            return result;
        }

        // If old_fd and new_fd are equal, then dup2() just returns new_fd;
        // no other changes are made to the existing descriptor.
        if old_fd != new_fd {
            xet::close_fd_if_registered(new_fd);

            if let Some(fd_info) = xet::maybe_fd_read_managed(old_fd) {
                ld_trace!("dup2: fd={new_fd} to point to same file as {old_fd}, path={:?}", fd_info.path());
                xet::set_fd_read_interpose(new_fd, fd_info.dup(new_fd));
            }
        }

        result
    }
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
        ld_func_trace!("mmap", length, prot, flags,fd, offset);

        // Avoid cyclic internal calls to malloc when actually allocating virtual memory,
        // or when mmap non-regular file, e.g. shared memory
        if (flags & libc::MAP_ANON != 0) || (flags & libc::MAP_ANONYMOUS != 0) || !is_regular_fd(fd) {
            return real!(mmap)(addr, length, prot, flags, fd, offset);
        }

        if process_in_interposable_state() {
            if xet::materialize_file_under_fd(fd) {
                ld_trace!("mmap: Materialized pointer file under descriptor {fd}.");
            } else {
                ld_trace!("mmap: fd={fd} not registered.");
            }
        }

        // Call the original mmap function
        real!(mmap)(addr, length, prot, flags, fd, offset)
    }
}
