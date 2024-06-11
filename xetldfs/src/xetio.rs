use errno::{set_errno, Errno};
use lazy_static::lazy_static;
use libc::{
    c_char, c_int, c_void, fileno, size_t, ssize_t, EOF, SEEK_CUR, SEEK_DATA, SEEK_END, SEEK_HOLE,
    SEEK_SET,
};
use std::collections::HashMap;
use std::sync::Mutex;
use std::{
    ffi::CStr,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    sync::RwLock,
};

use crate::filter::{file_metadata, FileType};
use crate::tokio_runtime::TOKIO_RUNTIME;
use crate::xet_rfile::maybe_fd_read_managed;
use crate::{real_lseek, real_read, utils::*};

// size of buffer used by setbuf, copied from stdio.h
const BUFSIZ: c_int = 1024;

// Copied from fread.c
// The maximum amount to read to avoid integer overflow.  INT_MAX is odd,
// so it make sense to make it even.  We subtract (BUFSIZ - 1) to get a
// whole number of BUFSIZ chunks.
const MAXREAD: c_int = c_int::MAX - (BUFSIZ - 1);

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

pub fn read(fd: c_int, buf: *mut c_void, nbyte: size_t) -> ssize_t {}

pub fn internal_fstat(fd: c_int, buf: *mut libc::stat) -> c_int {
    let Some(fd_info) = get_fd_info(fd) else {
        return EOF.try_into().unwrap();
    };

    // get the stat of the file on disk
    let Some(metadata) = file_metadata(&fd_info, Some(buf)) else {
        return EOF;
    };

    if let FileType::Pointer(pf) = metadata {
        unsafe {
            (*buf).st_size = pf.filesize() as i64; /* file size, in bytes */
            (*buf).st_blocks = 0; // todo!() /* blocks allocated for file */
            (*buf).st_blksize = libxet::merkledb::constants::IDEAL_CAS_BLOCK_SIZE as i32;
            /* optimal blocksize for I/O */
        }
    }

    0
}

pub fn internal_lseek(fd: libc::c_int, offset: libc::off_t, whence: libc::c_int) -> libc::off_t {
    let Some(fd_info) = get_fd_info(fd) else {
        return EOF.try_into().unwrap();
    };

    //  whence is not valid?
    if !matches!(
        whence,
        SEEK_SET | SEEK_CUR | SEEK_END | SEEK_DATA | SEEK_HOLE
    ) {
        set_errno(Errno(libc::EINVAL));
        return EOF.try_into().unwrap();
    }

    let Some(metadata) = file_metadata(&fd_info, None) else {
        return EOF.try_into().unwrap();
    };

    if let FileType::Regular(_) = metadata {
        return unsafe { real_lseek(fd, offset, whence) };
    };

    let fsize = if let FileType::Pointer(pf) = metadata {
        pf.filesize()
    } else {
        unreachable!()
    };

    // lock because it's difficult to implement with pure atomic variable.
    let _flock = fd_info.lock.lock();

    let cur_pos = fd_info.pos.load(Ordering::Relaxed);

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
    if !matches!(whence, SEEK_DATA | SEEK_HOLE) && offset.is_positive() && offset as u64 == fsize {
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

    fd_info.pos.store(new_pos, Ordering::Relaxed);
    cur_pos.try_into().unwrap()
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
