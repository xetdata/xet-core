use errno::{set_errno, Errno};
use libc::{c_char, c_int, c_void, EOF, O_ACCMODE, O_RDONLY, O_RDWR, SEEK_CUR, SEEK_SET};
use libxet::{constants::POINTER_FILE_LIMIT, data::PointerFile};
use std::{ffi::CStr, mem::size_of, sync::Arc};

use crate::{real_fstat, real_lseek, real_read, FdInfo};

pub fn is_managed(pathname: *const c_char, flags: c_int) -> bool {
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
fn is_write_managed(_pathname: *const c_char) -> bool {
    return false;
}

pub enum FileType {
    Regular(u64),
    Pointer(PointerFile),
}

pub fn file_metadata(fd_info: &Arc<FdInfo>, buf: Option<*mut libc::stat>) -> Option<FileType> {
    let fd = fd_info.fd;

    let stat = buf.unwrap_or_else(|| {
        let mut stat = vec![0u8; size_of::<libc::stat>()];
        stat.as_mut_ptr() as *mut libc::stat
    });

    unsafe {
        if real_fstat(fd, stat) == EOF {
            return None;
        }
    }

    unsafe {
        let disk_size = (*stat).st_size as u64;
        // may be a pointer file
        if disk_size < POINTER_FILE_LIMIT as u64 {
            let flock = fd_info.lock.lock().unwrap();

            // get current pos
            let cur_pos = real_lseek(fd, 0, SEEK_CUR);
            if cur_pos == -1 {
                return None;
            }

            // move pos to begining of file
            if real_lseek(fd, 0, SEEK_SET) == -1 {
                return None;
            }

            // load all bytes from disk
            let mut file_contents = vec![0u8; disk_size as usize];
            if real_read(
                fd,
                file_contents.as_mut_ptr() as *mut c_void,
                disk_size as usize,
            ) == -1
            {
                return None;
            }

            // restore the origin pos
            if real_lseek(fd, cur_pos, SEEK_SET) == -1 {
                return None;
            }

            drop(flock);

            // check pointer file validity
            if let Ok(utf8_contents) = std::str::from_utf8(&file_contents) {
                let pf = PointerFile::init_from_string(utf8_contents, "" /* not important */);

                if pf.is_valid() {
                    return Some(FileType::Pointer(pf));
                }
            }
        }

        Some(FileType::Regular(disk_size))
    }
}
