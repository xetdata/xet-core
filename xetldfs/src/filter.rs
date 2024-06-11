use errno::{set_errno, Errno};
use libc::{c_char, c_int, c_void, EOF, O_ACCMODE, O_RDONLY, O_RDWR, SEEK_CUR, SEEK_SET};
use libxet::{constants::POINTER_FILE_LIMIT, data::PointerFile};
use std::{ffi::CStr, mem::size_of, sync::Arc};

use crate::{real_fstat, real_lseek, real_read, xet_rfile::XetFdReadHandle};

pub enum FileType {
    Regular(u64),
    Pointer(PointerFile),
}

pub fn file_metadata(
    fd_info: &Arc<XetFdReadHandle>,
    buf: Option<*mut libc::stat>,
) -> Option<FileType> {
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

    unsafe { Some(FileType::Regular(disk_size)) }
}
