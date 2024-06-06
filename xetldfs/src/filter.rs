use errno::{set_errno, Errno};
use libc::{c_char, c_int, O_ACCMODE, O_RDONLY, O_RDWR};
use std::ffi::CStr;

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
fn is_write_managed(pathname: *const c_char) -> bool {
    return false;
}
