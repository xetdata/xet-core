use crate::real_stat;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::{Path, PathBuf};

pub fn resolve_path(raw_path: &str) -> Result<PathBuf, std::io::Error> {
    let path = Path::new(raw_path);

    // Canonicalize the parent, which we expect to exist
    if path.is_absolute() {
        if let Some(parent) = path.parent() {
            let canonical_parent = std::fs::canonicalize(parent)?;
            Ok(canonical_parent.join(path.file_name().unwrap()))
        } else {
            Ok(path.to_path_buf())
        }
    } else {
        let abs_path = std::env::current_dir()?.join(path);
        if let Some(parent) = abs_path.parent() {
            let canonical_parent = std::fs::canonicalize(parent)?;
            Ok(canonical_parent.join(abs_path.file_name().unwrap()))
        } else {
            Ok(abs_path)
        }
    }
}

pub fn absolute_path_from_dirfd(dirfd: libc::c_int, path: *const libc::c_char) -> Option<CString> {
    let mut absolute_dir_path = vec![0u8; libc::PATH_MAX as usize];
    let n = unsafe {
        libc::readlinkat(
            dirfd,
            path,
            absolute_dir_path.as_mut_ptr() as *mut i8,
            absolute_dir_path.len(),
        )
    };

    if n == -1 {
        None
    } else {
        absolute_dir_path.truncate(n as usize);
        unsafe { Some(CString::from_vec_unchecked(absolute_dir_path)) }
    }
}

pub fn is_regular_file(pathname: *const libc::c_char) -> bool {
    let mut buf = [0u8; std::mem::size_of::<libc::stat>()];
    let buf_ptr = buf.as_mut_ptr() as *mut libc::stat;
    unsafe {
        let ret = real_stat(pathname, buf_ptr);
        if ret == -1 {
            return false;
        }
        (*buf_ptr).st_mode & libc::S_IFMT == libc::S_IFREG
    }
}
