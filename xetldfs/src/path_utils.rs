use crate::{c_chars_to_cstring, real_fstat, real_stat};
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

#[cfg(target_os = "linux")]
fn path_of_fd_impl(fd: libc::c_int) -> Option<Vec<c_char>> {
    let mut dest_path = vec![0 as c_char; libc::PATH_MAX as usize];

    // On Linux, read the symbolic link at /proc/self/fd/dirfd
    let path = format!("/proc/self/fd/{}\0", fd);
    let c_path = CStr::from_bytes_with_nul(path.as_bytes()).unwrap();

    let len = unsafe {
        libc::readlink(
            c_path.as_ptr(),
            dest_path.as_mut_ptr() as *mut c_char,
            dest_path.len(),
        )
    };

    if len < 0 {
        return None;
    }

    dest_path.truncate(len as usize);
    Some(dest_path)
}

#[cfg(target_os = "macos")]
fn path_of_fd_impl(fd: libc::c_int) -> Option<Vec<c_char>> {
    let mut dest_path = vec![0 as c_char; libc::PATH_MAX as usize];

    // On macOS, use fcntl with F_GETPATH
    if unsafe { libc::fcntl(fd, libc::F_GETPATH, dest_path.as_mut_ptr()) } == -1 {
        return None;
    }

    let len = dest_path
        .iter()
        .position(|&c| c == 0)
        .unwrap_or(dest_path.len());

    dest_path.truncate(len as usize);
    Some(dest_path)
}

pub fn path_of_fd(fd: libc::c_int) -> Option<CString> {
    path_of_fd_impl(fd).map(c_chars_to_cstring)
}

unsafe fn get_cwd() -> Option<Vec<c_char>> {
    let mut dest_path = vec![0 as c_char; libc::PATH_MAX as usize];

    let cwd_ptr = unsafe { libc::getcwd(dest_path.as_mut_ptr(), dest_path.len()) };
    if cwd_ptr.is_null() {
        return None;
    }

    dest_path.truncate(
        dest_path
            .iter()
            .position(|&c| c == 0)
            .unwrap_or(dest_path.len()),
    );
    Some(dest_path)
}

pub fn resolve_path_from_fd(dirfd: libc::c_int, path: *const libc::c_char) -> Option<CString> {
    unsafe {
        if path.is_null() || *path == 0 {
            let dest_path = path_of_fd_impl(dirfd)?;
            return Some(c_chars_to_cstring(dest_path));
        }

        // Check if the path is absolute
        if *path == b'/' as c_char {
            return Some(CStr::from_ptr(path).to_owned());
        }

        let mut dest_path = {
            if dirfd == libc::AT_FDCWD {
                get_cwd()?
            } else {
                path_of_fd_impl(dirfd)?
            }
        };

        assert_ne!(*dest_path.last().unwrap(), b'\0' as c_char);
        dest_path.push(b'/' as c_char);
        for i in 0.. {
            let c = *(path.add(i));
            if c == 0 {
                break;
            } else {
                dest_path.push(c);
            }
        }

        Some(c_chars_to_cstring(dest_path))
    }
}

pub fn is_regular_file(pathname: *const libc::c_char) -> bool {
    let mut buf: libc::stat = unsafe { std::mem::zeroed() };
    let buf_ptr = &mut buf as *mut libc::stat;
    unsafe {
        let ret = real_stat(pathname, buf_ptr);
        if ret == -1 {
            return false;
        }
        (*buf_ptr).st_mode & libc::S_IFMT == libc::S_IFREG
    }
}

pub fn is_regular_fd(fd: libc::c_int) -> bool {
    let mut buf: libc::stat = unsafe { std::mem::zeroed() };
    let buf_ptr = &mut buf as *mut libc::stat;
    unsafe {
        let ret = real_fstat(fd, buf_ptr);
        if ret == -1 {
            return false;
        }
        (*buf_ptr).st_mode & libc::S_IFMT == libc::S_IFREG
    }
}
