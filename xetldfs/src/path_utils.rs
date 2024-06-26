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

pub fn absolute_path_from_dirfd(dirfd: libc::c_int, path: *const libc::c_char) -> Option<String> {
    // Convert pathname to Rust string
    let c_str = unsafe { CStr::from_ptr(path) };
    let relative_path = c_str.to_string_lossy().into_owned();

    // Check if the path is absolute
    if PathBuf::from(&relative_path).is_absolute() {
        return Some(relative_path);
    }

    let mut absolute_dir_path = vec![0; libc::PATH_MAX as usize];

    if dirfd == libc::AT_FDCWD {
        // Resolve current working directory to an absolute path
        let cwd = std::env::current_dir().ok()?;
        let cwd_str = cwd.to_string_lossy().into_owned();
        absolute_dir_path = cwd_str.into_bytes();
    } else {
        #[cfg(target_os = "linux")]
        {
            // On Linux, read the symbolic link at /proc/self/fd/dirfd
            let dir_path = format!("/proc/self/fd/{}", dirfd);
            let len = unsafe {
                let c_dir_path = CString::new(dir_path).unwrap();
                libc::readlink(
                    c_dir_path.as_ptr(),
                    absolute_dir_path.as_mut_ptr() as *mut i8,
                    absolute_dir_path.len(),
                )
            };
            if len == -1 {
                return None;
            }
            absolute_dir_path.truncate(len as usize);
        }

        #[cfg(target_os = "macos")]
        {
            // On macOS, use fcntl with F_GETPATH
            if unsafe { libc::fcntl(dirfd, libc::F_GETPATH, absolute_dir_path.as_mut_ptr()) } == -1
            {
                return None;
            }
            // Trim null byte at the end
            absolute_dir_path.truncate(
                absolute_dir_path
                    .iter()
                    .position(|&c| c == 0)
                    .unwrap_or(absolute_dir_path.len()),
            );
        }
    }

    let absolute_dir_path = String::from_utf8_lossy(&absolute_dir_path).into_owned();

    // Concatenate to get the absolute path
    let absolute_path = PathBuf::from(absolute_dir_path).join(relative_path);

    Some(absolute_path.to_string_lossy().into_owned())
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
