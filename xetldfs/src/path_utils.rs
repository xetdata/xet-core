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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::io::AsRawFd;

    #[test]
    fn test_absolute_path() {
        let path = CString::new("/absolute/path/to/file.txt").unwrap();
        assert_eq!(
            absolute_path_from_dirfd(libc::AT_FDCWD, path.as_ptr()),
            Some("/absolute/path/to/file.txt".to_string())
        );
    }

    #[test]
    fn test_relative_path_with_at_fdcwd() {
        let path = CString::new("relative/path/to/file.txt").unwrap();
        let cwd = std::env::current_dir().unwrap();
        let expected_path = cwd.join("relative/path/to/file.txt");

        assert_eq!(
            absolute_path_from_dirfd(libc::AT_FDCWD, path.as_ptr()),
            Some(expected_path.to_string_lossy().into_owned())
        );
    }

    #[test]
    fn test_relative_path_with_dirfd() {
        // Create a temporary directory
        let temp_dir = tempfile::tempdir().unwrap();
        let dirfd = fs::File::open(temp_dir.path()).unwrap().as_raw_fd();

        let path = CString::new("relative/path/to/file.txt").unwrap();
        let expected_path = temp_dir.path().join("relative/path/to/file.txt");

        assert_eq!(
            absolute_path_from_dirfd(dirfd, path.as_ptr()),
            Some(expected_path.to_string_lossy().into_owned())
        );
    }

    #[test]
    fn test_non_existent_dirfd() {
        let path = CString::new("relative/path/to/file.txt").unwrap();
        let invalid_dirfd = -1; // Invalid file descriptor

        assert_eq!(absolute_path_from_dirfd(invalid_dirfd, path.as_ptr()), None);
    }
}
