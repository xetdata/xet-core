use crate::runtime::{
    interposing_disabled, process_in_interposable_state, with_interposing_disabled,
};
use crate::{hook, my_open, my_openat, real};
use libc::*;

// 0666, copied from sys/stat.h
const DEFFILEMODE: mode_t = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;

#[derive(Debug)]
enum PathMode {
    PathName(*const libc::c_char),
    Fd(c_int),
    PathFromFd(c_int, *const libc::c_char),
}

enum StatStructBuf {
    Stat(*mut libc::stat),

    #[cfg(target_os = "linux")]
    Stat64(*mut libc::stat64),

    #[cfg(target_os = "linux")]
    StatX(*mut libc::statx),
}

#[inline]
unsafe fn stat_impl(
    name: &str,
    file_info: PathMode,
    stat_buf: StatStructBuf,
    base: impl Fn() -> c_int,
) -> c_int {
    ld_func_trace!("{name}", file_info);

    let r = base();

    if !process_in_interposable_state() || r < 0 {
        return r;
    }

    let (fd, respect_interposing) = match file_info {
        PathMode::PathName(pathname) => (my_open(pathname, O_RDONLY, DEFFILEMODE), false),
        PathMode::Fd(fd) => (fd, true),
        PathMode::PathFromFd(dirfd, pathname) => {
            if pathname.is_null() || *pathname == (0 as c_char) {
                (dirfd, true)
            } else {
                (my_openat(dirfd, pathname, O_RDONLY, DEFFILEMODE), false)
            }
        }
    };

    let _maybe_lg = {
        if respect_interposing {
            if interposing_disabled() {
                return r;
            }
            Some(with_interposing_disabled())
        } else {
            None
        }
    };

    // Ignore stdin, stdout, and stderr
    if fd >= 3 {
        if let Some(fd_info) = crate::xet_interface::maybe_fd_read_managed(fd) {
            ld_trace!("XetLDFS: {name} called on {fd} is managed");

            match stat_buf {
                StatStructBuf::Stat(buf) => {
                    fd_info.update_stat(buf);
                }
                #[cfg(target_os = "linux")]
                StatStructBuf::Stat64(buf) => {
                    fd_info.update_stat64(buf);
                }
                #[cfg(target_os = "linux")]
                StatStructBuf::StatX(buf) => {
                    fd_info.update_statx(buf);
                }
            };
        }
    }

    r
}

// int stat(const char *pathname, struct stat *buf);
hook! {
    unsafe fn stat(pathname: *const libc::c_char, buf: *mut libc::stat) -> c_int => stat_hook {
        stat_impl("stat", PathMode::PathName(pathname), StatStructBuf::Stat(buf), || {
            #[cfg(target_os = "macos")]
            {
                real!(stat)(pathname, buf)
            }
            #[cfg(target_os = "linux")]
            {
                if fn_is_valid!(stat) {
                    ld_trace!("linux stat: stat called directly.");
                    real!(stat)(pathname, buf)
                } else if fn_is_valid!(__xstat) {
                    ld_trace!("linux stat: fallback to __xstat.");
                    real!(__xstat)(3, pathname, buf)
                } else {
                    ld_error!("Unknown function stat");
                    errno::set_errno(errno::Errno(ENOSYS)); // Function does not exist
                    return -1;
                }
            }
        })
    }
}

// int __xstat(int ver, const char *pathname, struct stat *buf);
#[cfg(target_os = "linux")]
hook! {
    unsafe fn __xstat(ver : c_int, pathname: *const libc::c_char, buf: *mut libc::stat) -> c_int => __xstat_hook {
        stat_impl("__xstat", PathMode::PathName(pathname), StatStructBuf::Stat(buf), || real!(__xstat)(ver, pathname, buf))
    }
}

// int stat64(const char *pathname, struct stat64 *buf);
#[cfg(target_os = "linux")]
hook! {
    unsafe fn stat64(pathname: *const libc::c_char, buf: *mut libc::stat64) -> c_int => stat64_hook {
        stat_impl("stat64", PathMode::PathName(pathname), StatStructBuf::Stat64(buf), || real!(stat64)(pathname, buf))
    }
}

// int __xstat64(int ver, const char *pathname, struct stat64 *buf);
#[cfg(target_os = "linux")]
hook! {
    unsafe fn __xstat64(ver : c_int, pathname: *const libc::c_char, buf: *mut libc::stat64) -> c_int => __xstat64_hook {
        stat_impl("__xstat64", PathMode::PathName(pathname), StatStructBuf::Stat64(buf), || real!(__xstat64)(ver, pathname, buf))
    }
}

// int lstat(const char *pathname, struct stat *buf);
hook! {
    unsafe fn lstat(pathname: *const libc::c_char, buf: *mut libc::stat) -> c_int => lstat_hook {
        stat_impl("lstat", PathMode::PathName(pathname), StatStructBuf::Stat(buf),
        || {
            #[cfg(target_os = "macos")]
            {
                real!(lstat)(pathname, buf)
            }
            #[cfg(target_os = "linux")]
            {
                if fn_is_valid!(lstat) {
                    real!(lstat)(pathname, buf)
                } else if fn_is_valid!(__lxstat){
                    real!(__lxstat)(3, pathname, buf)
                } else {
                    ld_error!("Unknown function lstat");
                    errno::set_errno(errno::Errno(ENOSYS)); // Function does not exist
                    return -1;
                }
            }
        })
    }
}

// int __lxstat(int ver, const char *pathname, struct stat *buf);
#[cfg(target_os = "linux")]
hook! {
    unsafe fn __lxstat(ver : c_int, pathname: *const libc::c_char, buf: *mut libc::stat) -> c_int => __lxstat_hook {
        stat_impl("__lxstat", PathMode::PathName(pathname), StatStructBuf::Stat(buf), || real!(__lxstat)(ver, pathname, buf))
    }
}

// int lstat64(const char *pathname, struct stat64 *buf);
#[cfg(target_os = "linux")]
hook! {
    unsafe fn lstat64(pathname: *const libc::c_char, buf: *mut libc::stat64) -> c_int => lstat64_hook {
        stat_impl("lstat64", PathMode::PathName(pathname), StatStructBuf::Stat64(buf), || real!(lstat64)(pathname, buf))
    }
}

// int __lxstat64(int ver, const char *pathname, struct stat64 *buf);
#[cfg(target_os = "linux")]
hook! {
    unsafe fn __lxstat64(ver : c_int, pathname: *const libc::c_char, buf: *mut libc::stat64) -> c_int => __lxstat64_hook {
        stat_impl("__lxstat64", PathMode::PathName(pathname), StatStructBuf::Stat64(buf), || real!(__lxstat64)(ver, pathname, buf))
    }
}

// int fstat(int fd, struct stat *buf);
hook! {
    unsafe fn fstat(fd: c_int, buf: *mut libc::stat) -> c_int => fstat_hook {
        stat_impl("fstat", PathMode::Fd(fd), StatStructBuf::Stat(buf),
        || {

            #[cfg(target_os = "macos")]
            {
                real!(fstat)(fd, buf)
            }
            #[cfg(target_os = "linux")]
            {
                if fn_is_valid!(fstat) {
                    real!(fstat)(fd, buf)
                } else if fn_is_valid!(__fxstat){
                    real!(__fxstat)(3, fd, buf)
                } else {
                     ld_error!("Unknown function fstat");
                    errno::set_errno(errno::Errno(ENOSYS)); // Function does not exist
                    return -1;
                }
            }
        })
    }
}

// int __fxstat(int ver, int fd, struct stat *buf);
#[cfg(target_os = "linux")]
hook! {
    unsafe fn __fxstat(ver : c_int, fd: c_int, buf: *mut libc::stat) -> c_int => __fxstat_hook {
        stat_impl("__fxstat", PathMode::Fd(fd), StatStructBuf::Stat(buf), || real!(__fxstat)(ver, fd, buf))
    }
}

// int fstat64(int fd, struct stat64 *buf);
#[cfg(target_os = "linux")]
hook! {
    unsafe fn fstat64(fd: c_int, buf: *mut libc::stat64) -> c_int => fstat64_hook {
        stat_impl("fstat64", PathMode::Fd(fd), StatStructBuf::Stat64(buf), || real!(fstat64)(fd, buf))
    }
}

// int __fxstat64(int ver, int fd, struct stat64 *buf);
#[cfg(target_os = "linux")]
hook! {
    unsafe fn __fxstat64(ver : c_int, fd: c_int, buf: *mut libc::stat64) -> c_int => __fxst64_hook {
        stat_impl("__fxstat64", PathMode::Fd(fd), StatStructBuf::Stat64(buf), || real!(__fxstat64)(ver, fd, buf))
    }
}

// int fstatat(int dirfd, const char *pathname, struct stat *buf, int flags);
hook! {
    unsafe fn fstatat(dirfd: libc::c_int, pathname: *const libc::c_char, buf: *mut libc::stat, flags: libc::c_int) -> libc::c_int => fstatat_hook {
        stat_impl("fstatat", PathMode::PathFromFd(dirfd, pathname), StatStructBuf::Stat(buf),
        || real!(fstatat)(dirfd, pathname, buf, flags))
    }
}

// int __fxstatat(int ver, int dirfd, const char *pathname, struct stat *buf, int flags);
#[cfg(target_os = "linux")]
hook! {
    unsafe fn __fxstatat(ver : c_int, dirfd: libc::c_int, pathname: *const libc::c_char, buf: *mut libc::stat, flags: libc::c_int) -> libc::c_int => __fxstatat_hook {
        stat_impl("__fxstatat", PathMode::PathFromFd(dirfd, pathname), StatStructBuf::Stat(buf),
        || real!(__fxstatat)(ver, dirfd, pathname, buf, flags))
    }
}

// int fstatat64(int dirfd, const char *pathname, struct stat64 *buf, int flags);
#[cfg(target_os = "linux")]
hook! {
    unsafe fn fstatat64(dirfd: libc::c_int, pathname: *const libc::c_char, buf: *mut libc::stat64, flags: libc::c_int) -> libc::c_int => fstatat64_hook {
        stat_impl("fstatat64", PathMode::PathFromFd(dirfd, pathname), StatStructBuf::Stat64(buf),
        || real!(fstatat64)(dirfd, pathname, buf, flags))
    }
}

// int __fxstatat64(int ver, int dirfd, const char *pathname, struct stat64 *buf, int flags);
#[cfg(target_os = "linux")]
hook! {
    unsafe fn __fxstatat64(ver : c_int, dirfd: libc::c_int, pathname: *const libc::c_char, buf: *mut libc::stat64, flags: libc::c_int) -> libc::c_int => __fxstatat64_hook {
        stat_impl("__fxstatat64", PathMode::PathFromFd(dirfd, pathname), StatStructBuf::Stat64(buf),
        || real!(__fxstatat64)(ver, dirfd, pathname, buf, flags))
    }
}

// int statx(int dirfd, const char *pathname, int flags, unsigned int mask, struct statx *buf);
#[cfg(target_os = "linux")]
hook! {
    unsafe fn statx(dirfd: libc::c_int, pathname: *const libc::c_char, flags: libc::c_int, mask: libc::c_uint, statxbuf: *mut libc::statx) -> libc::c_int => statx_hook {
        stat_impl("statx", PathMode::PathFromFd(dirfd, pathname), StatStructBuf::StatX(statxbuf),
        || real!(statx)(dirfd, pathname, flags, mask, statxbuf))
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn is_regular_file(pathname: *const libc::c_char) -> bool {
    #[cfg(target_os = "macos")]
    unsafe {
        let mut buf: libc::stat = std::mem::zeroed();
        let buf_ptr = &mut buf as *mut libc::stat;

        let ret = real!(stat)(pathname, buf_ptr);
        ret == 0 && ((*buf_ptr).st_mode & libc::S_IFMT) == libc::S_IFREG
    }

    #[cfg(target_os = "linux")]
    unsafe {
        use libc::STATX_BASIC_STATS;
        // Create a statx buffer to hold the results
        let mut statx_buf: libc::statx = std::mem::zeroed();

        let statx_buf_ptr = &mut statx_buf as *mut libc::statx;

        // Call statx with AT_FDCWD (current working directory) and 0 flags
        let ret = real!(statx)(AT_FDCWD, pathname, 0, STATX_BASIC_STATS, statx_buf_ptr);
        ret == 0 && (statx_buf.stx_mode & (libc::S_IFMT as u16)) == libc::S_IFREG as u16
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn is_regular_fd(fd: libc::c_int) -> bool {
    #[cfg(target_os = "macos")]
    unsafe {
        let mut buf: libc::stat = std::mem::zeroed();
        let buf_ptr = &mut buf as *mut libc::stat;

        let ret = real!(fstat)(fd, buf_ptr);
        (ret == 0) && (*buf_ptr).st_mode & libc::S_IFMT == libc::S_IFREG
    }

    // Create a statx buffer to hold the results
    #[cfg(target_os = "linux")]
    unsafe {
        use crate::path_utils::path_of_fd;
        use libc::STATX_BASIC_STATS;

        // Convert the file descriptor to a path representation: "/proc/self/fd/<fd>"
        let Some(path) = path_of_fd(fd) else {
            return false;
        };

        let mut statx_buf: libc::statx = std::mem::zeroed();
        let statx_buf_ptr = &mut statx_buf as *mut libc::statx;

        // Call statx with AT_FDCWD (current working directory) and 0 flags
        let ret = real!(statx)(
            AT_FDCWD,
            path.as_ptr() as *const c_char,
            0,
            STATX_BASIC_STATS,
            statx_buf_ptr,
        );
        ret == 0 && (statx_buf.stx_mode & (libc::S_IFMT as u16)) == libc::S_IFREG as u16
    }
}
