use libc::{c_int, O_APPEND, O_RDWR, O_TRUNC};
use std::ffi::CStr;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};

pub unsafe fn c_to_str<'a>(c_str: *const libc::c_char) -> &'a str {
    let c_str = CStr::from_ptr(c_str);
    c_str.to_str().expect("Invalid UTF-8")
}

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

fn register_io_error_impl(err: std::io::Error, context: Option<&str>) -> std::io::Error {
    use libc::*;

    let (err_code, err_msg) = match err.kind() {
        ErrorKind::NotFound => (ENOENT, "File not found"),
        ErrorKind::PermissionDenied => (EACCES, "Permission denied"),
        ErrorKind::AlreadyExists => (EEXIST, "File already exists"),
        ErrorKind::InvalidInput => (EINVAL, "Invalid input"),
        ErrorKind::OutOfMemory => (ENOMEM, "Out of memory"),
        ErrorKind::AddrInUse => (EADDRINUSE, "Address in use"),
        ErrorKind::AddrNotAvailable => (EADDRNOTAVAIL, "Address not available"),
        ErrorKind::BrokenPipe => (EPIPE, "Broken pipe"),
        ErrorKind::ConnectionAborted => (ECONNRESET, "Connection aborted"),
        ErrorKind::ConnectionRefused => (ECONNREFUSED, "Connection refused"),
        ErrorKind::ConnectionReset => (ECONNRESET, "Connection reset"),
        ErrorKind::Interrupted => (EINTR, "Interrupted"),
        ErrorKind::InvalidData => (EINVAL, "Invalid data"),
        ErrorKind::TimedOut => (ETIMEDOUT, "Operation timed out"),
        ErrorKind::UnexpectedEof => (EIO, "Unexpected end of file"),
        ErrorKind::WriteZero => (EIO, "Write zero"),
        ErrorKind::WouldBlock => (EAGAIN, "Operation would block"),
        ErrorKind::Unsupported => (ENOSYS, "Operation not supported"),
        ErrorKind::Other => (EIO, "An unknown error occurred"),
        _ => (EIO, "An unknown error occurred"),
    };

    errno::set_errno(errno::Errno(err_code));
    if let Some(ctx) = context {
        eprintln!("XetFS Error: {err_msg}. {ctx}");
    } else {
        eprintln!("XetFS Error: {err_msg}");
    }
    err
}

pub fn register_io_error(err: std::io::Error) -> std::io::Error {
    register_io_error_impl(err, None)
}

#[allow(dead_code)]
pub fn register_io_error_with_context(err: std::io::Error, context: &str) -> std::io::Error {
    register_io_error_impl(err, Some(context))
}

pub fn open_flags_from_mode_string(mode: &str) -> Option<c_int> {
    use libc::{O_APPEND, O_CREAT, O_RDONLY, O_RDWR, O_TRUNC, O_WRONLY};

    match mode {
        "r" => Some(O_RDONLY),
        "r+" => Some(O_RDWR),
        "w" => Some(O_WRONLY | O_CREAT | O_TRUNC),
        "w+" => Some(O_RDWR | O_CREAT | O_TRUNC),
        "a" => Some(O_WRONLY | O_CREAT | O_APPEND),
        "a+" => Some(O_RDWR | O_CREAT | O_APPEND),
        _ => {
            eprintln!("XETLDFS Error: invalid mode string {mode}.");
            None
        }
    }
}

pub fn file_needs_materialization(open_flags: c_int) -> bool {
    let on = |flag| open_flags & flag != 0;

    (on(O_RDWR) && !on(O_TRUNC)) || on(O_APPEND)
}

pub fn open_options_from_mode_string(mode: &str) -> Option<std::fs::OpenOptions> {
    let mut open_options = std::fs::OpenOptions::new();
    match mode {
        "r" => {
            open_options.read(true);
        }
        "r+" => {
            open_options.read(true).write(true);
        }
        "w" => {
            open_options.write(true).truncate(true).create(true);
        }
        "w+" => {
            open_options
                .read(true)
                .write(true)
                .truncate(true)
                .create(true);
        }
        "a" => {
            open_options.write(true).append(true).create(true);
        }
        "a+" => {
            open_options
                .read(true)
                .write(true)
                .append(true)
                .create(true);
        }
        _ => {
            return None;
        } // If mode is not recognized, return None
    }
    Some(open_options)
}

pub fn open_options_from_flags(flags: c_int) -> std::fs::OpenOptions {
    use libc::*;

    let mut open_options = std::fs::OpenOptions::new();
    if flags & O_RDONLY != 0 {
        open_options.read(true);
    }
    if flags & O_WRONLY != 0 {
        open_options.write(true);
    }
    if flags & O_RDWR != 0 {
        open_options.read(true).write(true);
    }
    if flags & libc::O_CREAT != 0 {
        open_options.create(true);
    }
    if flags & libc::O_TRUNC != 0 {
        open_options.truncate(true);
    }
    if flags & libc::O_APPEND != 0 {
        open_options.append(true);
    }
    open_options
}
