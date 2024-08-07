use std::ffi::{CStr, CString};
use std::io::ErrorKind;

#[allow(dead_code)]
pub const C_EMPTY_STR: *const libc::c_char = c"".as_ptr() as *const libc::c_char;

pub unsafe fn c_to_str<'a>(c_str: *const libc::c_char) -> &'a str {
    if c_str.is_null() {
        return "";
    }

    let c_str = CStr::from_ptr(c_str);
    std::str::from_utf8_unchecked(c_str.to_bytes())
}

pub fn cstring_to_str(s: &CString) -> &str {
    unsafe { std::str::from_utf8_unchecked(s.as_bytes()) }
}

pub fn c_chars_to_cstring(mut vec: Vec<libc::c_char>) -> CString {
    unsafe {
        // A null terminator will be appended at the conversion below
        while let Some(&0) = vec.last() {
            vec.pop();
        }

        // Reinterpret the Vec<c_char> as Vec<u8> without copying
        let ptr = vec.as_mut_ptr() as *mut u8;
        let len = vec.len();
        let cap = vec.capacity();

        // Prevent the original vector from being dropped
        std::mem::forget(vec);

        // Create the CString from the raw parts of the Vec<u8>
        let u8_vec = Vec::from_raw_parts(ptr, len, cap);
        CString::from_vec_unchecked(u8_vec)
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

#[allow(dead_code)]
pub fn register_io_error(err: std::io::Error) -> std::io::Error {
    register_io_error_impl(err, None)
}

#[allow(dead_code)]
pub fn register_io_error_with_context(err: std::io::Error, context: &str) -> std::io::Error {
    register_io_error_impl(err, Some(context))
}

pub fn open_flags_from_mode_string(mode: &str) -> Option<libc::c_int> {
    use libc::{O_APPEND, O_CREAT, O_RDONLY, O_RDWR, O_TRUNC, O_WRONLY};

    // File access mode flag "b" can optionally be specified to open a file in binary mode.
    // This flag has no effect on POSIX systems but it's still valid.
    let mode = mode.replace('b', "");

    match mode.as_str() {
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
