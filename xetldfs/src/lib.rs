mod utils;
mod xet_interface;

#[macro_use]
extern crate redhook;

use crate::{utils::*, xet_interface::get_xet_instance};
use ctor;
use libc::{c_char, c_int};
use std::ffi::{CStr, CString};
use std::fs;

#[ctor::ctor]
fn on_load() {
    eprintln!("{} loaded successfully.", env!("CARGO_PKG_NAME"));
}

fn rust_open(pathname: *const c_char, _mode: fs::OpenOptions) -> Option<c_int> {
    eprintln!("XetLDFS: rust_open called");
    unsafe {
        let Ok(path) = CStr::from_ptr(pathname).to_str() else {
            register_io_error(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid pathname (UTF8 Error)",
            ));
            return Some(-1);
        };

        // See if there's a xet wrapper with which to convert this all.
        let Ok(maybe_xet_wrapper) = get_xet_instance(path).map_err(|e| {
            eprintln!("ERROR: Error opening Xet Instance from {path:?}: {e:?}");
            e
        }) else {
            // Fall back to raw
            return None;
        };

        {
            use std::io::Write;
            if path.ends_with("_TEST_FILE") {
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(path)
                    .map_err(|e| {
                        register_io_error_with_context(e, &format!("Opening test file {path:?}"))
                    })
                    .and_then(|mut f| f.write(b" :SUCCESSFUL:"))
                    .unwrap();
            }
        }

        let Some(_xet_wrapper) = maybe_xet_wrapper else {
            return None;
        };

        // TODO: intercept it
        None

        // return xet_wrapper.get_fd(path, mode);
    }
}

hook! {
    unsafe fn fopen(pathname: *const c_char, mode: *const c_char) -> *mut libc::FILE => my_fopen {
    eprintln!("XetLDFS: fopen called");
        // Convert fopen mode to OpenOptions
        let mode_str = CStr::from_ptr(mode).to_str().unwrap();
        let open_options = open_options_from_mode_string(mode_str);

        if let Some(open_options) = open_options {
            if let Some(out) = rust_open(pathname, open_options) {
                // Convert the file descriptor to FILE* using fdopen
                let file_mode = CString::new(mode_str).unwrap();
                return libc::fdopen(out, file_mode.as_ptr());
            }
        }

        real!(fopen)(pathname, mode)
    }
}

hook! {
        unsafe fn fopen64(pathname: *const c_char, mode: *const c_char) -> *mut libc::FILE => my_fopen64 {
    eprintln!("XetLDFS: fopen64 called");
        // Convert fopen mode to OpenOptions
        let mode_str = CStr::from_ptr(mode).to_str().unwrap();
        let open_options = open_options_from_mode_string(mode_str);

        if let Some(open_options) = open_options {
            if let Some(out) = rust_open(pathname, open_options) {
                // Convert the file descriptor to FILE* using fdopen
                let file_mode = CString::new(mode_str).unwrap();
                return libc::fdopen(out, file_mode.as_ptr());
            }
        }

        real!(fopen64)(pathname, mode)
    }
}

// Hook for open
hook! {
    unsafe fn open(pathname: *const c_char, flags: c_int, mode: c_int) -> c_int => my_open {
    eprintln!("XetLDFS: open called");
        // Convert open flags to OpenOptions
        let open_options = open_options_from_flags(flags);

        // Check if the path is for a file that exists
        let path = CStr::from_ptr(pathname).to_str().unwrap();
        if std::path::Path::new(path).is_file() {
            if let Some(out) = rust_open(pathname, open_options) {
                return out;
            }
        }

        real!(open)(pathname, flags, mode)
    }
}
hook! {
    unsafe fn open64(pathname: *const c_char, flags: c_int, mode: c_int) -> c_int => my_open64 {
    eprintln!("XetLDFS: open64 called");
        // Convert open flags to OpenOptions
        let open_options = open_options_from_flags(flags);

        // Check if the path is for a file that exists
        let path = CStr::from_ptr(pathname).to_str().unwrap();
        if std::path::Path::new(path).is_file() {
            if let Some(out) = rust_open(pathname, open_options) {
                return out;
            }
        }

        real!(open64)(pathname, flags, mode)
    }
}
