mod utils;
mod xet_interface;
mod xetio;

#[macro_use]
extern crate redhook;

use crate::{utils::*, xet_interface::get_xet_instance, xetio::*};
use ctor;
use errno::errno;
use libc::{
    c_char, c_int, c_ushort, c_void, ferror, fileno, mode_t, size_t, ssize_t, S_IRGRP, S_IROTH,
    S_IRUSR, S_IWGRP, S_IWOTH, S_IWUSR,
};
use std::{
    ffi::{CStr, CString},
    ptr::{null, null_mut},
};

#[ctor::ctor]
fn on_load() {
    eprintln!("{} loaded successfully.", env!("CARGO_PKG_NAME"));
}

// 0666, copied from sys/stat.h
const DEFFILEMODE: mode_t = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;

hook! {
    unsafe fn fopen(pathname: *const c_char, mode: *const c_char) -> *mut libc::FILE => my_fopen {
        eprintln!("XetLDFS: fopen called");
        // Convert fopen mode to OpenOptions
        let mode_str = CStr::from_ptr(mode).to_str().unwrap();
        let maybe_open_flags = open_flags_from_mode_string(mode_str);

        // if let Some(option_flags) = maybe_open_flags {
        //     if let Some(out) = rust_open(pathname, option_flags, None) {
        //         // Convert the file descriptor to FILE* using fdopen
        //         let file_mode = CString::new(mode_str).unwrap();
        //         return libc::fdopen(out, file_mode.as_ptr());
        //     }
        // }

        // real!(fopen)(pathname, mode)

        let Some(flags) = maybe_open_flags else {
            return null_mut();
        };

        let file = real!(fopen)(pathname, mode);

        let fd = fileno(file);
        register_interposed_fd(fd, pathname, flags);

        file
    }
}

#[cfg(target_os = "linux")]
hook! {
        unsafe fn fopen64(pathname: *const c_char, mode: *const c_char) -> *mut libc::FILE => my_fopen64 {
         eprintln!("XetLDFS: fopen64 called");
        // Convert fopen mode to OpenOptions
        let mode_str = CStr::from_ptr(mode).to_str().unwrap();
        let maybe_open_flags = open_flags_from_mode_string(mode_str);

        if let Some(option_flags) = maybe_open_flags {
            if let Some(out) = rust_open(pathname, option_flags, None) {
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
    unsafe fn open(pathname: *const c_char, flags: c_int, filemode: mode_t) -> c_int => my_open {
    eprintln!("XetLDFS: open called {flags:?} {filemode:?}");

        // Check if the path is for a file that exists
        // let path = CStr::from_ptr(pathname).to_str().unwrap();
        // if std::path::Path::new(path).is_file() {
        //     if let Some(out) = rust_open(pathname, flags, Some(filemode)) {
        //         return out;
        //     }
        // }


        let fd = real!(open)(pathname, flags, filemode);

        register_interposed_fd(fd, pathname, flags as c_int);

        fd
    }
}

#[cfg(target_os = "linux")]
hook! {
    unsafe fn open64(pathname: *const c_char, flags: c_int, filemode: c_int) -> c_int => my_open64 {
    eprintln!("XetLDFS: open64 called");
        // Check if the path is for a file that exists
        let path = CStr::from_ptr(pathname).to_str().unwrap();
        if std::path::Path::new(path).is_file() {
            if let Some(out) = rust_open(pathname, flags, Some(filemode)) {
                return out;
            }
        }

        eprintln!("XetLDFS open64: rust_open completed");

        real!(open64)(pathname, flags, filemode)
    }
}

hook! {
    unsafe fn read(fd: c_int, buf: *mut c_void, nbyte: size_t) -> ssize_t => my_read {
        eprintln!("XetLDFS: read called on {fd}");

        if is_registered(fd) {
            internal_read(fd, buf, nbyte)
        } else {
            real!(read)(fd, buf, nbyte)
        }
    }
}

hook! {
    unsafe fn fread(buf: *mut c_void, size: size_t, count: size_t, stream: *mut libc::FILE) -> size_t => my_fread {
        let fd = fileno(stream);

        eprintln!("XetLDFS: fread called on {fd}");

        if is_registered(fd) {
            internal_fread(buf, size, count, stream)
        } else {
            real!(fread)(buf, size, count, stream)
        }
    }
}
