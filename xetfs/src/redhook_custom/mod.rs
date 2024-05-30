extern crate libc;

#[cfg(target_env = "gnu")]
pub mod ld_preload;

#[cfg(any(target_os = "macos"))]
pub mod dyld_insert_libraries;
