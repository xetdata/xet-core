use colored::Colorize;
#[cfg(unix)]
use libc;
use std::path::Path;
#[cfg(windows)]
use std::ptr;
use tracing::warn;
#[cfg(windows)]
use winapi::{
    shared::winerror::ERROR_SUCCESS,
    um::{
        processthreadsapi::GetCurrentProcess,
        processthreadsapi::OpenProcessToken,
        securitybaseapi::GetTokenInformation,
        winnt::{TokenElevation, HANDLE, TOKEN_ELEVATION, TOKEN_QUERY},
    },
};

#[derive(Debug, Clone)]
pub enum Permission {
    Regular,
    Elevated,
}

impl Permission {
    pub fn current() -> Permission {
        match is_elevated() {
            false => Permission::Regular,
            true => Permission::Elevated,
        }
    }

    pub fn is_elevated(&self) -> bool {
        match self {
            Permission::Regular => false,
            Permission::Elevated => true,
        }
    }

    pub fn check_path(&self, path: &Path) {
        if self.is_elevated() && !path.exists() {
            let message = format!("Warning: A xet command is running with elevated privileges. A xet metadata directory will be 
created at {path:?} with elevated privileges. Future xet commands running with standard 
privileges may not be able to access this folder, causing them to fail. If this is not desired, 
please change the directory permissions accordingly.");

            eprintln!("{}", message.bright_blue());
            warn!("Xet directory {path:?} created with elevated privileges");
        }
    }
}

/// Checks if the program is run under elevated privilege

fn is_elevated() -> bool {
    // In a Unix-like environment, when a program is run with sudo,
    // the effective user ID (euid) of the process is set to 0.
    #[cfg(unix)]
    return unsafe { libc::geteuid() == 0 };

    #[cfg(windows)]
    {
        let mut token: HANDLE = ptr::null_mut();
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return false;
        }

        let mut elevation: TOKEN_ELEVATION = unsafe { std::mem::zeroed() };
        let mut return_length = 0;
        let success = unsafe {
            GetTokenInformation(
                token,
                TokenElevation,
                &mut elevation as *mut _ as *mut _,
                std::mem::size_of::<TOKEN_ELEVATION>() as u32,
                &mut return_length,
            )
        };

        if success == 0 {
            false
        } else {
            elevation.TokenIsElevated != 0
        }
    }
}

#[test]
fn main() {
    if is_elevated() {
        println!("The program is running with elevated privileges.");
    } else {
        println!("The program is not running with elevated privileges.");
    }
}
