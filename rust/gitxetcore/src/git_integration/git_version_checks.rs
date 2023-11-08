use crate::constants::MINIMUM_GIT_VERSION;
use crate::errors::GitXetRepoError;
use crate::errors::Result;
use crate::git_integration::git_process_wrapping::run_git_captured;
use lazy_static::lazy_static;
use regex::Regex;
use tracing::error;
use tracing::info;
use version_compare::Version;

// Version checking information.
lazy_static! {
    static ref GIT_NO_VERSION_CHECK: bool = {
        match std::env::var_os("XET_NO_GIT_VERSION_CHECK") {
            Some(v) => v != "0",
            None => false,
        }
    };
}

lazy_static! {
    static ref GIT_VERSION_REGEX: Regex = Regex::new(r"^.*version ([0-9\\.a-zA-Z]+).*$").unwrap();
}

// Tools to retrieve and check the git version.
fn get_git_version() -> Result<String> {
    let raw_version_string = run_git_captured(None, "--version", &[], true, None)?.1;

    let captured_text = match GIT_VERSION_REGEX.captures(&raw_version_string) {
        Some(m) => m,
        None => {
            return Err(GitXetRepoError::Other(format!(
                "Error: cannot parse version string {raw_version_string:?}."
            )));
        }
    };

    let version = captured_text.get(1).unwrap().as_str();

    Ok(version.to_owned())
}

fn verify_git_version(version: &str) -> bool {
    let vv_test = match Version::from(version) {
        Some(v) => v,
        None => {
            error!("Could not parse \"{}\" as a version string.", version);
            return false;
        }
    };

    let vv_min = Version::from(MINIMUM_GIT_VERSION).unwrap();

    if vv_test >= vv_min {
        info!("Current git version {:?} acceptable.", &version);
        true
    } else {
        error!(
            "Git version {:?} does not meet minimum requirements.",
            &version
        );
        false
    }
}

lazy_static! {
    static ref GIT_VERSION_CHECK_PASSED: bool = {
        if *GIT_NO_VERSION_CHECK {
            true
        } else {
            match get_git_version() {
                Err(_) => false,
                Ok(v) => verify_git_version(&v),
            }
        }
    };
}

pub fn perform_git_version_check() -> Result<()> {
    if *GIT_VERSION_CHECK_PASSED {
        Ok(())
    } else {
        // There's a reason it's wrong, but perform the version checking again in case there's an error or other issue.
        let version = get_git_version()?;
        if verify_git_version(&version) {
            Ok(())
        } else {
            Err(GitXetRepoError::Other(format!("Only git version 2.29 or later is compatible with git-xet.  Please upgrade your version of git. (Installed version = {}", &version)))
        }
    }
}
