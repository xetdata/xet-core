use std::path::{Path, PathBuf};

use itertools::Itertools;
use xet_config::{Cas, Cfg, Log, User};

use crate::command::CliOverrides;
use crate::config::ConfigError;
use crate::config::ConfigError::HomePathUnknown;
use crate::errors::GitXetRepoError;
use crate::git_integration::get_repo_path;

/// Config files ///

/// Within the current HOME directory
const GLOBAL_CONFIG_PATH: &str = ".xetconfig";
/// Within a repo root
const LOCAL_CONFIG_PATH: &str = ".xet/config";

/// check to see if we can get metadata and if the permissions are not readonly.
pub fn can_write(path: &Path) -> bool {
    path.metadata()
        .map_or(false, |m| !m.permissions().readonly())
}

pub fn is_empty(path: &Path) -> bool {
    path.to_str().map(str::is_empty).unwrap_or(false)
}

/// Gets the path to the global config file on the user's machine.
/// If the path for the config cannot be identified (i.e. the HOME
/// env isn't set), then an error will be returned.
///
/// This will not check that the file exists or is read/writable.
pub fn get_global_config() -> Result<PathBuf, ConfigError> {
    let mut path = dirs::home_dir().ok_or(HomePathUnknown)?;
    path.push(GLOBAL_CONFIG_PATH);
    Ok(path)
}

/// Gets the path to the local config file for a particular xet.
/// This will not check that the file exists or is read/writable.
pub fn get_local_config(gitpath: Option<PathBuf>) -> Result<PathBuf, GitXetRepoError> {
    Ok(match get_repo_path(gitpath)? {
        Some(repo) => repo.join(LOCAL_CONFIG_PATH),
        None => PathBuf::default(),
    })
}

/// Overrides from the CLI
pub fn get_override_cfg(overrides: &CliOverrides) -> Cfg {
    let mut log_overrides = None;
    if overrides.verbose > 0 || overrides.log.is_some() {
        let path = overrides.log.as_ref().cloned();
        log_overrides = Some(Log {
            path,
            level: verbosity_to_level(overrides.verbose),
            format: None,
            tracing: None,
            silentsummary: None,
            exceptions: None,
        })
    }
    let mut cas_overrides = None;
    if let Some(cas_endpoint) = overrides.cas.as_ref() {
        cas_overrides = Some(Cas {
            server: Some(cas_endpoint.clone()),
            ..Default::default()
        })
    }

    let mut user_overrides = None;
    if overrides.user_name.is_some()
        || overrides.user_token.is_some()
        || overrides.user_email.is_some()
        || overrides.user_login_id.is_some()
    {
        user_overrides = Some(User {
            name: overrides.user_name.clone(),
            token: overrides.user_token.clone(),
            email: overrides.user_email.clone(),
            login_id: overrides.user_login_id.clone(),
            ..Default::default()
        });
    }
    Cfg {
        log: log_overrides,
        cas: cas_overrides,
        user: user_overrides,
        ..Default::default()
    }
}

fn verbosity_to_level(verbosity: i8) -> Option<String> {
    match verbosity {
        1 => Some("info".to_string()),
        2 => Some("debug".to_string()),
        x if x >= 3 => Some("trace".to_string()),
        _ => None,
    }
}

pub trait OptionHelpers<T: Sized> {
    fn ok_or_result<E, F: FnOnce() -> Result<T, E>>(self, op: F) -> Result<T, E>;

    fn validate<E, F: Fn(&T) -> Result<(), E>>(self, op: F) -> Result<Option<T>, E>;
}

impl<T: Sized> OptionHelpers<T> for Option<T> {
    /// Shorthand for `Option::ok_or(()).or_else(op)`.
    ///
    /// Transforms the [Option<T>] into a [Result<T, E>], mapping [Some(v)] to [Ok(v)]
    /// and [None] to the output of `op()`.
    fn ok_or_result<E, F: FnOnce() -> Result<T, E>>(self, op: F) -> Result<T, E> {
        match self {
            Some(x) => Ok(x),
            None => op(),
        }
    }

    /// Applies a validation check against an [Option<T>] that will run the validation
    /// function only when self is [Some(x)]. If the validation fails, then the error
    /// is returned. [None] is not checked.
    fn validate<E, F: Fn(&T) -> Result<(), E>>(self, op: F) -> Result<Option<T>, E> {
        match self {
            None => Ok(None),
            Some(x) => op(&x).map(|_| Some(x)),
        }
    }
}

/// Returns a sanitized version of the command used to invoke the current process, with absolute paths stripped
/// of the current working directory
pub fn get_sanitized_invocation_command(strip_program_path: bool) -> String {
    let mut args = std::env::args();
    let invocation_prog = args
        .next()
        .map(|prog| {
            if strip_program_path {
                Path::new(&prog)
                    .file_name()
                    .unwrap_or_default()
                    .to_str()
                    .unwrap_or_default()
                    .to_owned()
            } else {
                prog
            }
        })
        .unwrap_or_default();

    let cwd = std::env::current_dir().unwrap_or_default();
    let cwd_str = cwd.to_str().unwrap_or_default();

    let subcommand: String = args
        .map(|mut e| {
            if e.starts_with(cwd_str) {
                if let Ok(e_rel) = PathBuf::from(&e).strip_prefix(&cwd) {
                    e = e_rel.to_str().unwrap_or(&e).to_owned();
                }
            }
            if e.contains(' ') {
                format!("\"{e}\"")
            } else {
                e
            }
        })
        .join(" ");

    format!("{invocation_prog} {subcommand}")
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use tokio_test::assert_err;

    use super::*;

    #[test]
    fn test_is_empty() {
        assert!(is_empty(PathBuf::new().as_path()));
        assert!(!is_empty(Path::new("foo/bar")));
    }

    #[test]
    fn test_verbosity_to_level() {
        fn check_some(i: i8, expected: &str) {
            assert_eq!(expected.to_string(), verbosity_to_level(i).unwrap());
        }
        check_some(1, "info");
        check_some(2, "debug");
        check_some(3, "trace");
        check_some(127, "trace");

        assert!(verbosity_to_level(0).is_none());
        assert!(verbosity_to_level(-128).is_none());
    }

    #[test]
    fn test_cfg_override() {
        let path = PathBuf::from_str("foo/bar.log").unwrap();
        let expected_cas_server = "http://localhost:60000".to_string();
        let overrides = CliOverrides {
            verbose: 2,
            log: Some(path.clone()),
            smudge_query_policy: Default::default(),
            cas: Some(expected_cas_server.clone()),
            merkledb: None,
            merkledb_v2_cache: None,
            merkledb_v2_session: None,
            profile: None,
            user_name: None,
            user_token: None,
            user_email: None,
            disable_version_check: true,
            user_login_id: None,
        };
        let cfg = get_override_cfg(&overrides);
        assert_eq!("debug", cfg.log.as_ref().unwrap().level.as_ref().unwrap());
        assert_eq!(&path, cfg.log.as_ref().unwrap().path.as_ref().unwrap());
        assert_eq!(
            &expected_cas_server,
            cfg.cas.as_ref().unwrap().server.as_ref().unwrap()
        )
    }

    #[test]
    fn test_cfg_override_user() {
        let expected_cas_server = "http://localhost:60000".to_string();
        let overrides = CliOverrides {
            verbose: 2,
            log: None,
            cas: Some(expected_cas_server.clone()),
            smudge_query_policy: Default::default(),
            merkledb: None,
            merkledb_v2_cache: None,
            merkledb_v2_session: None,
            profile: None,
            user_name: Some("hello".to_string()),
            user_token: None,
            user_email: Some("hello@hello.com".to_string()),
            user_login_id: None,
            disable_version_check: true,
        };
        let cfg = get_override_cfg(&overrides);
        assert_eq!(
            &expected_cas_server,
            cfg.cas.as_ref().unwrap().server.as_ref().unwrap()
        );
        assert_eq!("hello", cfg.user.as_ref().unwrap().name.as_ref().unwrap());
        assert_eq!(
            "hello@hello.com",
            cfg.user.as_ref().unwrap().email.as_ref().unwrap()
        );
    }

    #[test]
    fn test_option_ok_or_result() {
        fn panic_fn() -> Result<i32, ()> {
            panic!("Shouldn't get called");
        }
        let y = Some(16).ok_or_result(panic_fn).unwrap();
        assert_eq!(16, y);

        let y = None.ok_or_result(|| Ok::<i32, ()>(32)).unwrap();
        assert_eq!(32, y);
        assert_err!(None.ok_or_result(|| Err::<(), &'static str>("err")));
    }

    #[test]
    fn test_option_validate() {
        fn panic_fn(_: &i32) -> Result<(), ()> {
            panic!("Shouldn't get called");
        }

        let x = Some(16)
            .validate(|x| {
                assert_eq!(16, *x);
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();
        assert_eq!(16, x);

        assert_err!(Some(16).validate(|_| Err("err")));

        assert!(None.validate(panic_fn).unwrap().is_none());
    }
}
