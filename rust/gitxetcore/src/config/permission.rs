use anyhow::anyhow;
use colored::Colorize;
use rand::{thread_rng, Rng};
use std::path::{Path, PathBuf};
use tracing::info;
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

// Facts:
// Assume there's a standard user A that is not a root user.
// 1. On Unix systems, suppose there is a path 'dir/f' where 'dir' is created by A but 'f'
//    created by 'sudo A', A can read, rename or remove 'dir/f'. This implies that it's enough
//    to check the permission of 'dir' if we don't directly write into 'dir/f'. This is exactly
//    how we interact with the xorb cache: if an eviction is deemed necessary, the replacement
//    data is written to a tempfile first and then renamed to the to-be-evicted entry. So even
//    if a certain cache file was created by 'sudo A', the eviction by 'A' will succeed.
// 2. On Windows, 'Run as administrator' by logged in user A actually sets %HOMEPATH% to administrator's
//    HOME, so by default the xet metadata folders are isolated. If 'run as admin A' explicility configures
//    cache or repo path to another location owned by A, ACLs for the created path inherit from the parent
//    folder, so A still has full control.

#[derive(Debug, Clone, Copy)]
pub enum Permission {
    Regular,
    Elevated,
}

#[derive(Debug, Clone, Copy)]
pub enum FileType {
    File,
    Dir,
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

    /// Check if the current process has R+W permission writing to 'path', if not can suggest an alternate
    /// if 'suggest' is true by searching varies options in the below order:
    ///
    /// 1. paths in 'priority_suggest_list' from first to last.
    /// 2. path_[$USER]
    /// 3. $HOME/[suggest_prefix]_[random_number]
    /// 4. $TMPDIR/[suggest_prefix]_[random_number]
    /// 5. $PWD/[suggest_prefix]_[random_number]
    /// The random numbers are generated from a CSPRNG and are unlikely to collide.
    ///
    /// If the current process is running with elevated privileges, warn if the checked path doesn't exist and will be created.
    ///
    /// Return Ok(path) if the provided path is good to write into or an alternate path is suggested;
    /// Return Err(_) if lacking permision for the provided path and unable to suggest an alternate.
    pub fn check_or_suggest_path(
        &self,
        path: &Path,
        expect_type: FileType,
        suggest: bool,
        priority_suggest_list: Option<&[&Path]>,
        suggest_prefix: Option<&str>,
    ) -> anyhow::Result<PathBuf> {
        if self.is_elevated() {
            if !path.exists() {
                let message = format!("Warning: A xet command is running with elevated privileges. A xet metadata directory will be 
    created at {path:?} with elevated privileges. Future xet commands running with standard 
    privileges may not be able to access this folder, causing them to fail. If this is not desired, 
    please change the directory permissions accordingly.");

                eprintln!("{}", message.bright_blue());
                info!("Xet directory {path:?} created with elevated privileges");
            }

            // root user / administrator can create or access any path
            return Ok(path.to_owned());
        }

        // Now the process is running with standard privileges
        if may_have_write_permission_into(path, expect_type) {
            return Ok(path.to_owned());
        }

        info!("Lack permission for R+W into {path:?}");

        if suggest {
            if let Some(suggest_list) = priority_suggest_list {
                for &path in suggest_list {
                    if may_have_write_permission_into(path, expect_type) {
                        return Ok(path.to_owned());
                    }
                    info!("Lack permission for R+W into {path:?}");
                }
            }

            // Cannot write into path, try append the path by "_[username]"
            let mut path = path.to_owned();
            let pathstr = path.as_mut_os_string();
            pathstr.push(format!("_{}", whoami::username()));

            if may_have_write_permission_into(&path, expect_type) {
                return Ok(path);
            }

            info!("Lack permission for R+W into {path:?}");

            let last_component = format!(
                "{}_{}",
                suggest_prefix.unwrap_or_default(),
                thread_rng().gen::<u32>() // thread_rng is CSPRNG, very unlikely to collide
            );

            // Cannot write into "path_[username]", try a random path "[prefix]_[xxx]" under HOME
            if let Some(home) = dirs::home_dir() {
                let path = home.join(&last_component);

                if may_have_write_permission_into(&path, expect_type) {
                    return Ok(path);
                }

                info!("Lack permission for R+W into {path:?}");
            }

            // Cannot write into home directory, try a random path "[prefix]_[xxx]" under /tmp
            let path = std::env::temp_dir().join(&last_component);

            if may_have_write_permission_into(&path, expect_type) {
                return Ok(path);
            }

            info!("Lack permission for R+W into {path:?}");

            // Cannot write into tmp directory, try a random path "[prefix]_[xxx]" under the cwd
            let path = std::env::current_dir()
                .unwrap_or_default()
                .join(&last_component);

            if may_have_write_permission_into(&path, expect_type) {
                return Ok(path);
            }

            info!("Lack permission for R+W into {path:?}");
        }

        Err(anyhow!("Fail to find a path with correct permission"))
    }
}

/// Checks if the program is running under elevated privilege
fn is_elevated() -> bool {
    // In a Unix-like environment, when a program is run with sudo,
    // the effective user ID (euid) of the process is set to 0.
    #[cfg(unix)]
    {
        unsafe { libc::getegid() == 0 }
    }

    #[cfg(windows)]
    {
        let mut token: HANDLE = std::ptr::null_mut();
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

/// Check if the current process may have R+W permission to a path.
fn may_have_write_permission_into(path: impl AsRef<Path>, expect_type: FileType) -> bool {
    let path = path.as_ref();
    match expect_type {
        FileType::File => {
            if let Some(pparent) = path.parent() {
                if std::fs::create_dir_all(pparent).is_err() {
                    return false;
                }
            }
            let exist = path.exists();
            let f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(path);
            if f.is_err() {
                return false;
            }
            drop(f);

            // exist is only trustable if opening file for R+W succeeded
            if !exist {
                // removal can fail and it's ok
                let _ = std::fs::remove_file(path);
            }

            true
        }
        FileType::Dir => {
            if std::fs::create_dir_all(path).is_err() {
                return false;
            }
            tempfile::tempfile_in(path).is_ok()
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use super::{may_have_write_permission_into, FileType, Permission};
    use crate::config::permission::is_elevated;

    #[test]
    #[ignore = "run manually due to sudo"]
    fn test_read_write_permission() -> anyhow::Result<()> {
        // Run this test manually, steps:

        // For Unix
        // 1. Run the below shell script in an empty dir with standard privileges.
        // 2. Set env var 'XET_TEST_PATH' to this path.
        // 3. Build the test executable by running 'cargo test -p gitxetcore --lib --no-run'.
        // 4. Locate the path to the executable as TEST_EXE
        // 5. Run with test with a non-root user: 'TEST_EXE config::permission::test::test_read_write_permission --exact --nocapture --include-ignored'
        // 6. Run with root user: 'sudo -E TEST_EXE config::permission::test::test_read_write_permission --exact --nocapture --include-ignored'
        r#"
# a regular dir with a regular file, a root file and a root dir
mkdir regdir
touch regdir/regf
sudo touch regdir/rootf
sudo mkdir regdir/rootdir

# a root dir with a regular file, a root file and a regular dir
sudo mkdir rootdir
sudo touch rootdir/regf
sudo chown $USER rootdir/regf
sudo touch rootdir/rootf
sudo mkdir rootdir/regdir
sudo chown $USER rootdir/regdir
"#;
        let test_path = std::env::var("XET_TEST_PATH")?;
        std::env::set_current_dir(test_path)?;

        if is_elevated() {
            // test path that exists
            assert!(may_have_write_permission_into("regdir", FileType::Dir));
            assert!(may_have_write_permission_into(
                "regdir/regf",
                FileType::File
            ));
            assert!(may_have_write_permission_into(
                "regdir/rootf",
                FileType::File
            ));
            assert!(may_have_write_permission_into(
                "regdir/rootdir",
                FileType::Dir
            ));

            assert!(may_have_write_permission_into("rootdir", FileType::Dir));
            assert!(may_have_write_permission_into(
                "rootdir/regf",
                FileType::File
            ));
            assert!(may_have_write_permission_into(
                "rootdir/rootf",
                FileType::File
            ));
            assert!(may_have_write_permission_into(
                "rootdir/regdir",
                FileType::Dir
            ));

            // test path that doesn't exist
            assert!(may_have_write_permission_into("regdir/adir", FileType::Dir));
            assert!(may_have_write_permission_into("regdir/af", FileType::File));
            assert!(may_have_write_permission_into(
                "rootdir/adir",
                FileType::Dir
            ));
            assert!(may_have_write_permission_into("rootdir/af", FileType::File));

            assert!(may_have_write_permission_into("adir/adir", FileType::Dir));

            assert!(may_have_write_permission_into("bdir/bf", FileType::File));

            assert!(may_have_write_permission_into("cdir", FileType::Dir));

            assert!(may_have_write_permission_into("cf", FileType::Dir));
        } else {
            // test path that exists
            assert!(may_have_write_permission_into("regdir", FileType::Dir));
            assert!(may_have_write_permission_into(
                "regdir/regf",
                FileType::File
            ));
            assert!(!may_have_write_permission_into(
                "regdir/rootf",
                FileType::File
            ));
            assert!(!may_have_write_permission_into(
                "regdir/rootdir",
                FileType::Dir
            ));

            assert!(!may_have_write_permission_into("rootdir", FileType::Dir));
            assert!(may_have_write_permission_into(
                "rootdir/regf",
                FileType::File
            ));
            assert!(!may_have_write_permission_into(
                "rootdir/rootf",
                FileType::File
            ));
            assert!(may_have_write_permission_into(
                "rootdir/regdir",
                FileType::Dir
            ));

            // test path that doesn't exist
            assert!(may_have_write_permission_into("regdir/adir", FileType::Dir));
            assert!(may_have_write_permission_into("regdir/af", FileType::File));
            assert!(!may_have_write_permission_into(
                "rootdir/adir",
                FileType::Dir
            ));
            assert!(!may_have_write_permission_into(
                "rootdir/af",
                FileType::File
            ));

            assert!(may_have_write_permission_into("adir/adir", FileType::Dir));

            assert!(may_have_write_permission_into("bdir/bf", FileType::File));

            assert!(may_have_write_permission_into("cdir", FileType::Dir));

            assert!(may_have_write_permission_into("cf", FileType::Dir));
        }

        Ok(())
    }

    #[test]
    #[ignore = "run manually due to sudo"]
    fn test_path_check_or_suggest_dir() -> anyhow::Result<()> {
        // Run this test manually, steps:

        // For Unix
        // 1. Run the below shell script in an empty dir with standard privileges.
        // 2. Set env var 'XET_TEST_PATH' to this path.
        // 3. Build the test executable by running 'cargo test -p gitxetcore --lib --no-run'.
        // 4. Locate the path to the executable as TEST_EXE
        // 5. Run with test with a non-root user: 'TEST_EXE config::permission::test::test_path_check_or_suggest_dir --exact --nocapture --include-ignored'
        r#"
sudo mkdir .xet
"#;
        let test_path = std::env::var("XET_TEST_PATH")?;
        std::env::set_current_dir(test_path)?;

        let permission = Permission::current();

        let xet_home = Path::new(".xet");
        let xet_home =
            permission.check_or_suggest_path(xet_home, FileType::Dir, true, None, Some(".xet"))?;
        assert_eq!(
            &xet_home,
            Path::new(&format!(".xet_{}", whoami::username()))
        );

        let cache_path = Path::new(".xet/cache");
        let cache_path = permission.check_or_suggest_path(
            cache_path,
            FileType::File,
            true,
            Some(&[&xet_home.join("cache")]),
            Some("cache"),
        )?;

        assert_eq!(
            &cache_path,
            Path::new(&format!(".xet_{}/cache", whoami::username()))
        );

        Ok(())
    }

    #[test]
    #[ignore = "run manually due to sudo"]
    fn test_path_check_or_suggest_dir_prefix() -> anyhow::Result<()> {
        // Run this test manually, steps:

        // For Unix
        // 1. Run the below shell script in an empty dir with standard privileges.
        // 2. Set env var 'XET_TEST_PATH' to this path.
        // 3. Build the test executable by running 'cargo test -p gitxetcore --lib --no-run'.
        // 4. Locate the path to the executable as TEST_EXE
        // 5. Run with test with a non-root user: 'TEST_EXE config::permission::test::test_path_check_or_suggest_dir_prefix --exact --nocapture --include-ignored'
        r#"
sudo mkdir .xet
sudo mkdir .xet_$USER
"#;
        let test_path = std::env::var("XET_TEST_PATH")?;
        std::env::set_current_dir(test_path)?;

        let permission = Permission::current();

        let xet_home = Path::new(".xet");
        let xet_home =
            permission.check_or_suggest_path(xet_home, FileType::Dir, true, None, Some(".xet"))?;

        assert!(xet_home.to_str().unwrap_or_default().contains(".xet"));

        Ok(())
    }

    #[test]
    #[ignore = "run manually due to sudo"]
    fn test_path_check_or_suggest_file() -> anyhow::Result<()> {
        // Run this test manually, steps:

        // For Unix
        // 1. Run the below shell script in an empty dir with standard privileges.
        // 2. Set env var 'XET_TEST_PATH' to this path.
        // 3. Build the test executable by running 'cargo test -p gitxetcore --lib --no-run'.
        // 4. Locate the path to the executable as TEST_EXE
        // 5. Run with test with a non-root user: 'TEST_EXE config::permission::test::test_path_check_or_suggest_file --exact --nocapture --include-ignored'
        r#"
mkdir .xet
sudo touch .xet/upgrade_check
"#;
        let test_path = std::env::var("XET_TEST_PATH")?;
        std::env::set_current_dir(test_path)?;

        let permission = Permission::current();

        let ucfile = Path::new(".xet/upgrade_check");
        let ucfile = permission.check_or_suggest_path(ucfile, FileType::Dir, true, None, None)?;

        assert_eq!(
            &ucfile,
            Path::new(&format!(".xet/upgrade_check_{}", whoami::username()))
        );

        Ok(())
    }
}
