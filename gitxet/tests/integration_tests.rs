use anyhow::anyhow;
use itertools::Itertools;
use std::{io::Write, path::Path, process::Command};
use tempfile::TempDir;
use tracing::info;

/// Set this to true to see the output of the tests on success.
const DEBUG: bool = false;

struct IntegrationTest {
    test_script: String,
    arguments: Vec<String>,
    assets: Vec<(String, &'static [u8])>,
}

impl IntegrationTest {
    fn new(test_script: &str) -> Self {
        Self {
            test_script: test_script.to_owned(),
            arguments: Vec::new(),
            assets: Vec::new(),
        }
    }

    fn add_arguments(&mut self, args: &[&str]) -> &mut Self {
        self.arguments.extend(args.iter().map(|s| s.to_string()));
        self
    }

    fn add_asset(&mut self, name: &str, arg: &'static [u8]) -> &mut Self {
        self.assets.push((name.to_owned(), arg));
        self
    }

    fn run(&self) -> anyhow::Result<()> {
        // Create a temporary directory
        let tmp_repo_dest = TempDir::new().unwrap();
        let tmp_path_path = tmp_repo_dest.path().to_path_buf();

        std::fs::write(tmp_path_path.join("test_script.sh"), &self.test_script).unwrap();
        std::fs::write(
            tmp_path_path.join("initialize.sh"),
            include_str!("integration_tests/initialize.sh"),
        )
        .unwrap();

        // Write the assets into the tmp path
        for (name, data) in self.assets.iter() {
            let asset_path = tmp_path_path.join(name);
            std::fs::create_dir_all(asset_path.parent().unwrap())?;
            std::fs::write(&asset_path, data)?;
        }

        let mut cmd = Command::new("bash");
        cmd.current_dir(tmp_path_path.clone());
        cmd.args(["-e", "-x", "test_script.sh"]);
        cmd.args(&self.arguments[..]);

        // Add in the path of the git-xet executable

        let git_xet_path = env!("CARGO_BIN_EXE_git-xet");
        let git_xettest_create_file_path = env!("CARGO_BIN_EXE_xettest_create_file");
        let git_xettest_create_csv_path = env!("CARGO_BIN_EXE_xettest_create_csv");

        let bin_dirs = &[
            &git_xet_path,
            &git_xettest_create_file_path,
            git_xettest_create_csv_path,
        ]
        .map(|src| Path::new(src).parent().unwrap());

        for path in bin_dirs.iter().unique() {
            info!("Adding {:?} to path.", &path);
            cmd.env(
                "PATH",
                format!(
                    "{}:{}",
                    &path.to_str().unwrap(),
                    &std::env::var("PATH").unwrap()
                ),
            );
        }

        // Now, to prevent ~/.gitconfig to be read, we need to reset the home directory; otherwise
        // these tests will not be run in an isolated environment.
        //
        // NOTE: this is not a problem with git version 2.32 or later.  There, GIT_CONFIG_GLOBAL
        // works and the scripts take advantage of it.  However, outside of that, this is needed
        // to avoid issues with a lesser git.
        cmd.env("HOME", tmp_path_path.as_os_str());

        // Set defaults for all of these, but allow them to be overridden
        cmd.env(
            "XET_LOG_LEVEL",
            std::env::var_os("XET_LOG_LEVEL").unwrap_or("error".into()),
        );
        cmd.env(
            "XET_GLOBAL_DEDUP_POLICY",
            std::env::var_os("XET_GLOBAL_DEDUP_POLICY").unwrap_or("always".into()),
        );
        cmd.env(
            "XET_SHARD_QUERY_POLICY",
            std::env::var_os("XET_SHARD_QUERY_POLICY").unwrap_or("LocalFirst".into()),
        );

        cmd.env("XET_AXE_ENABLED", "false");

        // Now, run the script.
        let out = cmd.output()?;
        let status = out.status;

        if status.success() {
            if DEBUG {
                // Just dump things to the output
                eprintln!("Test succeeded, STDOUT:");
                std::io::stdout().write_all(&out.stdout).unwrap();
                eprintln!("STDERR:");
                std::io::stderr().write_all(&out.stderr).unwrap();
            }
            Ok(())
        } else {
            eprintln!("Test failed, STDOUT:");
            std::io::stderr().write_all(&out.stderr).unwrap();
            // Parse output for error string:
            let stderr_out = std::str::from_utf8(&out.stderr)?;

            eprintln!("STDERR:\n{}", &stderr_out);

            let error_re = regex::Regex::new("ERROR:>>>>>(.*)<<<<<").unwrap();

            let captures = error_re.captures(stderr_out);

            if let Some(captured_text) = captures {
                Err(anyhow!(
                    "Test failed: {}",
                    captured_text.get(1).unwrap().as_str()
                ))
            } else {
                Err(anyhow!("Test failed: Unknown Error."))
            }
        }
    }
}

#[cfg(test)]
mod git_integration_tests {
    use super::*;

    #[test]
    fn test_basic_push_pull() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_basic_push_pull.sh")).run()
    }

    #[test]
    fn test_basic_clone_with_checkout() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!(
            "integration_tests/test_basic_clone_with_checkout.sh"
        ))
        .run()
    }

    #[test]
    fn test_global_config_independent_install() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!(
            "integration_tests/test_global_config_independent_install.sh"
        ))
        .run()
    }

    #[test]
    fn test_global_config() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_global_config.sh")).run()
    }

    #[test]
    fn test_branch_operations() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_branch_operations.sh")).run()
    }

    #[test]
    fn test_merging() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_merging.sh")).run()
    }

    #[test]
    fn test_no_smudge_env() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_no_smudge_env.sh")).run()
    }

    #[test]
    fn test_basic_forking() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_basic_forking.sh")).run()
    }

    #[test]
    #[cfg(unix)] // the testing trick doesn't work on Windows
    fn test_git_version_check() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_git_version_check.sh")).run()
    }

    #[test]
    fn test_xet_version_check() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_xet_version_check.sh")).run()
    }

    #[test]
    fn test_github_integration_simple() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!(
            "integration_tests/test_github_integration_simple.sh"
        ))
        .run()
    }

    #[test]
    fn test_github_integration_forking() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!(
            "integration_tests/test_github_integration_forking.sh"
        ))
        .run()
    }

    #[test]
    fn test_github_integration_prs() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!(
            "integration_tests/test_github_integration_prs.sh"
        ))
        .run()
    }

    #[test]
    fn test_xet_remote() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_xet_remote.sh")).run()
    }

    #[test]
    fn test_xet_folder_passthrough() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!(
            "integration_tests/test_xet_folder_passthrough.sh"
        ))
        .run()
    }

    #[test]
    fn test_stored_notes() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_stored_notes.sh"))
            .add_asset(
                "files/data_v1.csv.gz",
                include_bytes!("integration_tests/files/data_v1.csv.gz"),
            )
            .add_asset(
                "files/data_v2.csv.gz",
                include_bytes!("integration_tests/files/data_v2.csv.gz"),
            )
            .run()
    }

    #[test]
    fn test_lfs_locking_install() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!(
            "integration_tests/test_lfs_locking_install.sh"
        ))
        .run()
    }

    #[test]
    #[cfg_attr(not(feature = "expensive_tests"), ignore)]
    fn test_lfs_import() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_lfs_import.sh")).run()
    }

    #[test]
    fn test_gitattributes_writing() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!(
            "integration_tests/test_gitattributes_writing.sh"
        ))
        .run()
    }

    #[test]
    fn test_packet_like_file() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_packet_like_file.sh")).run()
    }

    #[test]
    fn test_uninit() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_uninit.sh")).run()
    }

    #[test]
    fn test_uninstall() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_uninstall.sh")).run()
    }

    #[test]
    fn test_dir_summaries() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_dir_summaries.sh")).run()
    }

    #[test]
    fn test_local_cas_env() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_local_cas_env.sh")).run()
    }

    #[test]
    fn test_git_xet_clone() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_git_xet_clone.sh")).run()
    }

    #[test]
    fn test_repo_salt() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_repo_salt.sh")).run()
    }

    #[test]
    fn test_git_xet_init() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_git_xet_init.sh")).run()
    }

    #[test]
    fn test_merkledb_upgrade() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_merkledb_upgrade.sh")).run()
    }

    #[test]
    fn test_xet_lazy() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_xet_lazy.sh")).run()
    }
}

#[cfg(test)]
mod git_upgrade_consistency_tests {

    use super::*;
    macro_rules! _dir {
        () => {
            "upgrade_consistency_tests/"
        };
    }

    macro_rules! test_archive {
        ($archive:expr) => {{
            let mut upgrade_test =
                IntegrationTest::new(include_str!(concat!(_dir!(), "run_test.sh")));

            upgrade_test.add_arguments(&[concat!($archive, ".tar.bz2")]);

            upgrade_test.add_asset(
                concat!($archive, ".tar.bz2"),
                include_bytes!(concat!(_dir!(), "repos/", $archive, ".tar.bz2"),),
            );
            upgrade_test.add_asset(
                "repo_upgrade_validation_test.sh",
                include_bytes!(concat!(_dir!(), "repo_upgrade_validation_test.sh"),),
            );
            upgrade_test.add_asset(
                "setup_run_environment.sh",
                include_bytes!(concat!(_dir!(), "setup_run_environment.sh"),),
            );

            upgrade_test.run()
        }};
    }

    #[test]
    fn test_v0_8_1() -> anyhow::Result<()> {
        test_archive!("v0_8_1")
    }
}
