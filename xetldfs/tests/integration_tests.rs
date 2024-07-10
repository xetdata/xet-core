use anyhow::anyhow;
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

    #[allow(unused)]
    fn add_arguments(&mut self, args: &[&str]) {
        self.arguments.extend(args.iter().map(|s| s.to_string()))
    }

    #[allow(unused)]
    fn add_asset(&mut self, name: &str, arg: &'static [u8]) {
        self.assets.push((name.to_owned(), arg));
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
            std::fs::write(tmp_path_path.join(name), data)?;
        }

        let mut cmd = Command::new("bash");
        cmd.current_dir(tmp_path_path.clone());
        cmd.args(["-e", "-x", "test_script.sh"]);
        cmd.args(&self.arguments[..]);

        // Add in the path of the git-xet  / xetcat executable

        let git_xet_path = env!("CARGO_BIN_EXE_git-xet");
        let buildpath = Path::new(&git_xet_path).parent().unwrap();
        info!("Adding {:?} to path.", &buildpath);
        cmd.env(
            "PATH",
            format!(
                "{}:{}",
                &buildpath.to_str().unwrap(),
                &std::env::var("PATH").unwrap()
            ),
        );

        // Export the path of the ld_preload lib
        let lib_name = env!("CARGO_PKG_NAME");
        let lib_file = if cfg!(target_os = "linux") {
            format!("lib{}.so", lib_name)
        } else if cfg!(target_os = "macos") {
            format!("lib{}.dylib", lib_name)
        } else {
            panic!("Unsupported target OS");
        };

        cmd.env("LDPRELOAD_LIB", buildpath.join(lib_file).as_os_str());

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

#[cfg(all(test, unix))]
mod git_integration_tests {
    use super::*;

    #[test]
    fn test_read_write() -> anyhow::Result<()> {
        IntegrationTest::new(include_str!("integration_tests/test_read_write.sh")).run()
    }
}
