#[cfg(test)]
mod cas_plumb_cli_tests {
    use assert_cmd::prelude::*;
    use predicates::prelude::*;
    use std::path::Path;
    use std::process;

    fn get_git_xet_path() -> String {
        let path = Path::new(env!("CARGO_BIN_EXE_git-xet"));

        path.parent()
            .unwrap()
            .to_str()
            .map(|s| s.to_string())
            .unwrap()
    }

    fn add_git_xet_to_path() -> String {
        let path = env!("PATH");

        format!("{}:{}", path, get_git_xet_path())
    }

    #[test]
    fn test_git_call() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = process::Command::new("git");

        cmd.env("PATH", add_git_xet_to_path());
        cmd.arg("xet");
        cmd.assert()
            .stderr(predicate::str::contains("git-xet command line"));

        Ok(())
    }
}
