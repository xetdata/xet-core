#[cfg(test)]
mod cas_plumb_cli_tests {
    use assert_cmd::prelude::*;
    use predicates::prelude::*;
    use std::process;
    const CAS_ENDPOINT: &str = "cas-lb.xetsvc.com:443";

    #[test]
    fn test_cas_status() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = process::Command::cargo_bin("git-xet")?;

        cmd.arg("cas").arg("status");
        cmd.env("XET_CAS_SERVER", CAS_ENDPOINT);
        cmd.assert()
            .success()
            .stdout(predicate::str::is_match("CAS remote: cas-lb\\.xetsvc\\.com:443\n").unwrap())
            .stdout(predicate::str::is_match("CAS prefix: default\n").unwrap())
            .stdout(
                predicate::str::is_match(
                    "Cache Size: \\d*\\.?\\d* (bytes|KiB|MiB|GiB|TiB) / \\d*\\.?\\d* (bytes|KiB|MiB|GiB|TiB)",
                )
                .unwrap(),
            )
            .stdout(
                predicate::str::is_match("Staging Size: \\d*\\.?\\d* (bytes|KiB|MiB|GiB|TiB)").unwrap(),
            );

        let mut cmd = process::Command::cargo_bin("git-xet")?;
        cmd.env("XET_CAS_SERVER", "cas-lb.foo.bar:1234")
            .arg("cas")
            .arg("status");
        cmd.assert()
            .success()
            .stdout(predicate::str::contains("CAS remote: cas-lb.foo.bar:1234"));

        Ok(())
    }

    // NOTE: this may fail based on whether the XORB is present or not.
    #[cfg_attr(not(feature = "xorb_integration_tests"), ignore)]
    #[test]
    fn test_cas_probe() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = process::Command::cargo_bin("git-xet")?;

        // NOTE: this may fail if the Xorb has been deleted from S3.
        cmd.env("XET_CAS_SERVER", CAS_ENDPOINT);
        cmd.env("XET_CAS_PREFIX", "integration_test");
        cmd.arg("cas")
            .arg("probe")
            .arg("6c2bad94393ac1ae4af10ce85e137d07646421ecdcb3ac1ee4c8edeb0de9d4de");

        cmd.assert()
            .success()
            .stdout(predicate::str::contains("Staging: \n"))
            .stdout(predicate::str::contains("Remote:\n"))
            .stdout(
                predicate::str::is_match("File Size: \\d*\\.?\\d* (bytes|KiB|MiB|GiB|TiB)")
                    .unwrap(),
            )
            .stdout(predicate::str::is_match("\t√ Available*").unwrap());

        let mut cmd = process::Command::cargo_bin("git-xet")?;

        cmd.env("XET_CAS_SERVER", CAS_ENDPOINT);
        cmd.env("XET_CAS_PREFIX", "integration_test");
        cmd.arg("cas")
            .arg("probe")
            .arg("6c2bad94393ac1ae4af10ce85e137d07646421ecdcb3ac1ee4c8edeb0de12345");

        cmd.assert()
            .success()
            .stdout(predicate::str::is_match("\t◯ Not available").unwrap())
            .stdout(predicate::str::contains("Staging: \n"))
            .stdout(predicate::str::contains("Remote: \n"));

        Ok(())
    }

    #[test]
    fn test_cas_stage_list() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = process::Command::cargo_bin("git-xet")?;

        cmd.arg("cas").arg("stage-list");
        cmd.env("XET_CAS_SERVER", CAS_ENDPOINT);
        cmd.assert().success().stdout(
            predicate::str::is_match("(Items contained in staging:|Staging is empty.)").unwrap(),
        );

        Ok(())
    }

    #[test]
    fn test_cas_stage_push() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = process::Command::cargo_bin("git-xet")?;

        cmd.arg("cas").arg("stage-push").arg("--retain").arg("true");
        cmd.env("XET_CAS_SERVER", CAS_ENDPOINT);
        cmd.env("XET_CAS_PREFIX", "integration_test");
        cmd.assert()
            .success()
            .stdout(predicate::str::contains("√ Successfully uploaded objects."));

        Ok(())
    }

    // NOTE: this may fail based on whether the XORB is present or not.
    #[cfg_attr(not(feature = "xorb_integration_tests"), ignore)]
    #[test]
    fn test_cas_get() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = process::Command::cargo_bin("git-xet")?;

        cmd.env("XET_CAS_SERVER", CAS_ENDPOINT);
        cmd.env("XET_CAS_PREFIX", "integration_test");
        cmd.arg("cas")
            .arg("get")
            .arg("6c2bad94393ac1ae4af10ce85e137d07646421ecdcb3ac1ee4c8edeb0de9d4de");
        cmd.assert()
            .success()
            .stdout(predicate::str::contains("9b07080a01c4ebb44af848d30e1e5329"));

        Ok(())
    }

    #[test]
    fn test_config() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = process::Command::cargo_bin("git-xet")?;

        cmd.arg("config");
        cmd.assert()
            .success()
            .stdout(predicate::str::starts_with("config"));
        Ok(())
    }
}
