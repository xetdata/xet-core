use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::time::Duration;
use std::{
    io::Write,
    process::{Command, Stdio},
};
use tracing::info;

#[derive(Default)]
pub struct XeteaAuth {}

fn exec_git_credential(command: &str) -> Result<std::process::Child> {
    Ok(Command::new("git")
        .args(["credential", command])
        .env("GCM_INTERACTIVE", "never")
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?)
}

/// Return type of validate_xetea_auth
#[derive(Clone, Default, Debug, Deserialize)]
pub struct XeteaLoginProbe {
    /// If true, login was successful
    pub ok: bool,
    /// a unique identifier for this login
    pub login_id: Option<String>,
    /// storage endpoint
    pub cas: Option<String>,
    /// s3 service endpoint
    pub xs3: Option<String>,
    /// monitoring endpoint
    pub axe_key: Option<String>,
    /// AWS Access Key
    pub aws_access_key: Option<String>,
    /// AWS Secret Key
    pub aws_secret_key: Option<String>,
}

/// In ms. How long to wait for the login probe in validate_xetea_auth
const XETEA_LOGIN_PROBE_TIMEOUT: u64 = 5000;

#[mockall::automock]
impl XeteaAuth {
    pub fn fetch_ssh_output(&self, user_hostname: &str) -> Result<String> {
        let output = Command::new("ssh").args(["-T", user_hostname]).output()?;
        if !output.status.success() {
            return Err(anyhow!("ssh authentication failure"));
        }
        let output = String::from_utf8(output.stderr.clone()).map_err(|_| {
            anyhow!(
                "Invalid ssh output {}",
                String::from_utf8_lossy(&output.stderr).to_string(),
            )
        })?;
        Ok(output)
    }

    pub fn fetch_https_output(&self, hostname: &str) -> Result<String> {
        let mut child = exec_git_credential("fill")?;
        {
            let child_stdin = child.stdin.as_mut().unwrap();
            let mut cred_input: String = "protocol=https\nhost=".to_owned();
            cred_input.push_str(hostname);
            child_stdin.write_all(cred_input.as_bytes())?;
        }
        let output = child.wait_with_output()?;
        let output = String::from_utf8(output.stdout.clone()).map_err(|_| {
            anyhow!(
                "Invalid git creds output {}",
                String::from_utf8_lossy(&output.stderr).to_string(),
            )
        })?;
        Ok(output)
    }

    /// Adds a credential to the git credential store. Note that
    /// if a matching credential exists, it will not be overwritten.
    pub fn add_credential(
        &self,
        protocol: &str,
        host: &str,
        user: &str,
        password: &str,
    ) -> Result<()> {
        info!("Adding credentials for protocol={protocol}, host={host}, user={user}");
        let mut child = exec_git_credential("approve")?;
        {
            let child_stdin = child.stdin.as_mut().unwrap();
            let cred_input: String = format!(
                "protocol={protocol}\nhost={host}\nusername={user}\npassword={password}\n",
            );
            child_stdin.write_all(cred_input.as_bytes())?;
        }
        let _ = child.wait_with_output()?;
        Ok(())
    }

    /// Returns Ok(true) if a matching credential is found in the git credential store.
    pub fn has_existing_credential(&self, protocol: &str, host: &str, user: &str) -> Result<bool> {
        info!("Testing for existing credential for protocol={protocol}, host={host}, user={user}");
        let mut child = exec_git_credential("fill")?;
        {
            let child_stdin = child.stdin.as_mut().unwrap();
            let cred_input: String =
                format!("protocol={protocol}\nhost={host}\nusername={user}\n",);
            child_stdin.write_all(cred_input.as_bytes())?;
        }
        let res = child.wait_with_output()?;
        let has_password_field = String::from_utf8_lossy(&res.stdout).contains("password=");
        info!("Has existing credential : {has_password_field}");
        Ok(has_password_field)
    }

    /// Drops any credentials in the git credential store matching the constraints
    pub fn drop_credential(&self, protocol: &str, host: &str, user: &str) -> Result<()> {
        info!("Dropping credentials for protocol={protocol}, host={host}, user={user}");
        let mut child = exec_git_credential("reject")?;
        {
            let child_stdin = child.stdin.as_mut().unwrap();
            let cred_input: String =
                format!("protocol={protocol}\nhost={host}\nusername={user}\n",);
            child_stdin.write_all(cred_input.as_bytes())?;
        }
        let _ = child.wait_with_output()?;
        Ok(())
    }

    pub async fn validate_xetea_auth(
        &self,
        protocol: &str,
        host: &str,
        user: &str,
        password: &str,
    ) -> Result<XeteaLoginProbe> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(XETEA_LOGIN_PROBE_TIMEOUT))
            .build()?;
        let resp = client
            .get(format!("{protocol}://{host}/{user}/login_probe"))
            .basic_auth(user, Some(password))
            .send()
            .await?;
        if resp.status() != reqwest::StatusCode::OK {
            return Err(anyhow!(
                "Unable to authenticate. HTTP Status {:?}",
                resp.status()
            ));
        }
        let probe_result = resp.json::<XeteaLoginProbe>().await?;
        Ok(probe_result)
    }
}
