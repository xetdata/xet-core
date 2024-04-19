use crate::config::{get_global_config, XetConfig};
use crate::errors::{GitXetRepoError, Result};
use anyhow::anyhow;
use clap::Args;
use std::path::{Path, PathBuf};
use xet_config::{Cfg, DEFAULT_XET_HOME};

#[cfg(unix)]
const S3_CONFIG_FILE_NAME: &str = "s3env";
#[cfg(windows)]
const S3_CONFIG_FILE_NAME: &str = "s3env.bat";

#[derive(Args, Debug)]
pub struct S3configArgs {
    /// The domain for Xet S3 service
    #[clap(long, default_value = "xethub.com")]
    pub host: String,
}

pub fn s3config_command(config: XetConfig, args: &S3configArgs) -> Result<()> {
    // login always write auth info into the global config.
    let global_config = get_global_config()?;
    let cfg = Cfg::from_file(&global_config).unwrap_or_default();

    let mut config_ret = Err(GitXetRepoError::AuthError(anyhow!("No match profile")));

    if args.host.is_empty() || args.host == "xethub.com" {
        // this goes into the root profile
        config_ret = write_xs3_config(&config, &cfg);
    } else {
        // search in the sub-profiles
        for v in cfg.profiles.values() {
            if let Some(e) = &v.endpoint {
                if e == &args.host {
                    config_ret = write_xs3_config(&config, v);
                    break;
                }
            }
        }
    }

    match config_ret {
        Ok(path) => {
            print_help_message(&path);
            Ok(())
        }
        Err(GitXetRepoError::AuthError(_)) => {
            print_auth_error_message(&args.host);
            Ok(())
        }
        Err(GitXetRepoError::InvalidOperation(_)) => {
            print_service_unavailable_message(&args.host);
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn print_help_message(s3config_path: &Path) {
    #[cfg(unix)]
    let default_path_str = format!("$HOME/{DEFAULT_XET_HOME}/{S3_CONFIG_FILE_NAME}");
    #[cfg(windows)]
    let default_path_str =
        format!("%HOMEDRIVE%%HOMEPATH%\\{DEFAULT_XET_HOME}\\{S3_CONFIG_FILE_NAME}");
    let path_str = s3config_path.to_str().unwrap_or(&default_path_str);
    #[cfg(unix)]
    eprintln!(
        r#"Profile for the XetHub S3 Service is configured now. Great!

1. To get started source the corresponding env file. This is usually done by
running one of the following (note the leading DOT):
. "{}"                            # For sh/bash/zsh/ash/dash/pdksh
source "{}"                       # For fish
        "#,
        path_str, path_str,
    );
    #[cfg(windows)]
    eprintln!(
        r#"1. Profile for the XetHub S3 Service is configured now. Great!

To get started source the corresponding env file. This can be done by running
the following:
"{}"                              # For cmd
        "#,
        path_str,
    );
    eprintln!(
        r#"2. To see the repos you have access to via XetHub S3, run: 
aws s3 ls
"#
    );
}

fn print_auth_error_message(host: &str) {
    eprintln!(
        r#"No authentication information found.

Please go to https://{}/user/settings/pat to create a token and 
run `git xet login -u <user_name> -e <email> -p <PAT>{}` to login. 

After that, re-run this command."#,
        host,
        if host == "xethub.com" {
            "".to_owned()
        } else {
            format!(" --host {host}")
        }
    );
}

fn print_service_unavailable_message(host: &str) {
    eprintln!(
        r#"A XetHub S3 service is not available for this XetHub deployment.

If you believe this is a mistake, try re-run `git xet login` with your credentials
to pick up configuration changes. If you lost your personal access token, you can 
go to https://{}/user/settings/pat to create a new one.

After that, re-run this command.

If this still happens, contact XetHub support or your administrator."#,
        host
    );
}

fn write_xs3_config(xet_config: &XetConfig, cfg: &Cfg) -> Result<PathBuf> {
    let no_login_info_err = || GitXetRepoError::AuthError(anyhow!("Not authenticated"));
    let service_unavailable_err =
        || GitXetRepoError::InvalidOperation("XS3 service not available".to_owned());

    let user = cfg.user.as_ref().ok_or_else(no_login_info_err)?;
    let aws_access_key = user.aws_access_key.as_ref().ok_or_else(no_login_info_err)?;
    let aws_secret_key = user.aws_secret_key.as_ref().ok_or_else(no_login_info_err)?;
    let xs3 = cfg.xs3.as_ref().ok_or_else(service_unavailable_err)?;
    let xs3_server = xs3.server.as_ref().ok_or_else(service_unavailable_err)?;

    write_xs3_config_script_file(xet_config, (aws_access_key, aws_secret_key), xs3_server)
}

fn write_xs3_config_script_file(
    xet_config: &XetConfig,
    (aws_access_key, aws_secret_key): (&str, &str),
    host: &str,
) -> Result<PathBuf> {
    #[cfg(unix)]
    let script = format!(
        r#"export AWS_ACCESS_KEY_ID={}
export AWS_SECRET_ACCESS_KEY={}
export AWS_ENDPOINT_URL={}
        "#,
        aws_access_key, aws_secret_key, host
    );
    #[cfg(windows)]
    let script = format!(
        r#"set AWS_ACCESS_KEY_ID={}
set AWS_SECRET_ACCESS_KEY={}
set AWS_ENDPOINT_URL={}
        "#,
        aws_access_key, aws_secret_key, host
    );

    let config_file = xet_config.xet_home.join(S3_CONFIG_FILE_NAME);

    xet_config.permission.create_file(&config_file)?;

    std::fs::write(&config_file, script)?;

    Ok(config_file)
}
