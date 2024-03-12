use crate::config::authentication::{XeteaAuth, XeteaLoginProbe};
use crate::config::{get_global_config, XetConfig};
use crate::errors;
use anyhow::anyhow;
use clap::Args;
use std::process::Command;
use tracing::{error, warn};
use xet_config::{Axe, Cas, Cfg, User};

#[derive(Args, Debug)]
pub struct LoginArgs {
    /// The host to authenticate to
    #[clap(long, default_value = "xethub.com")]
    pub host: String,

    /// The username to authenticate with
    #[clap(long, short)]
    pub user: String,

    /// The email address to authenticate with
    #[clap(long, short)]
    pub email: String,

    /// The password to authenticate with
    #[clap(long, short)]
    pub password: String,

    /// Do not attempt authentication against the remote host
    #[clap(long, short)]
    pub force: bool,

    /// Do not overwrite credentials if they already exist
    #[clap(long)]
    pub no_overwrite: bool,

    /// Configures aws-cli for xethub S3 access
    #[clap(long)]
    pub s3: bool,
}

/// applies config from LoginArgs onto a cfg
fn apply_config(
    cfg: &mut Cfg,
    args: &LoginArgs,
    maybe_auth_check: Option<XeteaLoginProbe>,
) -> errors::Result<()> {
    if cfg.user.is_none() {
        cfg.user = Some(User::default());
    }

    let user = cfg.user.as_mut().unwrap();

    if user.name.is_some() || user.token.is_some() {
        if args.no_overwrite {
            error!("Existing matching credential found. Since --no-overwrite is set, we will not proceed.");
            return Ok(());
        } else {
            warn!("Existing credentials will be overwritten");
        }
    }
    user.https = Some(args.user.clone());
    user.name = Some(args.user.clone());
    user.token = Some(args.password.clone());
    user.email = Some(args.email.clone());

    if let Some(auth_check) = maybe_auth_check {
        if let Some(login_id) = auth_check.login_id {
            if !login_id.is_empty() {
                user.login_id = Some(login_id);
            }
        }
        if let Some(cas) = auth_check.cas {
            if !cas.is_empty() {
                if cfg.cas.is_none() {
                    cfg.cas = Some(Cas::default());
                }
                let cas_config = cfg.cas.as_mut().unwrap();
                cas_config.server = Some(cas);
            }
        }
        if let Some(axe_key) = auth_check.axe_key {
            if !axe_key.is_empty() {
                if cfg.axe.is_none() {
                    cfg.axe = Some(Axe::default())
                }
                let axe_config = cfg.axe.as_mut().unwrap();
                axe_config.axe_code = Some(axe_key);
            }
        }
        if let Some(aws_access_key) = auth_check.aws_access_key {
            if !aws_access_key.is_empty() {
                user.aws_access_key = Some(aws_access_key);
            }
        }
        if let Some(aws_secret_key) = auth_check.aws_secret_key {
            if !aws_secret_key.is_empty() {
                user.aws_secret_key = Some(aws_secret_key);
            }
        }
    }
    Ok(())
}

// Run AWS CLI to write an access key and secret key
// Returns the profilename we wrote to
fn write_aws_config(cfg: &Cfg, host: &str) -> errors::Result<String> {
    if cfg.user.is_none() {
        return Err(errors::GitXetRepoError::Other(
            "No user configuration".to_string(),
        ));
    }
    let user = &cfg.user.as_ref().unwrap();
    let aws_access_key = user
        .aws_access_key
        .as_ref()
        .ok_or(errors::GitXetRepoError::AuthError(anyhow!(
            "Not authenticated. Run git-xet login first"
        )))?;
    let aws_secret_key = user
        .aws_secret_key
        .as_ref()
        .ok_or(errors::GitXetRepoError::AuthError(anyhow!(
            "Not authenticated. Run git-xet login first"
        )))?;
    // dots and dashes are ok
    // this makes the profile name slightly more readable
    let mut profilename = host.to_string();
    profilename.retain(|x| x.is_alphanumeric() || x == '.' || x == '-');
    // strip the .com so xethub.com just becomes xethub
    if profilename.ends_with(".com") {
        profilename = profilename[..profilename.len() - 4].to_string();
    }
    let output = Command::new("aws")
        .args([
            "configure",
            "--profile",
            &profilename,
            "set",
            "aws_access_key_id",
            &aws_access_key,
        ])
        .status()?;
    if !output.success() {
        return Err(errors::GitXetRepoError::Other(
            "Unable to run aws cli".to_string(),
        ));
    }
    let output = Command::new("aws")
        .args([
            "configure",
            "--profile",
            &profilename,
            "set",
            "aws_secret_access_key",
            &aws_secret_key,
        ])
        .status()?;
    if !output.success() {
        return Err(errors::GitXetRepoError::Other(
            "Unable to run aws cli".to_string(),
        ));
    }
    Ok(profilename.to_string())
}

/// Prints the AWS Config stdout.
/// Does not do anything if there is no user config.
fn print_s3_config(cfg: &Cfg) -> errors::Result<()> {
    if let Some(ref user) = cfg.user {
        let aws_access_key =
            &user
                .aws_access_key
                .as_ref()
                .ok_or(errors::GitXetRepoError::AuthError(anyhow!(
                    "Not authenticated. Run git-xet login first"
                )))?;
        let aws_secret_key =
            &user
                .aws_secret_key
                .as_ref()
                .ok_or(errors::GitXetRepoError::AuthError(anyhow!(
                    "Not authenticated. Run git-xet login first"
                )))?;
        println!("\tAWS_ACCESS_KEY_ID = {aws_access_key}");
        println!("\tAWS_SECRET_ACCESS_KEY = {aws_secret_key}");
    }
    Ok(())
}

/// handles the --s3 option and handles all the printing
/// Eats all errors. Does not return errors.
fn handle_s3_login_option(cfg: &Cfg, host: &str, try_write_aws_config: bool) {
    if cfg.user.is_none() {
        eprintln!("No user configuration. Not authenticated");
        return;
    }
    let user = cfg.user.as_ref().unwrap();
    if user.aws_access_key.is_none() || user.aws_secret_key.is_none() {
        eprintln!(
            "AWS configuration not found.\n\
            Xethub service failed to provide credentials.\n\
            Please contact support or your administrator"
        );
        return;
    }

    let mut failed_to_run_awscli: bool = false;
    let maybe_profilename: Option<String>;
    let ok;
    if try_write_aws_config {
        maybe_profilename = write_aws_config(cfg, host).ok();
        ok = maybe_profilename.is_some();
        if !ok {
            failed_to_run_awscli = true;
            // actually failed to run aws.
            eprintln!(
                "Failed to run aws cli.\n\
              Unable to configure S3 automatically."
            );
        }
    } else {
        maybe_profilename = None;
        ok = false;
    }
    if !ok {
        // instructions without an aws profile
        eprintln!("Set the following environment variables to use AWS cli");
        if !failed_to_run_awscli {
            eprintln!("Re-run `git-xet login` with the --s3 option to write these keys to an AWS cli configuration profile")
        }
        eprintln!("");
        let _ = print_s3_config(cfg);
        eprintln!("");
        eprintln!("To see the repos you have access to via S3,");
        eprintln!("set the environment variables above and run: ");
        eprintln!("\taws --endpoint-url=s3.{host} s3 ls");
        eprintln!("");
    } else {
        // instructions with an aws profile
        eprintln!("The following was config written successfully to AWS cli configuration.");
        eprintln!("");
        let _ = print_s3_config(cfg);
        eprintln!("");
        let profilename = maybe_profilename.unwrap();
        eprintln!("To see the repos you have access to via S3, run: ");
        eprintln!("\taws --endpoint-url=s3.{host} --profile {profilename} s3 ls");
        eprintln!("");
    }
}

pub async fn login_command(_: XetConfig, args: &LoginArgs) -> errors::Result<()> {
    let protocol = if args.host.contains("localhost") {
        // this is for testing sanity.
        // One does not usually get https on localhost.
        "http"
    } else {
        "https"
    };

    let auth = XeteaAuth::default();

    let mut maybe_auth_check: Option<XeteaLoginProbe> = None;
    if !args.force {
        // attempt to authenticate against the host.
        let authcheck = auth
            .validate_xetea_auth(protocol, &args.host, &args.user, &args.password)
            .await
            .map_err(|_| {
                // we collapse all the communication errors
                // into a single message.
                errors::GitXetRepoError::AuthError(anyhow!(
                    "Failed to authenticate against {}. Use --force to override.",
                    args.host
                ))
            })?;
        if !authcheck.ok {
            return Err(errors::GitXetRepoError::AuthError(anyhow!(
                "Unable to authenticate. Wrong username/password."
            )));
        }
        maybe_auth_check = Some(authcheck);
    }

    let global_config = get_global_config()?;
    let mut cfg = Cfg::from_file(&global_config).unwrap_or_default();
    if args.host.is_empty() || args.host == "xethub.com" {
        // this goes into the root profile
        apply_config(&mut cfg, args, maybe_auth_check)?;
        handle_s3_login_option(&cfg, &args.host, args.s3);
    } else {
        // this goes into a sub-profile
        //
        // create the profile hashmap if there is not one already
        let prof = &mut cfg.profiles;

        // search the list of profiles for a key that matches the host
        let mut config_applied = false;
        for (_, v) in prof.iter_mut() {
            if let Some(e) = v.endpoint.clone() {
                if e == args.host {
                    apply_config(v, args, maybe_auth_check.clone())?;
                    config_applied = true;
                    break;
                }
            }
        }

        // we need to create a new profile
        if !config_applied {
            // new config has the endpoint
            let mut newcfg = xet_config::Cfg {
                endpoint: Some(args.host.clone()),
                ..Default::default()
            };
            apply_config(&mut newcfg, args, maybe_auth_check.clone())?;

            // make a version of host with no special characters and only alphanumeric
            let mut root_name = args.host.clone();
            root_name.retain(|x| x.is_alphanumeric());
            let mut name = root_name.clone();
            let mut ctr = 0;
            while prof.contains_key(&name) {
                name = format!("{root_name}{ctr}").to_string();
                ctr += 1;
            }
            handle_s3_login_option(&newcfg, &args.host, args.s3);
            prof.insert(name, newcfg);
        }
    }
    cfg.to_file(&global_config)
        .map_err(|e| errors::GitXetRepoError::ConfigError(e.into()))?;
    eprintln!("Login successful");

    Ok(())
}
