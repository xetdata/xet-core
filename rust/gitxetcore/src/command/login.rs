use crate::config::{get_global_config, XetConfig};
use crate::errors;
use crate::user::{XeteaAuth, XeteaLoginProbe};
use anyhow::anyhow;
use clap::Args;
use std::collections::HashMap;
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
    }
    Ok(())
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
    } else {
        // this goes into a sub-profile
        //
        // create the profile hashmap if there is not one already
        if cfg.profiles.is_none() {
            cfg.profiles = Some(HashMap::new());
        }
        let prof = cfg.profiles.as_mut().unwrap();

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
            prof.insert(name, newcfg);
        }
    }
    cfg.to_file(&global_config)
        .map_err(|e| errors::GitXetRepoError::ConfigError(e.into()))?;

    Ok(())
}
