use crate::config;
use crate::config::ConfigError;
use crate::config::ConfigError::{InvalidGitRemote, InvalidUserCommandOutput};
use cas::constants::*;
use itertools::Itertools;
use mockall_double::double;
use url::Url;
use xet_config::{Level, User};

/// IF ajit@ anonymously accesses
/// https://xethub.com/alice/VisualBehaviorNeuralpixels
/// THEN
///  - xet_owner will be Some("alice")
///  - xet_accessor will be None
///
/// IF ajit@ logs in and accesses
/// https://xethub.com/alice/VisualBehaviorNeuralpixels
/// AND
/// types his username and password everytime
/// THEN
///  - xet_owner will be Some("alice")
///  - xet_accessor will be None
///
/// IF ajit@ logs in and accesses
/// https://xethub.com/alice/VisualBehaviorNeuralpixels
/// AND
/// has a git credential helper set up to avoid typing username/passwords
/// THEN
///  - xet_owner will be Some("alice")
///  - xet_accessor will be Some("bob")
///
/// IF ajit@ logs in and accesses
/// xet@xethub.com:alice/VisualBehaviorNeuralpixels.git
/// THEN
///  - xet_owner will be Some("alice")
///  - xet_accessor will be Some("bob")
///
///  email is available only if it is in the user.email section of xetconfig.
///
///  https/ssh field are the currently known *derived* usernames currently used
///  for authentication in the current repo. Note that this can be different
///  from the configured name/token pair if the repository access was
///  authenticated via other means. (For instance via gitcredential or via
///  explicit username in the path, or via ssh key).
///
///  name and token are available only if it is in the user.name and
///  user.token sections of xetconfig.
#[derive(Debug, Clone, Default)]
pub struct UserSettings {
    pub ssh: Option<String>,
    pub https: Option<String>,
    pub owner: Option<String>,
    pub email: Option<String>,
    pub login_id: Option<String>,
    pub name: Option<String>,
    pub token: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum UserIdType {
    SSH,
    HTTPS,
    UNKNOWN,
}

impl UserSettings {
    pub fn get_user_id(&self) -> (String, UserIdType) {
        if let Some(user_ssh) = &self.ssh {
            return (user_ssh.clone(), UserIdType::SSH);
        }
        if let Some(user_https) = &self.https {
            return (user_https.clone(), UserIdType::HTTPS);
        }
        (DEFAULT_USER.to_string(), UserIdType::UNKNOWN)
    }

    pub fn get_login_id(&self) -> String {
        if let Some(login_id) = &self.login_id {
            return login_id.clone();
        }
        DEFAULT_AUTH.to_string()
    }
}

/**************************************************************************/
/*                                                                        */
/*                   UserSettings loading from Cfg.User                   */
/*                                                                        */
/**************************************************************************/

impl TryFrom<(Option<&User>, &Vec<String>)> for UserSettings {
    type Error = ConfigError;

    fn try_from(cfg_and_xetea_urls: (Option<&User>, &Vec<String>)) -> Result<Self, Self::Error> {
        let (user_cfg, xetea_urls) = cfg_and_xetea_urls;
        let mut user = UserSettings::default();
        // code below allow git_integration tests to work
        #[allow(unused_mut)]
        let mut xetea_auth = XeteaAuth::default();
        #[cfg(test)]
        xetea_auth.expect_fetch_https_output().returning(|_| {
            Ok("protocol=https\nhost=xethub.com\nusername=bob\npassword=abcd".to_string())
        });

        let xetea_owners = xetea_urls
            .iter()
            .map(|url| get_xet_owner(url))
            .filter_map(|x| x.ok())
            .collect::<Vec<_>>()
            .into_iter()
            .unique()
            .collect::<Vec<_>>()
            .join(":");
        user.owner = Some(xetea_owners);
        if let Some(user_cfg) = user_cfg {
            let mut user_cfg_clone = user_cfg.clone();
            match user_cfg.ssh.as_ref() {
                Some(x_a_s) if !x_a_s.is_empty() => {
                    user.ssh = Some(x_a_s.clone());
                }
                _ => {
                    let xetea_users_ssh = xetea_urls
                        .iter()
                        .map(|url| get_xet_user_ssh(url, &xetea_auth))
                        .filter_map(|x| x.ok())
                        .collect::<Vec<_>>()
                        .into_iter()
                        .unique()
                        .collect::<Vec<_>>()
                        .join(":");
                    if !xetea_users_ssh.is_empty() {
                        user.ssh = Some(xetea_users_ssh.clone());
                        user_cfg_clone.ssh = Some(xetea_users_ssh.clone());
                        if let Ok(loader) = config::xet::create_config_loader() {
                            let _ =
                                loader.override_value(Level::GLOBAL, "user.ssh", xetea_users_ssh);
                        }
                    }
                }
            };
            match user_cfg.https.as_ref() {
                Some(x_a_h) if !x_a_h.is_empty() => {
                    user.https = Some(x_a_h.clone());
                }
                _ => {
                    let xetea_users_https = xetea_urls
                        .iter()
                        .map(|url| get_xet_user_https(url, &xetea_auth))
                        .filter_map(|x| if let Ok(Some(y)) = x { Some(y) } else { None })
                        .collect::<Vec<_>>()
                        .into_iter()
                        .unique()
                        .collect::<Vec<_>>()
                        .join(":");
                    if !xetea_users_https.is_empty() {
                        user.https = Some(xetea_users_https.clone());
                        user_cfg_clone.https = Some(xetea_users_https.clone());
                        if let Ok(loader) = config::xet::create_config_loader() {
                            let _ = loader.override_value(
                                Level::GLOBAL,
                                "user.https",
                                xetea_users_https,
                            );
                        }
                    }
                }
            };

            match user_cfg.email.as_ref() {
                Some(email) if !email.is_empty() => {
                    user.email = Some(email.clone());
                }
                _ => {}
            }

            match user_cfg.login_id.as_ref() {
                Some(login_id) if !login_id.is_empty() => {
                    user.login_id = Some(login_id.clone());
                }
                _ => {}
            }

            match user_cfg.name.as_ref() {
                Some(name) if !name.is_empty() => {
                    user.name = Some(name.clone());
                }
                _ => {}
            }

            match user_cfg.token.as_ref() {
                Some(token) if !token.is_empty() => {
                    user.token = Some(token.clone());
                }
                _ => {}
            }
        }

        Ok(user)
    }
}

#[double]
use crate::user::XeteaAuth;

fn get_xet_user_ssh(remote: &str, xetea_auth: &XeteaAuth) -> Result<String, ConfigError> {
    if !remote.starts_with("xet@") {
        return Err(InvalidGitRemote(remote.to_string()));
    }
    let splits: Vec<_> = remote.split(&['/', ':'][..]).collect();
    if splits.len() < 2 || !splits[0].contains("xet") {
        return Err(InvalidGitRemote(remote.to_string()));
    }
    let xet_user_hostname = splits[0].to_string();
    let ssh_output = xetea_auth
        .fetch_ssh_output(&xet_user_hostname)
        .map_err(|e| InvalidUserCommandOutput(e.to_string()))?;
    let splits: Vec<_> = ssh_output.split(&[' ', ',', '!'][..]).collect();
    if splits.len() < 4 {
        return Err(InvalidUserCommandOutput(ssh_output.to_string()));
    }
    Ok(splits[3].to_string())
}

fn get_xet_user_https(remote: &str, xetea_auth: &XeteaAuth) -> Result<Option<String>, ConfigError> {
    let remote_url = Url::parse(remote).map_err(|_| InvalidGitRemote(remote.to_string()))?;
    if remote_url.scheme() != "https" {
        return Err(InvalidGitRemote(remote.to_string()));
    }
    // try to get the user from the url path
    if remote_url.username() != "" {
        return Ok(Some(remote_url.username().to_string()));
    }
    // if Token is used for authentication, it should already be in the
    // the remote URL above.

    // next try the git creds for the hostname
    if let Some(hostname) = remote_url.host_str() {
        let https_output = xetea_auth
            .fetch_https_output(hostname)
            .map_err(|e| InvalidUserCommandOutput(e.to_string()))?;
        if let Some(username_kv) = https_output
            .split(&['\n'][..])
            .find(|&kv| kv.starts_with("username"))
        {
            let splits: Vec<_> = username_kv.split(&['='][..]).collect();
            if splits.len() == 2 {
                return Ok(Some(splits[1].to_string()));
            }
        }
    }
    Ok(None)
}

fn get_xet_owner(remote: &str) -> Result<String, ConfigError> {
    // parse out the xet_owner
    // The path form is either https://xethub.com/[username]/[repo].git
    // Or xet@xethub.com:[username]/[repo].git
    // we can extract [username] by splitting on both ":" and "/" and taking
    // the second-to-last element.
    if remote.starts_with("xet@") {
        let splits: Vec<_> = remote.split(&['/', ':'][..]).collect();
        if splits.len() > 2 {
            let username = splits[1].to_string();
            return Ok(username);
        }
    } else {
        let remote_url = Url::parse(remote).map_err(|_| InvalidGitRemote(remote.to_string()))?;
        if remote_url.scheme() == "https" {
            if let Some(path_segments) = remote_url.path_segments() {
                let path_segments: Vec<_> = path_segments.collect();
                if path_segments.len() > 1 {
                    return Ok(path_segments[0].to_string());
                }
            }
        }
    }
    Err(InvalidGitRemote(remote.to_string()))
}

#[cfg(test)]
mod user_config_tests {
    use crate::config::user::{
        get_xet_owner, get_xet_user_https, get_xet_user_ssh, UserIdType, UserSettings,
    };
    use crate::user::MockXeteaAuth;
    use cas::constants::DEFAULT_USER;

    #[test]
    fn owner_test_ssh() {
        let remote_url = "xet@xethub.com:oscar/ppp_foia.git";
        assert_eq!("oscar".to_string(), get_xet_owner(remote_url).unwrap());
        let remote_url = "xet@xethub.com:bad.git";
        assert!(get_xet_owner(remote_url).is_err());
    }

    #[test]
    fn owner_test_https_uri() {
        let remote_url = "https://xethub.com/owner/Tutorial.git";
        assert_eq!("owner".to_string(), get_xet_owner(remote_url).unwrap());

        let remote_url = "https://xethub.com/bad.git";
        assert!(get_xet_owner(remote_url).is_err());
    }

    #[test]
    fn owner_test_bad() {
        let remote_url = "bad_bad_bad";
        assert!(get_xet_owner(remote_url).is_err());
    }

    #[test]
    fn user_test_bad() {
        let mut mock_xetea_ssh = MockXeteaAuth::default();
        mock_xetea_ssh
            .expect_fetch_ssh_output()
            .returning(|_| Ok("BadSSHError".to_string()));

        let remote_url = "bad_bad_bad";
        assert!(get_xet_user_ssh(remote_url, &mock_xetea_ssh).is_err());

        let remote_url = "git@github.com:xetdata/xethub.git";
        assert!(get_xet_user_ssh(remote_url, &mock_xetea_ssh).is_err());

        // valid url
        let remote_url = "xet@xethub.com:wendy/vega.git";
        assert!(get_xet_user_ssh(remote_url, &mock_xetea_ssh).is_err());
    }

    #[test]
    fn user_test_ssh() {
        let mut mock_xetea_ssh = MockXeteaAuth::default();
        mock_xetea_ssh
            .expect_fetch_ssh_output()
            .returning(|_| Ok("Hi there, bob! You've successfully authenticated with the key named work laptop, but XetHub does not provide shell access.
 If this is unexpected, please log in with password and setup XetHub under another user.".to_string()));
        let remote_url = "xet@xethub.com:wendy/vega.git";
        assert_eq!(
            "bob".to_string(),
            get_xet_user_ssh(remote_url, &mock_xetea_ssh).unwrap()
        );
    }

    #[test]
    fn user_test_https() {
        let mut mock_xetea_https = MockXeteaAuth::default();
        mock_xetea_https.expect_fetch_https_output().returning(|_| {
            Ok("protocol=https\nhost=xethub.com\nusername=bob\npassword=abcd".to_string())
        });
        let remote_url = "https://xethub.com/carol/Tutorial.git";
        assert_eq!(
            "bob".to_string(),
            get_xet_user_https(remote_url, &mock_xetea_https)
                .unwrap()
                .unwrap()
        );
        let remote_url = "https://accessor:pw@xethub.com/owner/Tutorial.git";
        assert_eq!(
            "accessor".to_string(),
            get_xet_user_https(remote_url, &mock_xetea_https)
                .unwrap()
                .unwrap()
        );
    }

    #[test]
    fn get_user_id_test() {
        // test parsing out user_id from UserSettings struct
        let user_id_ssh: &str = "xet_user_id_ssh";
        let user_id_https: &str = "xet_user_id_https";
        let with_ssh = UserSettings {
            ssh: Some(user_id_ssh.to_string()),
            ..Default::default()
        };
        let with_https = UserSettings {
            https: Some(user_id_https.to_string()),
            ..Default::default()
        };
        let with_nothing = UserSettings::default();
        let with_ssh_and_https = UserSettings {
            ssh: Some(user_id_ssh.to_string()),
            https: Some(user_id_https.to_string()),
            ..Default::default()
        };

        let (user_id, user_id_type) = with_ssh.get_user_id();
        assert_eq!(user_id.as_str(), user_id_ssh);
        assert_eq!(user_id_type, UserIdType::SSH);

        let (user_id, user_id_type) = with_https.get_user_id();
        assert_eq!(user_id.as_str(), user_id_https);
        assert_eq!(user_id_type, UserIdType::HTTPS);

        // should be anonymous
        let (user_id, user_id_type) = with_nothing.get_user_id();
        assert_eq!(user_id.as_str(), DEFAULT_USER);
        assert_eq!(user_id_type, UserIdType::UNKNOWN);

        // check for user_ssh priority
        let (user_id, user_id_type) = with_ssh_and_https.get_user_id();
        assert_eq!(user_id.as_str(), user_id_ssh);
        assert_eq!(user_id_type, UserIdType::SSH);
    }
}
