use crate::config::env::XetEnv;
use crate::config::ConfigError;
use crate::config::ConfigError::{InvalidCasEndpoint, InvalidCasPrefix};
use crate::constants::LOCAL_CAS_SCHEME;
use http::Uri;
use std::path::PathBuf;
use std::str::FromStr;
use xet_config::Cas;

/// safe and special handling chars from: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
const VALID_PREFIX_SPECIAL_CHARS: &str = "!-_.*'()&$@=,/:+ ,?";

fn is_valid_prefix_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || VALID_PREFIX_SPECIAL_CHARS.contains(c)
}

#[derive(Debug, Clone, Default)]
pub struct CasSettings {
    pub endpoint: String,
    pub prefix: String,
}

impl TryFrom<(Option<&Cas>, &XetEnv)> for CasSettings {
    type Error = ConfigError;

    fn try_from(cas_and_xetenv: (Option<&Cas>, &XetEnv)) -> Result<Self, Self::Error> {
        let (cas, xetenv) = cas_and_xetenv;
        let (endpoint, prefix) = match cas {
            Some(x) => {
                let endpoint = match &x.server {
                    Some(server) if !server.is_empty() => {
                        check_uri(server)?;
                        Some(server)
                    }
                    _ => None,
                };

                let prefix = match &x.prefix {
                    Some(prefix) => {
                        if !prefix.chars().all(is_valid_prefix_char) {
                            return Err(InvalidCasPrefix(
                                prefix.clone(),
                                VALID_PREFIX_SPECIAL_CHARS,
                            ));
                        }
                        Some(prefix)
                    }
                    None => None,
                };
                (endpoint, prefix)
            }
            None => (None, None),
        };

        Ok(match (endpoint, prefix) {
            (Some(endpoint), Some(prefix)) => CasSettings {
                endpoint: endpoint.clone(),
                prefix: prefix.clone(),
            },
            (ep_opt, pr_opt) => {
                let dflt_endpoint = xetenv.get_default_cas_endpoint();
                let dflt_prefix = xetenv.get_default_cas_prefix();

                CasSettings {
                    endpoint: ep_opt.unwrap_or(&dflt_endpoint).clone(),
                    prefix: pr_opt.unwrap_or(&dflt_prefix).clone(),
                }
            }
        })
    }
}

fn check_uri(endpoint: &str) -> Result<(), ConfigError> {
    match endpoint.strip_prefix(LOCAL_CAS_SCHEME) {
        Some(path) => {
            PathBuf::from_str(path)
                .map_err(|e| InvalidCasEndpoint(endpoint.to_string(), e.into()))?;
            Ok(())
        }
        None => {
            endpoint
                .parse::<Uri>()
                .map_err(|e| InvalidCasEndpoint(endpoint.to_string(), e.into()))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod cas_setting_tests {
    use super::*;
    use crate::config::env::DEV_CAS_ENDPOINT;
    use xet_config::{Cas, DEFAULT_CAS_PREFIX};

    #[test]
    fn test_uri_check() {
        check_uri("cas-lb.xetsvc.com:5000").unwrap();
        check_uri("https://foo.bar.com").unwrap();
        check_uri("local:///tmp/cas").unwrap();

        assert!(check_uri("ftp:///bar").is_err());
        assert!(check_uri("not a url").is_err());
        assert!(check_uri("çoøl.com").is_err());
    }

    #[test]
    fn test_cas_into_settings() {
        let cas_cfg = Cas {
            prefix: Some("non_default".to_string()),
            server: Some("https://my-cas".to_string()),
        };

        let cas_settings: CasSettings = (Some(&cas_cfg), &XetEnv::Dev).try_into().unwrap();
        assert_eq!(cas_cfg.prefix.unwrap(), cas_settings.prefix);
        assert_eq!(cas_cfg.server.unwrap(), cas_settings.endpoint);
    }

    #[test]
    fn test_cas_into_settings_default_prefix() {
        let cas_cfg = Cas {
            server: Some("https://my-cas".to_string()),
            ..Default::default()
        };

        let cas_settings: CasSettings = (Some(&cas_cfg), &XetEnv::Dev).try_into().unwrap();
        assert_eq!(DEFAULT_CAS_PREFIX, cas_settings.prefix);
        assert_eq!(cas_cfg.server.unwrap(), cas_settings.endpoint);
    }

    #[test]
    fn test_cas_into_settings_default_endpoint() {
        let cas_cfg = Cas {
            prefix: Some("the/prefix".to_string()),
            ..Default::default()
        };

        let cas_settings: CasSettings = (Some(&cas_cfg), &XetEnv::Dev).try_into().unwrap();
        assert_eq!(cas_cfg.prefix.unwrap(), cas_settings.prefix);
        assert_eq!(DEV_CAS_ENDPOINT.to_string(), cas_settings.endpoint);
    }

    #[test]
    fn test_cas_into_settings_default() {
        let cas_cfg = Cas::default();
        let cas_settings: CasSettings = (Some(&cas_cfg), &XetEnv::Dev).try_into().unwrap();
        assert_eq!(DEFAULT_CAS_PREFIX.to_string(), cas_settings.prefix);
        assert_eq!(DEV_CAS_ENDPOINT.to_string(), cas_settings.endpoint);
    }

    #[test]
    fn test_cas_into_settings_none() {
        let cas_settings: CasSettings = (None, &XetEnv::Dev).try_into().unwrap();
        assert_eq!(DEFAULT_CAS_PREFIX.to_string(), cas_settings.prefix);
        assert_eq!(DEV_CAS_ENDPOINT.to_string(), cas_settings.endpoint);
    }

    #[test]
    fn test_cas_into_settings_validation() {
        let cas_cfg = Cas {
            prefix: Some("bäd%prefix".to_string()),
            ..Default::default()
        };
        let res: Result<CasSettings, ConfigError> = (Some(&cas_cfg), &XetEnv::Dev).try_into();
        assert!(res.is_err())
    }
}
