use crate::config::ConfigError;
use crate::config::ConfigError::{InvalidCasEndpoint, InvalidCasPrefix, InvalidCasSizeThreshold};
use crate::constants::{LOCAL_CAS_SCHEME, SMALL_FILE_THRESHOLD};
use http::Uri;
use std::path::PathBuf;
use std::str::FromStr;
use tracing::debug;
use xet_config::{Cas, DEFAULT_CAS_PREFIX, PROD_CAS_ENDPOINT};

/// safe and special handling chars from: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
const VALID_PREFIX_SPECIAL_CHARS: &str = "!-_.*'()&$@=,/:+ ,?";

fn is_valid_prefix_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || VALID_PREFIX_SPECIAL_CHARS.contains(c)
}

#[derive(Debug, Clone, Default)]
pub struct CasSettings {
    pub endpoint: String,
    pub prefix: String,
    pub size_threshold: usize,
}

impl CasSettings {
    pub fn shard_prefix(&self) -> String {
        format!("{}-merkledb", &self.prefix)
    }
}

impl TryFrom<Option<&Cas>> for CasSettings {
    type Error = ConfigError;

    fn try_from(cas: Option<&Cas>) -> Result<Self, Self::Error> {
        let (endpoint, prefix, size_threshold) = match cas {
            Some(x) => {
                let endpoint = match &x.server {
                    Some(server) if !server.is_empty() => {
                        debug!("Cas Settings config: Remote server is {server}");
                        check_uri(server)?;
                        Some(server)
                    }
                    _ => None,
                };

                let prefix = match &x.prefix {
                    Some(prefix) => {
                        debug!("Cas Settings config: prefix = {prefix}");
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

                let size_threshold = match x.sizethreshold {
                    Some(threshold) => {
                        debug!("Cas Settings config: sizethreshold = {threshold}");
                        if threshold > SMALL_FILE_THRESHOLD {
                            return Err(InvalidCasSizeThreshold(threshold, SMALL_FILE_THRESHOLD))
                        }
                        Some(threshold)
                    }
                    None => None
                };
                (endpoint, prefix, size_threshold)
            }
            None => (None, None, None),
        };

        Ok(match (endpoint, prefix, size_threshold) {
            (Some(endpoint), Some(prefix), Some(size_threshold)) => CasSettings {
                endpoint: endpoint.clone(),
                prefix: prefix.clone(),
                size_threshold,
            },
            (ep_opt, pr_opt, st_opt) => {
                let dflt_endpoint = PROD_CAS_ENDPOINT.to_string();
                let dflt_prefix = DEFAULT_CAS_PREFIX.to_string();
                let dflt_size_threshold = SMALL_FILE_THRESHOLD;

                CasSettings {
                    endpoint: ep_opt.unwrap_or(&dflt_endpoint).clone(),
                    prefix: pr_opt.unwrap_or(&dflt_prefix).clone(),
                    size_threshold: st_opt.unwrap_or(dflt_size_threshold),
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
    use xet_config::{Cas, DEFAULT_CAS_PREFIX, PROD_CAS_ENDPOINT};

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
            sizethreshold: Some(1024),
        };

        let cas_settings: CasSettings = Some(&cas_cfg).try_into().unwrap();
        assert_eq!(cas_cfg.prefix.unwrap(), cas_settings.prefix);
        assert_eq!(cas_cfg.server.unwrap(), cas_settings.endpoint);
        assert_eq!(cas_cfg.sizethreshold.unwrap(), cas_settings.size_threshold);
    }

    #[test]
    fn test_cas_into_settings_default_prefix() {
        let cas_cfg = Cas {
            server: Some("https://my-cas".to_string()),
            sizethreshold: Some(2345),
            ..Default::default()
        };

        let cas_settings: CasSettings = Some(&cas_cfg).try_into().unwrap();
        assert_eq!(DEFAULT_CAS_PREFIX, cas_settings.prefix);
        assert_eq!(cas_cfg.server.unwrap(), cas_settings.endpoint);
        assert_eq!(cas_cfg.sizethreshold.unwrap(), cas_settings.size_threshold);
    }

    #[test]
    fn test_cas_into_settings_default_endpoint() {
        let cas_cfg = Cas {
            prefix: Some("the/prefix".to_string()),
            sizethreshold: Some(98732),
            ..Default::default()
        };

        let cas_settings: CasSettings = Some(&cas_cfg).try_into().unwrap();
        assert_eq!(cas_cfg.prefix.unwrap(), cas_settings.prefix);
        assert_eq!(PROD_CAS_ENDPOINT.to_string(), cas_settings.endpoint);
        assert_eq!(cas_cfg.sizethreshold.unwrap(), cas_settings.size_threshold);
    }

    #[test]
    fn test_cas_into_settings_default_size_threshold() {
        let cas_cfg = Cas {
            server: Some("https://my-cas".to_string()),
            prefix: Some("the/prefix".to_string()),
            ..Default::default()
        };

        let cas_settings: CasSettings = Some(&cas_cfg).try_into().unwrap();
        assert_eq!(cas_cfg.prefix.unwrap(), cas_settings.prefix);
        assert_eq!(cas_cfg.server.unwrap(), cas_settings.endpoint);
        assert_eq!(SMALL_FILE_THRESHOLD, cas_settings.size_threshold);
    }

    #[test]
    fn test_cas_into_settings_default() {
        let cas_cfg = Cas::default();
        let cas_settings: CasSettings = Some(&cas_cfg).try_into().unwrap();
        assert_eq!(DEFAULT_CAS_PREFIX.to_string(), cas_settings.prefix);
        assert_eq!(PROD_CAS_ENDPOINT.to_string(), cas_settings.endpoint);
        assert_eq!(SMALL_FILE_THRESHOLD, cas_settings.size_threshold);
    }

    #[test]
    fn test_cas_into_settings_none() {
        let cas_settings: CasSettings = None.try_into().unwrap();
        assert_eq!(DEFAULT_CAS_PREFIX.to_string(), cas_settings.prefix);
        assert_eq!(PROD_CAS_ENDPOINT.to_string(), cas_settings.endpoint);
        assert_eq!(SMALL_FILE_THRESHOLD, cas_settings.size_threshold);
    }

    #[test]
    fn test_cas_into_settings_validation() {
        let cas_cfg = Cas {
            prefix: Some("bäd%prefix".to_string()),
            ..Default::default()
        };
        let res: Result<CasSettings, ConfigError> = Some(&cas_cfg).try_into();
        assert!(res.is_err())
    }
}
