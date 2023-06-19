use crate::config::ConfigError;
use crate::config::ConfigError::UnsupportedConfiguration;
use xet_config::{DEFAULT_CAS_PREFIX, PROD_CAS_ENDPOINT};

pub const PROD_XETEA_DOMAIN: &str = "xethub.com";

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum XetEnv {
    Custom,
    Prod,
}

impl XetEnv {
    pub fn get_default_cas_endpoint(&self) -> String {
        PROD_CAS_ENDPOINT.to_string()
    }

    pub fn get_default_cas_prefix(&self) -> String {
        DEFAULT_CAS_PREFIX.to_string()
    }

    pub fn from_xetea_url(url: &str) -> XetEnv {
        match url {
            _ if url.contains(PROD_XETEA_DOMAIN) => XetEnv::Prod,
            _ => XetEnv::Custom,
        }
    }
}

pub fn resolve_env(remote_envs: Vec<XetEnv>) -> Result<XetEnv, ConfigError> {
    let mut env = None;
    for elem in remote_envs {
        if let Some(old_env) = env {
            // Skip Custom since we are checking for XetHub environment consistency
            if elem == XetEnv::Custom {
                continue;
            }
            // If the old_env is Custom, we should overwrite with the XetHub environment
            if old_env == XetEnv::Custom {
                env = Some(elem);
            } else if old_env != elem {
                let err_msg = format!("The git remotes are using different Xet environments: (e.g. {elem:?}, {old_env:?}). This is an unsupported configuration");
                return Err(UnsupportedConfiguration(err_msg));
            }
        } else {
            env = Some(elem);
        }
    }
    Ok(env.unwrap_or(XetEnv::Prod))
}

#[cfg(test)]
mod env_tests {
    use crate::config::env::{resolve_env, XetEnv};

    #[test]
    fn test_resolve_env() {
        let env = resolve_env(vec![XetEnv::Custom, XetEnv::Prod, XetEnv::Prod]).unwrap();
        assert_eq!(XetEnv::Prod, env);
        let env = resolve_env(vec![
            XetEnv::Prod,
            XetEnv::Custom,
            XetEnv::Custom,
            XetEnv::Prod,
        ])
        .unwrap();
        assert_eq!(XetEnv::Prod, env);
        let env = resolve_env(vec![XetEnv::Prod]).unwrap();
        assert_eq!(XetEnv::Prod, env);
        let env = resolve_env(vec![XetEnv::Custom]).unwrap();
        assert_eq!(XetEnv::Custom, env);
        let env = resolve_env(vec![XetEnv::Custom, XetEnv::Custom]).unwrap();
        assert_eq!(XetEnv::Custom, env);
        let env = resolve_env(vec![]).unwrap();
        assert_eq!(XetEnv::Prod, env);
    }

    /*
    #[test]
    fn test_resolve_env_failures() {
        assert!(resolve_env(vec![XetEnv::Custom, XetEnv::Prod, XetEnv::Prod]).is_err());
        assert!(resolve_env(vec![XetEnv::Prod, XetEnv::Custom, XetEnv::Prod]).is_err());
        assert!(resolve_env(vec![XetEnv::Prod, XetEnv::Prod]).is_err());
    }
    */
}
