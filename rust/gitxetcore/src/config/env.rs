use crate::config::ConfigError;
use crate::config::ConfigError::UnsupportedConfiguration;
use xet_config::DEFAULT_CAS_PREFIX;

// default
pub const DEFAULT_AXE_CODE: &str = "phc_RD6wgvoPaccFflB5b2REGPeKNb3qcZh9YuvH8omqEjT";
pub const DEV_AXE_CODE: &str = "phc_47JxZFUnN8Od2Qn3u6psZSQ6s1Imu2I3Xtn7WUJSKk2";
pub const BETA_AXE_CODE: &str = "phc_LV7r5LyvsV5copsKABYINIdJsV3qw7UIA2VeTGIcxZs";
pub const PROD_AXE_CODE: &str = "phc_aE643CSQ5F9MrqF8VT1gr7smML8hDU8gzH9lZ4WhdUY";

pub const DEV_CAS_ENDPOINT: &str = "cas-lb.xetsvc.com:443";
pub const BETA_CAS_ENDPOINT: &str = "cas-lb.xetbeta.com:443";
pub const PROD_CAS_ENDPOINT: &str = "cas-lb.xethub.com:443";

pub const DEV_XETEA_DOMAIN: &str = "xetsvc.com";
pub const BETA_XETEA_DOMAIN: &str = "xetbeta.com";
pub const PROD_XETEA_DOMAIN: &str = "xethub.com";

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum XetEnv {
    Custom,
    Dev,
    Beta,
    Prod,
}

impl XetEnv {
    pub fn get_default_cas_endpoint(&self) -> String {
        match self {
            XetEnv::Custom => PROD_CAS_ENDPOINT.to_string(),
            XetEnv::Dev => DEV_CAS_ENDPOINT.to_string(),
            XetEnv::Beta => BETA_CAS_ENDPOINT.to_string(),
            XetEnv::Prod => PROD_CAS_ENDPOINT.to_string(),
        }
    }

    pub fn get_default_cas_prefix(&self) -> String {
        DEFAULT_CAS_PREFIX.to_string()
    }

    pub fn from_xetea_url(url: &str) -> XetEnv {
        match url {
            _ if url.contains(DEV_XETEA_DOMAIN) => XetEnv::Dev,
            _ if url.contains(BETA_XETEA_DOMAIN) => XetEnv::Beta,
            _ if url.contains(PROD_XETEA_DOMAIN) => XetEnv::Prod,
            _ => XetEnv::Custom,
        }
    }

    pub fn get_axe_code(&self) -> String {
        match self {
            XetEnv::Custom => DEFAULT_AXE_CODE.to_string(),
            XetEnv::Dev => DEV_AXE_CODE.to_string(),
            XetEnv::Beta => BETA_AXE_CODE.to_string(),
            XetEnv::Prod => PROD_AXE_CODE.to_string(),
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
            XetEnv::Dev,
            XetEnv::Custom,
            XetEnv::Custom,
            XetEnv::Dev,
        ])
        .unwrap();
        assert_eq!(XetEnv::Dev, env);
        let env = resolve_env(vec![XetEnv::Dev]).unwrap();
        assert_eq!(XetEnv::Dev, env);
        let env = resolve_env(vec![XetEnv::Custom]).unwrap();
        assert_eq!(XetEnv::Custom, env);
        let env = resolve_env(vec![XetEnv::Custom, XetEnv::Custom]).unwrap();
        assert_eq!(XetEnv::Custom, env);
        let env = resolve_env(vec![]).unwrap();
        assert_eq!(XetEnv::Prod, env);
    }
    #[test]
    fn test_resolve_env_failures() {
        assert!(resolve_env(vec![XetEnv::Custom, XetEnv::Prod, XetEnv::Beta]).is_err());
        assert!(resolve_env(vec![XetEnv::Prod, XetEnv::Custom, XetEnv::Beta]).is_err());
        assert!(resolve_env(vec![XetEnv::Prod, XetEnv::Beta]).is_err());
    }
}
