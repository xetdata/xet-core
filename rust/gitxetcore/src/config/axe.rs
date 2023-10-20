use crate::config::ConfigError;
use crate::config::ConfigError::InvalidAxeEnabled;
use xet_config::{Axe, PROD_AXE_CODE};

#[derive(Debug, Clone)]
pub struct AxeSettings {
    pub enabled: bool,
    pub axe_code: String,
}

impl Default for AxeSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            axe_code: PROD_AXE_CODE.to_string(),
        }
    }
}

impl AxeSettings {
    pub fn enabled(&self) -> bool {
        self.enabled
    }
}

impl TryFrom<Option<&Axe>> for AxeSettings {
    type Error = ConfigError;

    fn try_from(axe_cfg: Option<&Axe>) -> Result<Self, Self::Error> {
        let mut axe = AxeSettings::default();
        if let Some(axe_cfg) = axe_cfg {
            if let Some(enabled) = &axe_cfg.enabled {
                if let Some(axe_code) = &axe_cfg.axe_code {
                    axe.enabled = check_enabled(enabled.as_str())?;
                    axe.axe_code = axe_code.to_string()
                }
            }
        }
        Ok(axe)
    }
}

fn check_enabled(enabled: &str) -> Result<bool, ConfigError> {
    let match_str = enabled.to_lowercase();
    match match_str.as_str() {
        "true" | "yes" | "t" | "y" | "1" => Ok(true),
        "false" | "no" | "f" | "n" | "0" => Ok(false),
        &_ => Err(InvalidAxeEnabled(enabled.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test::assert_err;

    #[test]
    fn test_check_enabled() {
        fn check(s: &str, expected: bool) {
            assert_eq!(expected, check_enabled(s).unwrap());
        }
        fn check_err(s: &str) {
            assert_err!(check_enabled(s));
        }
        check("true", true);
        check("True", true);
        check("TrUe", true);
        check("yes", true);
        check("Yes", true);
        check("yES", true);
        check("t", true);
        check("T", true);
        check("y", true);
        check("Y", true);
        check("1", true);

        check("false", false);
        check("FALSE", false);
        check("FaLsE", false);
        check("no", false);
        check("nO", false);
        check("NO", false);
        check("F", false);
        check("f", false);
        check("N", false);
        check("n", false);
        check("0", false);

        check_err("other");
        check_err("maybe");
        check_err("-1");
        check_err("n0");
    }

    #[test]
    fn test_parse() {
        let axe_cfg = Axe {
            enabled: Some("true".to_string()),
            axe_code: Some("6767".to_string()),
        };
        let axe_settings = AxeSettings::try_from(Some(&axe_cfg)).unwrap();
        assert!(axe_settings.enabled());
        assert_eq!(axe_settings.axe_code, "6767".to_string());
    }

    #[test]
    fn test_parse_disabled() {
        let axe_cfg = Axe {
            enabled: Some("f".to_string()),
            axe_code: Some("6767".to_string()),
        };
        let axe_settings = AxeSettings::try_from(Some(&axe_cfg)).unwrap();
        assert!(!axe_settings.enabled());
    }

    #[test]
    fn test_parse_error() {
        let axe_cfg = Axe {
            enabled: Some("invalid".to_string()),
            axe_code: Some("6767".to_string()),
        };
        assert_err!(AxeSettings::try_from(Some(&axe_cfg)));
    }
}
