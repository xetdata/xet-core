use std::fs;
use std::path::PathBuf;

use config::builder::DefaultState;
use config::{Config, ConfigBuilder, ConfigError, Environment, File, FileFormat, Value, ValueKind};
use tracing::{debug, warn};

use crate::cfg::Cfg;
use crate::error::CfgError;
use crate::error::CfgError::{InvalidLevelModification, InvalidValue};
use crate::level::{Level, LEVELS};

const ENV_ROOT: &str = "XET";
const ENV_SEPARATOR: &str = "_";

/// Struct for managing the Xet instance's config across the various "levels" it's defined in.
/// There are various "load" methods that operate on the specifically mentioned config level
/// and "resolve" methods that will read all levels with <= priority than the indicated level.
pub struct XetConfigLoader {
    local_path: PathBuf,
    global_path: PathBuf,
}

impl XetConfigLoader {
    pub fn new(local_path: PathBuf, global_path: PathBuf) -> Self {
        Self {
            local_path,
            global_path,
        }
    }

    /// Overrides the provided key:value pair for the config at the indicated level.
    /// Only `Level::LOCAL` and `Level::GLOBAL` configs are allowed to be modified.
    ///
    /// Note that this will rewrite the entire config file with the settings that
    /// we parsed out of the file. This means that any non-xet configs in the file
    /// will be removed.
    pub fn override_value<T: Into<Value>>(
        &self,
        level: Level,
        key: &str,
        val: T,
    ) -> Result<(), CfgError> {
        match level {
            Level::LOCAL | Level::GLOBAL => {}
            _ => return Err(InvalidLevelModification(level)),
        }
        let mut settings = Config::builder();
        settings = self.add_config(settings, level);
        let config = settings.set_override(key, val)?.build()?;
        let cfg: Cfg = config.try_deserialize().map_err(|_| InvalidValue)?;
        self.serialize_to_file(&cfg, level)
    }

    /// Removes the indicated key from the config file at the indicated Level.
    /// Like `override_value` this only allows `Level::LOCAL` and `Level::GLOBAL`
    /// and will overwrite the file on disk.
    pub fn remove_value(&self, level: Level, key: &str) -> Result<(), CfgError> {
        self.override_value(level, key, ValueKind::Nil)
    }

    /// Loads configuration from the indicated level, providing the config if it exists.
    /// If there is an issue or the config isn't found, then an error will be returned.
    pub fn load_config(&self, level: Level) -> Result<Cfg, CfgError> {
        let mut settings = Config::builder();
        settings = self.add_config(settings, level);
        let config = settings.build()?;
        debug!("Config built: {config:?}");
        Ok(config.try_deserialize()?)
    }

    /// Similar to `load_config`, however this method will also read in any lower-priority
    /// configs below the level, merging them into the final config returned.
    pub fn resolve_config(&self, level: Level) -> Result<Cfg, CfgError> {
        let mut settings = Config::builder();
        // LEVELS is sorted ascending, thus properly overriding the config
        for l in LEVELS.into_iter() {
            if l <= level {
                settings = self.add_config(settings, l);
            }
        }
        let config = settings.build()?;
        Ok(config.try_deserialize()?)
    }

    /// Reads the config for the specified level, searching for the indicated key.
    ///
    /// If the key cannot be found (i.e. it is missing), then Ok(None) is returned.
    /// Returns an error if there is an issue retrieving the value or with the key
    /// (e.g. the provided key is not formatted properly).
    pub fn load_value(&self, level: Level, key: &str) -> Result<Option<String>, CfgError> {
        let mut settings = Config::builder();
        settings = self.add_config(settings, level);
        let config = settings.build()?;
        Self::get_value(&config, key)
    }

    /// Similar to `read_value`, however this method will also search in any lower-priority
    /// configs below the level if not found at the indicated level.
    pub fn resolve_value(&self, level: Level, key: &str) -> Result<Option<String>, CfgError> {
        let mut settings = Config::builder();
        // LEVELS is sorted ascending, thus properly overriding the config
        for l in LEVELS.into_iter() {
            if l <= level {
                settings = self.add_config(settings, l);
            }
        }
        let config = settings.build()?;
        Self::get_value(&config, key)
    }

    // unlike `config.get(key)` this will return an option if the key doesn't exist, making it
    // easier for the caller to know whether the setting wasn't set or if there is an issue
    // in the user's key string.
    fn get_value(config: &Config, key: &str) -> Result<Option<String>, CfgError> {
        let val = config
            .get::<Value>(key)
            .or_else(|e| match e {
                ConfigError::NotFound(_) => Ok(Value::new(None, ValueKind::Nil)),
                _ => Err(e),
            })
            .map(|v| {
                if v.kind == ValueKind::Nil {
                    None
                } else {
                    Some(v.to_string())
                }
            })?;
        Ok(val)
    }

    fn add_config(
        &self,
        settings: ConfigBuilder<DefaultState>,
        level: Level,
    ) -> ConfigBuilder<DefaultState> {
        match level {
            Level::ENV => settings.add_source(
                Environment::with_prefix(ENV_ROOT)
                    .separator(ENV_SEPARATOR)
                    .prefix_separator(ENV_SEPARATOR),
            ),
            Level::LOCAL => settings.add_source(
                File::from(self.local_path.clone())
                    .required(false)
                    .format(FileFormat::Toml),
            ),
            Level::GLOBAL => settings.add_source(
                File::from(self.global_path.clone())
                    .required(false)
                    .format(FileFormat::Toml),
            ),
            Level::DEFAULT => settings.add_source(get_default_config()),
        }
    }

    fn serialize_to_file(&self, cfg: &Cfg, level: Level) -> Result<(), CfgError> {
        let data = toml::to_string(&cfg)?;
        let path = match level {
            Level::ENV | Level::DEFAULT => return Err(InvalidLevelModification(level)),
            Level::LOCAL => self.local_path.clone(),
            Level::GLOBAL => self.global_path.clone(),
        };
        if let Some(parent_dir) = path.parent() {
            fs::create_dir_all(parent_dir)?;
        } else {
            warn!("config file at: {:?} has no parent directory", path);
        }

        fs::write(path, data)?;
        Ok(())
    }
}

fn get_default_config() -> Config {
    Config::try_from(&Cfg::with_default_values()).unwrap()
}

//TODO: test override (update size to str:"100" should succeed, value is None should delete)
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::{env, fs};

    use tempfile::NamedTempFile;

    use crate::cfg::{Axe, Cache, Cas, Cfg, Log, User, CURRENT_VERSION, DEFAULT_CACHE_SIZE};
    use crate::level::Level;

    use super::*;

    fn serialize_cfg_to_tmp(test_cfg: &Cfg) -> NamedTempFile {
        let local_cfg = NamedTempFile::new().unwrap();
        let data = toml::to_string(&test_cfg).unwrap();
        fs::write(local_cfg.path(), data).unwrap();
        local_cfg
    }

    #[test]
    fn test_load_config_default() {
        let loader = XetConfigLoader::new("".into(), "".into());
        let cfg = loader.load_config(Level::DEFAULT).unwrap();
        assert_eq!(cfg, Cfg::with_default_values());
    }

    #[test]
    fn test_load_config_env() {
        // TODO: env::set_var will change the env for all tests.
        //       Since tests are run in parallel, this means only
        //       one test can change the env (and Level should be LOCAL for others).
        env::set_var("XET_FOO", "ignore me");
        env::set_var("XET_CAS_SERVER", "localhost:40000");
        env::set_var("XET_log_lEvEl", "debug");
        env::set_var("XET_CACHE_size", "4294967296");
        env::set_var("XET_CACHE_BLOCKSIZE", "12345");
        env::set_var("XET_dev_USER_NAME", "user_dev");
        env::set_var("XET_DEV_ENDPOINT", "xethub.com");
        env::set_var("XET_BLAH", "ignore me");
        env::set_var("XET_ENDPOINT", "notgoogle.com");

        // Now set a ton of others too; this way the randomized order of the hash table
        // actually makes it all work.
        for i in 0..500 {
            env::set_var(format!("XET_BLAH_{i}"), format!("{i}"));
        }

        let loader = XetConfigLoader::new("".into(), "".into());
        let cfg = loader.load_config(Level::ENV).unwrap();
        assert_eq!(cfg.version, CURRENT_VERSION);
        assert_eq!(
            cfg.cas.as_ref().unwrap().server.as_ref().unwrap(),
            "localhost:40000"
        );
        assert!(cfg.cas.as_ref().unwrap().prefix.is_none());
        assert_eq!(cfg.cache.as_ref().unwrap().size.unwrap(), 4_294_967_296);
        assert_eq!(cfg.cache.as_ref().unwrap().blocksize.unwrap(), 12345);
        assert_eq!(cfg.log.as_ref().unwrap().level.as_ref().unwrap(), "debug");
        assert!(cfg.log.as_ref().unwrap().path.is_none());
        let dev_override = cfg.profiles.get("dev").unwrap();
        assert_eq!(
            dev_override.user.as_ref().unwrap().name.as_ref().unwrap(),
            "user_dev"
        );
        assert_eq!(dev_override.endpoint.as_ref().unwrap(), "xethub.com");
    }

    #[test]
    fn test_load_config_from_disk() {
        let test_cfg = Cfg {
            version: CURRENT_VERSION,
            endpoint: None,
            smudge: None,
            cas: Some(Cas {
                prefix: Some("test".to_string()),
                server: Default::default(),
            }),
            cache: Some(Cache {
                path: Default::default(),
                size: Some(4_294_967_296),
                blocksize: Some(12345),
            }),
            log: Some(Log {
                level: Some("debug".to_string()),
                path: Default::default(),
                format: None,
                tracing: None,
            }),
            user: Some(User {
                ssh: Some("mojombo".to_string()),
                https: Some("defunkt".to_string()),
                email: Some("test@test.com".to_string()),
                login_id: None,
                name: Some("hello".to_string()),
                token: Some("atoken".to_string()),
            }),
            axe: Some(Axe {
                enabled: Some("true".to_string()),
                axe_code: Some("123456".to_string()),
            }),
            profiles: HashMap::new(),
        };
        let local_cfg = serialize_cfg_to_tmp(&test_cfg);

        let buf = local_cfg.path().to_path_buf();
        let loader = XetConfigLoader::new(buf, "".into());
        let cfg = loader.load_config(Level::LOCAL).unwrap();
        assert_eq!(cfg, test_cfg);
    }

    #[test]
    fn test_resolve_config_default() {
        let loader = XetConfigLoader::new("".into(), "".into());
        let cfg = loader.resolve_config(Level::LOCAL).unwrap();
        assert_eq!(cfg, Cfg::with_default_values());
    }

    #[test]
    fn test_resolve_config_with_local() {
        let local_cfg = Cfg {
            cas: Some(Cas {
                prefix: Some("test".to_string()),
                ..Default::default()
            }),
            cache: Some(Cache {
                size: Some(100_000_000),
                ..Default::default()
            }),
            ..Default::default()
        };
        let local_cfg_path = serialize_cfg_to_tmp(&local_cfg);
        let loader = XetConfigLoader::new(local_cfg_path.path().to_path_buf(), "".into());
        let cfg = loader.resolve_config(Level::LOCAL).unwrap();
        assert_eq!(cfg.cas.unwrap().prefix, local_cfg.cas.unwrap().prefix);
        assert_eq!(
            cfg.cache.as_ref().unwrap().size,
            local_cfg.cache.as_ref().unwrap().size
        );
        let default_cfg = Cfg::with_default_values();
        assert_eq!(cfg.log.unwrap().level, default_cfg.log.unwrap().level);
        assert_eq!(cfg.cache.unwrap().path, default_cfg.cache.unwrap().path);
    }

    #[test]
    fn test_resolve_config_multiple() {
        let local_cfg = Cfg {
            cas: Some(Cas {
                prefix: Some("".to_string()),
                server: Some("some_server:5020".to_string()),
            }),
            log: Some(Log {
                level: Some("debug".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let local_cfg_path = serialize_cfg_to_tmp(&local_cfg);

        let global_cfg = Cfg {
            cas: Some(Cas {
                prefix: Some("prefix_global".to_string()),
                server: None,
            }),
            log: Some(Log {
                path: Some("/tmp/xet.log".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let global_cfg_path = serialize_cfg_to_tmp(&global_cfg);
        let default_cfg = Cfg::with_default_values();

        let loader = XetConfigLoader::new(
            local_cfg_path.path().to_path_buf(),
            global_cfg_path.path().to_path_buf(),
        );
        let cfg = loader.resolve_config(Level::LOCAL).unwrap();

        assert_eq!(
            cfg.cas.as_ref().unwrap().prefix,
            local_cfg.cas.as_ref().unwrap().prefix
        );
        assert_eq!(
            cfg.cas.as_ref().unwrap().server,
            local_cfg.cas.as_ref().unwrap().server
        );
        assert_eq!(
            cfg.log.as_ref().unwrap().path,
            global_cfg.log.as_ref().unwrap().path
        );
        assert_eq!(
            cfg.log.as_ref().unwrap().level,
            local_cfg.log.as_ref().unwrap().level
        );
        assert_eq!(
            cfg.cache.as_ref().unwrap().size,
            default_cfg.cache.as_ref().unwrap().size
        );
    }

    #[test]
    fn test_load_value() {
        let local_cfg = Cfg {
            cas: Some(Cas {
                server: Some("some_server:5020".to_string()),
                prefix: None,
            }),
            ..Default::default()
        };
        let local_cfg_path = serialize_cfg_to_tmp(&local_cfg);
        let global_cfg = Cfg {
            cas: Some(Cas {
                server: Some("some_global_server:5020".to_string()),
                prefix: Some("global".to_string()),
            }),
            ..Default::default()
        };
        let global_cfg_path = serialize_cfg_to_tmp(&global_cfg);
        let loader = XetConfigLoader::new(
            local_cfg_path.path().to_path_buf(),
            global_cfg_path.path().to_path_buf(),
        );
        let value = loader.load_value(Level::LOCAL, "cas.server").unwrap();
        assert_eq!(value, local_cfg.cas.as_ref().unwrap().server);

        let global_val = loader.load_value(Level::GLOBAL, "cas.server").unwrap();
        assert_eq!(global_val, global_cfg.cas.as_ref().unwrap().server);

        let empty_value = loader.load_value(Level::LOCAL, "cas.prefix").unwrap();
        assert!(empty_value.is_none());

        let unknown_value = loader.load_value(Level::LOCAL, "some.key").unwrap();
        assert!(unknown_value.is_none());
    }

    #[test]
    fn test_invalid_read() {
        let loader = XetConfigLoader::new("".into(), "".into());
        let res = loader.load_value(Level::LOCAL, "cas.");
        assert!(res.is_err());
        let res_invalid_char = loader.load_value(Level::LOCAL, "ca$");
        assert!(res_invalid_char.is_err());
    }

    #[test]
    fn test_resolve_value() {
        let local_cfg = Cfg {
            cas: Some(Cas {
                server: Some("some_local_server:5020".to_string()),
                prefix: None,
            }),
            ..Default::default()
        };
        let local_cfg_path = serialize_cfg_to_tmp(&local_cfg);
        let global_cfg = Cfg {
            cas: Some(Cas {
                server: Some("some_global_server:5020".to_string()),
                prefix: Some("global".to_string()),
            }),
            ..Default::default()
        };
        let global_cfg_path = serialize_cfg_to_tmp(&global_cfg);
        let loader = XetConfigLoader::new(
            local_cfg_path.path().to_path_buf(),
            global_cfg_path.path().to_path_buf(),
        );
        let value = loader.resolve_value(Level::LOCAL, "cas.server").unwrap();
        assert_eq!(value, local_cfg.cas.as_ref().unwrap().server);

        let global_val = loader.resolve_value(Level::LOCAL, "cas.prefix").unwrap();
        assert_eq!(global_val, global_cfg.cas.as_ref().unwrap().prefix);

        let default_val = loader.resolve_value(Level::LOCAL, "cache.size").unwrap();
        assert_eq!(default_val, Some(format!("{}", DEFAULT_CACHE_SIZE)));

        let unknown_val = loader.resolve_value(Level::LOCAL, "some.val").unwrap();
        assert!(unknown_val.is_none());
    }

    #[test]
    fn test_override() {
        let local_cfg = Cfg {
            cas: Some(Cas {
                server: Some("some_server:5020".to_string()),
                prefix: None,
            }),
            ..Default::default()
        };
        let local_cfg_path = serialize_cfg_to_tmp(&local_cfg);
        let loader = XetConfigLoader::new(local_cfg_path.path().to_path_buf(), "".into());
        loader
            .override_value(Level::LOCAL, "cas.server", "other_server:5000")
            .unwrap();
        let data = fs::read(local_cfg_path.path()).unwrap();
        let actual = String::from_utf8_lossy(&data);
        let expected = r#"version = 1

[cas]
server = "other_server:5000"
"#;
        assert_eq!(expected, actual);
    }

    /// Note: git config will serialize a setting that is not part of git's config list.
    #[test]
    fn test_override_non_xet() {
        let local_cfg = Cfg {
            cas: Some(Cas {
                server: Some("some_server2:5020".to_string()),
                prefix: None,
            }),
            ..Default::default()
        };
        let local_cfg_path = serialize_cfg_to_tmp(&local_cfg);
        let loader = XetConfigLoader::new(local_cfg_path.path().to_path_buf(), "".into());
        loader
            .override_value(Level::LOCAL, "some.key", "some_val")
            .unwrap();
        let data = fs::read(local_cfg_path.path()).unwrap();
        let actual = String::from_utf8_lossy(&data);
        let expected = r#"version = 1

[cas]
server = "some_server2:5020"

[some]
version = 1
"#;
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_serialize_new_file() {
        let tmp_cfg_path = NamedTempFile::new().unwrap();
        let tmp_global_cfg_path: PathBuf = "./test_serialize_cfg/xet_cfg.toml".into();
        let loader = XetConfigLoader::new(
            tmp_cfg_path.path().to_path_buf(),
            tmp_global_cfg_path.clone(),
        );
        loader
            .override_value(Level::LOCAL, "cas.prefix", "foo")
            .unwrap();
        let data = fs::read(tmp_cfg_path.path()).unwrap();
        let actual = String::from_utf8_lossy(&data);
        let expected = r#"version = 1

[cas]
prefix = "foo"
"#;
        assert_eq!(expected, actual);

        loader
            .override_value(Level::GLOBAL, "cas.prefix", "foo")
            .unwrap();
        let data_global = fs::read(tmp_global_cfg_path.clone()).unwrap();
        let actual_global = String::from_utf8_lossy(&data_global);
        assert_eq!(expected, actual_global);

        fs::remove_file(tmp_global_cfg_path.clone()).unwrap();
        fs::remove_dir(tmp_global_cfg_path.parent().unwrap()).unwrap();
    }

    #[test]
    fn test_remove() {
        let local_cfg = Cfg {
            cas: Some(Cas {
                server: Some("some_server:5020".to_string()),
                prefix: None,
            }),
            ..Default::default()
        };
        let local_cfg_path = serialize_cfg_to_tmp(&local_cfg);
        let loader = XetConfigLoader::new(local_cfg_path.path().to_path_buf(), "".into());
        loader.remove_value(Level::LOCAL, "cas.server").unwrap();
        let data = fs::read(local_cfg_path.path()).unwrap();
        let actual = String::from_utf8_lossy(&data);
        let expected = r#"version = 1

[cas]
"#;
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_resolve_with_override() {
        let local_cfg = Cfg {
            cas: Some(Cas {
                server: Some("some_server:5020".to_string()),
                prefix: None,
            }),
            ..Default::default()
        };
        let local_cfg_path = serialize_cfg_to_tmp(&local_cfg);
        let loader = XetConfigLoader::new(local_cfg_path.path().to_path_buf(), "".into());

        let override_cfg = Cfg {
            cas: Some(Cas {
                server: None,
                prefix: Some("override_pre".to_string()),
            }),
            cache: Some(Cache {
                size: Some(5020304),
                ..Default::default()
            }),
            ..Default::default()
        };
        let default_cfg = Cfg::with_default_values();

        let resolved_cfg = loader.resolve_config(Level::LOCAL).unwrap();
        let resolved_cfg = resolved_cfg.apply_override(override_cfg.clone()).unwrap();
        assert_eq!(
            resolved_cfg.cas.as_ref().unwrap().server,
            local_cfg.cas.as_ref().unwrap().server
        );
        assert_eq!(
            resolved_cfg.cas.as_ref().unwrap().prefix,
            override_cfg.cas.as_ref().unwrap().prefix
        );
        assert_eq!(
            resolved_cfg.cache.as_ref().unwrap().size,
            override_cfg.cache.as_ref().unwrap().size
        );
        assert_eq!(
            resolved_cfg.cache.as_ref().unwrap().path,
            default_cfg.cache.as_ref().unwrap().path
        );
        assert_eq!(resolved_cfg.log, default_cfg.log);
    }
}
