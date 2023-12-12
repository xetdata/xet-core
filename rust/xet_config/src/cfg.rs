use config::{Config, ConfigError, Map, Source, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::debug;

use serde::{Deserialize, Serialize};

use crate::{console_ser, CfgError};

pub const CURRENT_VERSION: u8 = 1;

pub const DEFAULT_CAS_PREFIX: &str = "default";
// default
pub const PROD_AXE_CODE: &str = "phc_aE643CSQ5F9MrqF8VT1gr7smML8hDU8gzH9lZ4WhdUY";
pub const PROD_CAS_ENDPOINT: &str = "cas-lb.xethub.com:443";

pub const DEFAULT_XET_HOME: &str = ".xet";
pub const DEFAULT_CACHE_PATH_UNDER_HOME: &str = "cache";
pub const DEFAULT_CACHE_SIZE: u64 = 10_737_418_240; // 10GiB

pub const DEFAULT_LOG_LEVEL: &str = "warn";

pub const DEFAULT_AXE_ENABLED: &str = "false";

/// A struct to represent the Config file for git-xet.
///
/// All fields are optional since they aren't required to be set
/// in the file. There are other ways to conditionally write settings
/// to the file (e.g. `skip_serializing_if`), but, we end up conflating
/// whether a setting was "unset" vs "empty".
///
/// As an example, consider the `log.path`. As a user, I have 3 choices
/// for this field in my config:
/// 1. Don't set the field => defer to a lower level of config or default
/// 2. Set the field to the empty string => print logs to the console
/// 3. Set the field to some path => write logs to the path
///
/// Here, whether `log.path` was set or empty makes a difference in how
/// the program executes. Having an Option<PathBuf> instead of PathBuf
/// helps us make this distinction in upper layers.
///
/// Note that with the exception of the `version`, semantically default
/// values are not set via the `Default` trait. The reason is similar to
/// why fields are `Option<T>` instead of `T`. For serialization,
/// we need to know whether a field was originally set or if the value
/// was pulled from the default.
///
/// TODO: We should make fields part of a "default_profile".
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(default)]
pub struct Cfg {
    pub version: u8,
    pub endpoint: Option<String>,
    pub smudge: Option<bool>,
    pub cas: Option<Cas>,
    pub cache: Option<Cache>,
    pub log: Option<Log>,
    pub user: Option<User>,
    pub axe: Option<Axe>,
    /// profiles is a map of different profiles a user might have    
    /// created identified by some key.
    /// Note that `.` characters can't be used as a key since they
    /// conflict with the TOML syntax.
    /// Also note that env overrides for the key (e.g. XET_KEY*)
    /// will be converted to lower-case (i.e. XET_DEV_ENDPOINT will
    /// store a value under the key "dev").
    #[serde(flatten, deserialize_with = "deserialize_profile_map")]
    pub profiles: HashMap<String, Cfg>,
}

use serde::de::{Deserializer, MapAccess, Visitor};
use std::fmt;

fn deserialize_profile_map<'de, D>(deserializer: D) -> Result<HashMap<String, Cfg>, D::Error>
where
    D: Deserializer<'de> + Sized,
{
    struct FilteredMapVisitor;

    impl<'de> Visitor<'de> for FilteredMapVisitor {
        type Value = HashMap<String, Cfg>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a map with Cfg values")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut map = HashMap::with_capacity(access.size_hint().unwrap_or(0));

            while let Ok(Some(key)) = access.next_key::<String>() {
                let value: Result<Cfg, _> = access.next_value();
                match value {
                    Ok(cfg) => {
                        debug!("Cfg deserialze: Found {key} = {cfg:?} (Config), inserting.");

                        map.insert(key, cfg);
                    }
                    Err(_) => {
                        debug!("Cfg deserialize: skipped {key}; value could not be put in a Cfg struct.");
                        // If an error occurs, just skip this pair and continue with the next pair
                        continue;
                    }
                }
            }

            Ok(map)
        }
    }

    deserializer.deserialize_map(FilteredMapVisitor)
}

impl Cfg {
    /// Will construct a new Cfg with semantically default values.
    /// See the struct docs for an explanation of why we are not
    /// spreading this throughout the `Default` trait.
    pub fn with_default_values() -> Self {
        let default_xet_home = dirs::home_dir().unwrap_or_default().join(DEFAULT_XET_HOME);
        let default_cache_path = default_xet_home.join(DEFAULT_CACHE_PATH_UNDER_HOME);

        Self {
            version: CURRENT_VERSION,
            endpoint: None,
            smudge: None,
            cas: Some(Cas {
                server: None,
                prefix: None,
            }),
            cache: Some(Cache {
                path: Some(default_cache_path),
                size: Some(DEFAULT_CACHE_SIZE),
                blocksize: None, // Keeping the default blocksize to None
                                 // instead of a constant to indicate that
                                 // this is something we may tune internally
                                 // and is not necessarily a constant.
            }),
            log: Some(Log {
                path: None,
                level: Some(DEFAULT_LOG_LEVEL.to_string()),
                format: None,
                tracing: None,
                silentsummary: None,
                exceptions: None,
            }),
            user: Some(User {
                ssh: None,
                https: None,
                email: None,
                login_id: None,
                name: None,
                token: None,
            }),
            axe: Some(Axe {
                enabled: Some(DEFAULT_AXE_ENABLED.to_string()),
                axe_code: Some("5454".to_string()),
            }),
            profiles: HashMap::default(), // Default serialization of the flattened map is to return Some empty map
        }
    }

    /// Serializes the Cfg into a string consisting of key-value pairs. Each key is formatted
    /// as a `.` delimited path (e.g. 'log.level' or 'cache.path'). For example:
    ///
    /// ```text
    /// version=1
    /// cache.path=/Users/me/.xet/cache
    /// log.level=warn
    /// ```
    ///
    /// We have this serialization in addition to the standard toml-based serialization since
    /// some workflows (e.g. `git-xet config --list`) require this output format.  
    pub fn to_key_value_string(&self) -> Result<String, CfgError> {
        console_ser::to_string(&self)
    }

    /// Apply overrides to the current config from another config
    pub fn apply_override(&self, othercfg: Cfg) -> Result<Cfg, CfgError> {
        let mut settings = Config::builder();
        settings = settings.add_source(self.clone());
        settings = settings.add_source(othercfg);
        let config = settings.build()?;
        Ok(config.try_deserialize()?)
    }

    /// Loads a config from a file
    pub fn from_file(path: &Path) -> Result<Cfg, CfgError> {
        let mut settings = Config::builder();
        let f = config::File::from(path)
            .required(true)
            .format(config::FileFormat::Toml);
        settings = settings.add_source(f);
        let config = settings.build()?;
        Ok(config.try_deserialize()?)
    }

    /// Writes a config to a file
    pub fn to_file(&self, path: &Path) -> Result<(), CfgError> {
        let data = toml::to_string(self)?;
        if let Some(parent_dir) = path.parent() {
            std::fs::create_dir_all(parent_dir)?;
        }

        std::fs::write(path, data)?;
        Ok(())
    }
}

impl Default for Cfg {
    fn default() -> Self {
        Self {
            version: CURRENT_VERSION,
            endpoint: None,
            smudge: None,
            cas: None,
            cache: None,
            log: None,
            user: None,
            axe: None,
            profiles: HashMap::default(),
        }
    }
}

impl Source for Cfg {
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new((*self).clone())
    }

    fn collect(&self) -> Result<Map<String, Value>, ConfigError> {
        console_ser::to_map(&self).map_err(|e| ConfigError::Message(e.to_string()))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(default)]
pub struct Cas {
    /// address to the CAS server.
    pub server: Option<String>,
    /// Optional prefix for any calls to CAS.
    pub prefix: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
#[serde(default)]
pub struct Cache {
    pub path: Option<PathBuf>,
    /// Size in bytes
    pub size: Option<u64>,
    /// block size in bytes.
    /// blocksize instead of block_size here because the Config environment
    /// parser doesn't seem to like underscores very much, and it doesn't
    /// match XET_CACHE_BLOCK_SIZE correctly.
    pub blocksize: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
#[serde(default)]
pub struct Log {
    pub path: Option<PathBuf>,
    pub level: Option<String>,
    /// Format of the log messages. Options are: `json` or `compact`. Default is `compact`
    pub format: Option<String>,
    /// Some(true) for jaeger trace propagation
    pub tracing: Option<bool>,
    /// Some(true) to silence summary parser warnings
    /// This is not silent_summary as I think that interferes with reading this
    /// from environment variable.
    pub silentsummary: Option<bool>,

    /// Whether or not to log exceptions when they occur.
    pub exceptions: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
#[serde(default)]
pub struct User {
    /// deprecated, but maintained for compatiblity. name/token pair preferred
    pub ssh: Option<String>,
    /// deprecated, but maintained for compatiblity. name/token pair preferred
    pub https: Option<String>,
    pub email: Option<String>,
    pub login_id: Option<String>,
    pub name: Option<String>,
    pub token: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
#[serde(default)]
pub struct Axe {
    pub enabled: Option<String>,
    pub axe_code: Option<String>,
}

#[cfg(test)]
mod serialization_tests {
    use crate::cfg::{Axe, Cache, Cas, Cfg, Log, User, CURRENT_VERSION};
    use std::collections::HashMap;

    #[test]
    fn test_serialization_empty() {
        let cfg = Cfg::default();
        assert_eq!(cfg.version, CURRENT_VERSION);
        assert!(cfg.cas.is_none());
        assert!(cfg.cache.is_none());
        assert!(cfg.log.is_none());
        assert!(cfg.user.is_none());

        let string = toml::to_string(&cfg).unwrap();
        assert_eq!(format!("version = {}\n", CURRENT_VERSION), string)
    }

    #[test]
    fn test_deserialization_empty() {
        let val = "".to_string();
        let cfg: Cfg = toml::from_str(&val).unwrap();
        assert_eq!(cfg.version, CURRENT_VERSION);
        assert!(cfg.cas.is_none());
        assert!(cfg.cache.is_none());
        assert!(cfg.log.is_none());
        assert!(cfg.user.is_none());
    }

    #[test]
    fn test_serde_default() {
        let cfg = Cfg::with_default_values();
        let data = toml::to_string(&cfg).unwrap();
        let cfg_deser: Cfg = toml::from_str(&data).unwrap();
        assert_eq!(cfg, cfg_deser);
    }

    #[test]
    fn test_serialization() {
        let cfg = Cfg {
            version: CURRENT_VERSION,
            endpoint: None,
            smudge: Some(false),
            cas: Some(Cas {
                server: Some("localhost:40000".to_string()),
                prefix: Some("test_prefix".to_string()),
            }),
            cache: Some(Cache {
                path: Some("/tmp/xet.log".into()),
                size: Some(2000),
                blocksize: Some(3000),
            }),
            log: Some(Log {
                path: Some("/tmp/cache".into()),
                level: Some("debug".to_string()),
                format: None,
                tracing: None,
                silentsummary: None,
                exceptions: None,
            }),
            user: Some(User {
                ssh: Some("mojombo".to_string()),
                https: Some("defunkt".to_string()),
                email: None,
                login_id: Some("wef32r32cn2-1".to_string()),
                name: Some("defunkt".to_string()),
                token: Some("123456".to_string()),
            }),
            axe: Some(Axe {
                enabled: Some("true".to_string()),
                axe_code: Some("5454".to_string()),
            }),
            profiles: HashMap::default(),
        };
        let toml_string = toml::to_string(&cfg).unwrap();
        let expected = r#"version = 1
smudge = false

[cas]
server = "localhost:40000"
prefix = "test_prefix"

[cache]
path = "/tmp/xet.log"
size = 2000
blocksize = 3000

[log]
path = "/tmp/cache"
level = "debug"

[user]
ssh = "mojombo"
https = "defunkt"
login_id = "wef32r32cn2-1"
name = "defunkt"
token = "123456"

[axe]
enabled = "true"
axe_code = "5454"
"#;
        assert_eq!(expected, toml_string);
    }

    #[test]
    fn test_serialize_with_profile() {
        let cfg = Cfg {
            version: CURRENT_VERSION,
            endpoint: None,
            smudge: Some(false),
            cas: None,
            cache: None,
            log: None,
            user: Some(User {
                name: Some("defunkt".to_string()),
                token: Some("123456".to_string()),
                ..Default::default()
            }),
            axe: None,
            profiles: HashMap::from([(
                "dev".to_string(),
                Cfg {
                    endpoint: Some("xethub.com".to_string()),
                    user: Some(User {
                        name: Some("other_name".to_string()),
                        token: Some("abc123".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )]),
        };
        let toml_string = toml::to_string(&cfg).unwrap();
        let expected = r#"version = 1
smudge = false

[user]
name = "defunkt"
token = "123456"

[dev]
version = 1
endpoint = "xethub.com"

[dev.user]
name = "other_name"
token = "abc123"
"#;
        assert_eq!(expected, toml_string);
    }

    #[test]
    fn test_deserialize_with_profile() {
        let cfg = Cfg {
            version: CURRENT_VERSION,
            endpoint: None,
            smudge: Some(false),
            cas: None,
            cache: None,
            log: None,
            user: Some(User {
                name: Some("defunkt".to_string()),
                token: Some("123456".to_string()),
                ..Default::default()
            }),
            axe: None,
            profiles: HashMap::from([(
                "dev".to_string(),
                Cfg {
                    endpoint: Some("xetbeta.com".to_string()),
                    user: Some(User {
                        name: Some("other_name".to_string()),
                        token: Some("abc123".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )]),
        };
        let data = r#"version = 1
smudge = false

[user]
name = "defunkt"
token = "123456"

[dev]
version = 1
endpoint = "xetbeta.com"

[dev.user]
name = "other_name"
token = "abc123"
"#;
        let c2: Cfg = toml::from_str(data).unwrap();
        assert_eq!(cfg, c2);
    }

    #[test]
    fn test_deserialization() {
        let cfg = Cfg {
            version: CURRENT_VERSION,
            endpoint: None,
            smudge: Some(true),
            cas: Some(Cas {
                server: Some("localhost:40000".to_string()),
                prefix: Some("test_prefix2".to_string()),
            }),
            cache: Some(Cache {
                path: Some("/tmp/xet.log".into()),
                size: Some(4356),
                blocksize: None,
            }),
            log: Some(Log {
                path: Some("/tmp/cache".into()),
                level: Some("debug".to_string()),
                format: None,
                tracing: None,
                silentsummary: None,
                exceptions: None,
            }),
            user: Some(User {
                ssh: Some("mojombo".to_string()),
                https: Some("defunkt".to_string()),
                email: Some("test@test.com".to_string()),
                login_id: Some("wef32r32cn2-1".to_string()),
                name: Some("defunkt".to_string()),
                token: Some("1234".to_string()),
            }),
            axe: Some(Axe {
                enabled: Some("true".to_string()),
                axe_code: Some("5454".to_string()),
            }),
            profiles: HashMap::default(),
        };
        let data = r#"version = 1
smudge = true

[cas]
server = "localhost:40000"
prefix = "test_prefix2"

[cache]
path = "/tmp/xet.log"
size = 4356

[log]
path = "/tmp/cache"
level = "debug"

[user]
ssh = "mojombo"
https = "defunkt"
email = "test@test.com"
login_id = "wef32r32cn2-1"
name = "defunkt"
token= "1234"

[axe]
enabled = "true"
axe_code = "5454"
"#;
        let c2: Cfg = toml::from_str(data).unwrap();
        assert_eq!(cfg, c2)
    }

    #[test]
    fn test_serde_set_vs_empty() {
        let data = r#"version = 1

[cas]
prefix = ""
"#;
        let c: Cfg = toml::from_str(data).unwrap();
        assert_eq!(c.cas.as_ref().unwrap().prefix, Some("".to_string()));
        let data_ser = toml::to_string(&c).unwrap();
        assert_eq!(data_ser.as_str(), data);
    }

    /// extra fields will be ommitted
    #[test]
    fn test_deserialize_extra_fields() {
        let data = r#"verson = 3
[cache]
pth = "localhost"
"#;
        let c: Cfg = toml::from_str(data).unwrap();
        assert_eq!(c.version, CURRENT_VERSION);
        assert!(c.cache.as_ref().unwrap().path.is_none());
        assert!(c.cache.as_ref().unwrap().size.is_none());
    }

    #[test]
    fn test_bidir_deserialization_with_profile() {
        let mut cfg_root = Cfg {
            version: CURRENT_VERSION,
            endpoint: None,
            smudge: Some(true),
            cas: Some(Cas {
                server: Some("localhost:40000".to_string()),
                prefix: Some("test_prefix2".to_string()),
            }),
            cache: Some(Cache {
                path: Some("/tmp/xet.log".into()),
                size: Some(4356),
                blocksize: None,
            }),
            log: Some(Log {
                path: Some("/tmp/cache".into()),
                level: Some("debug".to_string()),
                format: None,
                tracing: None,
                silentsummary: None,
                exceptions: None,
            }),
            user: Some(User {
                ssh: Some("mojombo".to_string()),
                https: Some("defunkt".to_string()),
                email: Some("test@test.com".to_string()),
                login_id: Some("wef32r32cn2-1".to_string()),
                name: Some("defunkt".to_string()),
                token: Some("1234".to_string()),
            }),
            axe: Some(Axe {
                enabled: Some("true".to_string()),
                axe_code: Some("5454".to_string()),
            }),
            profiles: HashMap::default(),
        };

        let cfg_dev = Cfg {
            version: CURRENT_VERSION,
            endpoint: Some("xethub.com".to_string()),
            smudge: None,
            cas: None,
            cache: None,
            log: Some(Log {
                path: Some("/tmp/cache2".into()),
                level: Some("debug".to_string()),
                format: None,
                tracing: None,
                silentsummary: None,
                exceptions: None,
            }),
            user: Some(User {
                ssh: None,
                https: Some("pika".to_string()),
                email: Some("test@test.com".to_string()),
                login_id: None,
                name: Some("pika".to_string()),
                token: Some("mooof".to_string()),
            }),
            axe: Some(Axe {
                enabled: Some("false".to_string()),
                axe_code: Some("5454".to_string()),
            }),
            profiles: HashMap::default(),
        };
        let mut profiles = HashMap::new();
        profiles.insert("dev".to_string(), cfg_dev);
        cfg_root.profiles = profiles;
        let data = r#"version = 1
smudge = true

[cas]
server = "localhost:40000"
prefix = "test_prefix2"

[cache]
path = "/tmp/xet.log"
size = 4356

[log]
path = "/tmp/cache"
level = "debug"

[user]
ssh = "mojombo"
https = "defunkt"
email = "test@test.com"
login_id = "wef32r32cn2-1"
name = "defunkt"
token = "1234"

[axe]
enabled = "true"
axe_code = "5454"

[dev]
version = 1
endpoint = "xethub.com"

[dev.log]
path = "/tmp/cache2"
level = "debug"

[dev.user]
https = "pika"
email = "test@test.com"
name = "pika"
token = "mooof"

[dev.axe]
enabled = "false"
axe_code = "5454"
"#;
        let c2: Cfg = toml::from_str(data).unwrap();
        assert_eq!(cfg_root, c2);

        let data_ser = toml::to_string(&cfg_root).unwrap();
        assert_eq!(data_ser.as_str(), data);
    }
}
