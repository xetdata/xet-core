use crate::config;
use crate::config::ConfigError;
use crate::config::ConfigError::{CachePathNotDir, CachePathReadOnly, InvalidCachePath};
use config::util::{can_write, is_empty};
use std::fs;
use std::path::PathBuf;
use xet_config::Cache;

#[derive(Debug, Clone, Default)]
pub struct CacheSettings {
    pub path: PathBuf,
    pub size: u64,
    pub enabled: bool,
    // TODO: This should not be optional and the default should be applied at config parsing time,
    //       not inferrred by the underlying CacheClient library.
    pub blocksize: Option<u64>,
}

impl TryFrom<Option<&Cache>> for CacheSettings {
    type Error = ConfigError;

    fn try_from(cache: Option<&Cache>) -> Result<Self, Self::Error> {
        fn validate_cache_path(path: &PathBuf) -> Result<(), ConfigError> {
            if !path.exists() {
                fs::create_dir_all(path).map_err(|e| InvalidCachePath(path.clone(), e))?;
            } else if !path.is_dir() {
                return Err(CachePathNotDir(path.clone()));
            } else if !can_write(path) {
                return Err(CachePathReadOnly(path.clone()));
            }
            Ok(())
        }

        Ok(match cache {
            Some(cache) => {
                let mut enabled = true;
                let path = match cache.path.as_ref() {
                    Some(path) if !is_empty(path) => {
                        validate_cache_path(path)?;
                        path.to_path_buf()
                    }
                    _ => {
                        enabled = false;
                        PathBuf::default()
                    }
                };
                let size = match cache.size {
                    Some(size) if size > 0 => size,
                    _ => {
                        enabled = false;
                        0
                    }
                };
                CacheSettings {
                    path,
                    size,
                    enabled,
                    blocksize: cache.blocksize,
                }
            }
            None => CacheSettings::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{NamedTempFile, TempDir};
    use tokio_test::assert_err;

    #[test]
    fn test_parse_cache() {
        let tmpdir = TempDir::new().unwrap();
        let path = tmpdir.path().to_path_buf();
        let cache_cfg = Cache {
            path: Some(path.clone()),
            size: Some(1024000),
            blocksize: Some(1024),
        };

        let cache_settings = CacheSettings::try_from(Some(&cache_cfg)).unwrap();
        assert!(cache_settings.enabled);
        assert_eq!(path, cache_settings.path);
        assert_eq!(1024000, cache_settings.size);
        assert_eq!(Some(1024), cache_settings.blocksize);
    }

    #[test]
    fn test_parse_cache_none() {
        let cache_settings = CacheSettings::try_from(None).unwrap();
        assert!(!cache_settings.enabled);
    }

    #[test]
    fn test_parse_cache_no_size() {
        let tmpdir = TempDir::new().unwrap();
        let path = tmpdir.path().to_path_buf();
        let cache_cfg = Cache {
            path: Some(path),
            size: Some(0),
            blocksize: Some(1024),
        };

        let cache_settings = CacheSettings::try_from(Some(&cache_cfg)).unwrap();
        assert!(!cache_settings.enabled);
    }

    #[test]
    fn test_parse_cache_no_path() {
        let cache_cfg = Cache {
            path: Some(PathBuf::default()),
            size: Some(1024000),
            blocksize: Some(1024),
        };

        let cache_settings = CacheSettings::try_from(Some(&cache_cfg)).unwrap();
        assert!(!cache_settings.enabled);
    }

    #[test]
    fn test_parse_cache_invalid_path() {
        let tmpfile = NamedTempFile::new().unwrap();
        let path = tmpfile.path().to_path_buf();
        let cache_cfg = Cache {
            path: Some(path),
            size: Some(1024000),
            blocksize: Some(1024),
        };

        assert_err!(CacheSettings::try_from(Some(&cache_cfg)));
    }
}
