use crate::config::permission::Permission;
use crate::errors::Result;
use chrono::{DateTime, Utc};
use error_printer::ErrorPrinter;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct QueryValuesImpl {
    query_time: DateTime<Utc>,
    value: String,
}

impl QueryValuesImpl {
    fn query_age_in_seconds(&self) -> u64 {
        Utc::now()
            .signed_duration_since(self.query_time)
            .num_seconds()
            .max(0) as u64
    }
}

pub struct CachedQueryWrapper {
    file_name: PathBuf,
    query_value: Option<QueryValuesImpl>,
    max_valid_seconds: u64,
}

impl CachedQueryWrapper {
    /// Cache a value with a timeout for persistance between runs of the program.
    ///
    /// cache_dir: the directory where the cache should live (config.xet_home is good).
    /// key_name: the name for this query.
    /// max_valid_seconds: The maximum number of seconds for which to load and return an existing value.
    ///
    /// Usage:
    ///
    ///   let mut query_cache = CachedQueryWrapper::new(...);
    ///
    ///   let value = {
    ///      if let Some(value) = query_cache.has_valid_value() {
    ///         value
    ///      } else {
    ///         let value = query_for_value(...); // get the value using an expensive operation
    ///         query_cache.set_value(value.clone());
    ///         value
    ///      }
    ///   };
    ///
    ///
    pub fn new(
        cache_dir: impl AsRef<Path>,
        key_name: &str,
        max_valid_seconds: u64,
    ) -> Result<Self> {
        let file_name = cache_dir.as_ref().join("query_cache").join(key_name);
        Permission::current().create_dir_all(file_name.parent().unwrap())?;

        Ok(Self {
            query_value: None,
            file_name,
            max_valid_seconds,
        })
    }

    /// Returns the value if it has been set or is cached and still valid.
    pub fn get(&mut self) -> Option<String> {
        if let Some(qv) = self.query_value.as_ref() {
            return Some(qv.value.clone());
        }

        let qv = self.attempt_load_from_file()?;

        if qv.query_age_in_seconds() < self.max_valid_seconds {
            let ret = Some(qv.value.clone());
            self.query_value = Some(qv);
            ret
        } else {
            info!(
                "Cached query age for {:?} too old, discarding.",
                self.file_name
            );
            None
        }
    }

    fn attempt_load_from_file(&self) -> Option<QueryValuesImpl> {
        // if file doesn't exist return None

        if !self.file_name.exists() {
            debug!("{:?} does not exist; skipping load.", &self.file_name);
            return None;
        }

        let Ok(file_contents) = std::fs::read_to_string(&self.file_name)
            .debug_error(format!("Error reading version file {:?}", &self.file_name))
        else {
            return None;
        };

        let Ok(qv) = serde_json::from_str::<QueryValuesImpl>(&file_contents).info_error(format!(
            "Error decoding version file contents from {:?}",
            self.file_name
        )) else {
            debug!(
                "Failed to parse query check information from {:?}.",
                self.file_name
            );
            return None;
        };

        info!(
            "Loaded cached query information {qv:?} from {:?}",
            &self.file_name
        );

        Some(qv)
    }

    /// Verifies that the
    pub fn set(&mut self, value: String) -> Result<()> {
        let qv = QueryValuesImpl {
            value,
            query_time: Utc::now(),
        };

        // Now, save out the information here.
        if let Ok(json_string) = serde_json::to_string_pretty(&qv).map_err(|e| {
            debug!("Error serializing query values to JSON: {:?}", e);
            e
        }) {
            debug!(
                "CachedQueryInfo:save: Serializing cached query struct {qv:?} to {:?}",
                &self.file_name
            );

            let _ = std::fs::write(&self.file_name, json_string).map_err(|e| {
                debug!(
                    "QueryCacheError:save: Error writing current version info to file to {:?}: {e:?}",
                    &self.file_name
                );
                e
            });
        }

        self.query_value = Some(qv);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_set_and_get_value() -> Result<()> {
        let temp_dir = tempdir()?;
        let cache_dir = temp_dir.path().join("cache");
        let key_name = "test_key";
        let max_valid_seconds = 60;

        let mut query_cache = CachedQueryWrapper::new(&cache_dir, key_name, max_valid_seconds)?;
        let test_value = "test_value".to_string();

        query_cache.set(test_value.clone())?;

        // Create a new instance to force load from disk
        let mut query_cache = CachedQueryWrapper::new(&cache_dir, key_name, max_valid_seconds)?;
        let cached_value = query_cache.get();

        assert_eq!(cached_value, Some(test_value));

        Ok(())
    }

    #[test]
    fn test_force_load_from_cache() -> Result<()> {
        let temp_dir = tempdir()?;
        let cache_dir = temp_dir.path().join("cache");
        let key_name = "test_key";
        let max_valid_seconds = 0; // Force load from cache, but expired

        let mut query_cache = CachedQueryWrapper::new(&cache_dir, key_name, max_valid_seconds)?;
        let test_value = "test_value".to_string();

        query_cache.set(test_value.clone())?;

        // Create a new instance to force load from disk
        let mut query_cache = CachedQueryWrapper::new(&cache_dir, key_name, max_valid_seconds)?;
        let cached_value = query_cache.get();
        assert_eq!(cached_value, None);

        Ok(())
    }

    #[test]
    fn test_get_value_from_empty_cache() -> Result<()> {
        let temp_dir = tempdir()?;
        let cache_dir = temp_dir.path().join("cache");
        let key_name = "test_key";
        let max_valid_seconds = 60;

        let mut query_cache = CachedQueryWrapper::new(cache_dir, key_name, max_valid_seconds)?;
        let cached_value = query_cache.get();
        assert_eq!(cached_value, None);

        Ok(())
    }
}
