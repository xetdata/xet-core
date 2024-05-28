use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::format::Debug;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::sync::Mutex;
use tracing::{debug, error, info};

#[derive(Debug, Serialize, Deserialize)]
struct QueryValuesImpl {
    query_time: DateTime<Utc>,

    #[serde(flatten)]
    values: HashMap<String, String>,
}

pub struct CachedQueryInstance<E: Debug> {
    file_name: PathBuf,
    query_values: Option<QueryValueImpl>,

    needs_refresh: Box(dyn Fn(&DateTime<Utc>, &HashMap<String, String>) -> bool),
    refresh_function: Box(dyn Future<Output = Result<HashMap<String, String>, E>>),
}

impl CachedQueryInstance {
    pub fn new(

        
        Self {
            latest_version: Default::default(),
            contains_critical_fix: false,
            query_time: Default::default(),
            inform_time: DateTime::<Utc>::MIN_UTC,
            version_check_filename: get_version_info_filename(config),
            local_version: get_current_version(),
            remote_repo_name: GITHUB_REPO_NAME.to_owned(),
        }
    }

    fn known_new_release(&self) -> bool {
        version_is_newer(&self.latest_version, &self.local_version)
    }

    pub fn notify_user_if_appropriate(&mut self) {
        debug!("VersionCheckInfo:notify_user_if_appropriate: version check info = {self:?}");
        if self.known_new_release()
            && std::env::var_os("XET_DISABLE_VERSION_CHECK").unwrap_or("0".into()) == "0"
        {
            let mut notification_happened = false;

            if self.contains_critical_fix {
                if self.inform_age_in_seconds() >= NEW_CRITICAL_VERSION_NOTIFICATION_INTERVAL {
                    eprintln!("\n\n**CRITICAL:**\nA new version of the Xet client tools, {}, is available at https://github.com/xetdata/xet-tools/releases and contains a critical bug fix from your current version.  It is strongly recommended to upgrade to this release immediately.\n\n",
                              self.latest_version);
                    notification_happened = true;
                }
            } else if self.inform_age_in_seconds() >= NEW_VERSION_NOTIFICATION_INTERVAL {
                eprintln!("\nA new version of the Xet client tools, {}, is available at https://github.com/xetdata/xet-tools/releases.\n",
                          self.latest_version);
                notification_happened = true;
            }

            if notification_happened {
                self.inform_time = Utc::now();
                self.save();
            }
        }
    }

    pub fn query_age_in_seconds(&self) -> u64 {
        Utc::now()
            .signed_duration_since(self.query_time)
            .num_seconds()
            .max(0) as u64
    }

    pub fn inform_age_in_seconds(&self) -> u64 {
        Utc::now()
            .signed_duration_since(self.inform_time)
            .num_seconds()
            .max(0) as u64
    }

    fn load_from_file(version_check_filename: Option<&PathBuf>) -> Option<Self> {
        // if file doesn't exist return None
        version_check_filename?;

        let version_check_filename = version_check_filename.unwrap();

        if !version_check_filename.exists() {
            info!("{version_check_filename:?} does not exist; skipping load.");
            return None;
        }

        let Ok(file_contents) = std::fs::read_to_string(version_check_filename).info_error(
            format!("Error reading version file {version_check_filename:?}"),
        ) else {
            return None;
        };

        if let Ok(mut vci) = serde_json::from_str::<CachedQueryInstance>(&file_contents).info_error(
            format!("Error decoding version file contents from {version_check_filename:?}"),
        ) {
            vci.version_check_filename = Some(version_check_filename.to_owned());
            info!("Loaded version check information {vci:?}.");
            Some(vci)
        } else {
            info!("Failed to parse version check information.");
            None
        }
    }

    fn save(&self) {
        if self.version_check_filename.is_none() {
            return;
        }

        if let Some(p) = self.version_check_filename.as_ref().unwrap().parent() {
            if !p.exists() {
                let _ = std::fs::create_dir_all(p);
            }
        }

        // Now, save out the information here.
        if let Ok(json_string) = serde_json::to_string_pretty(&self).map_err(|e| {
            info!("Error serializing version info to JSON: {:?}", e);
            e
        }) {
            info!(
                "VersionCheckInfo:save: Serializing upgrade check struct {self:?} to {:?}",
                &self.version_check_filename
            );
            let _ = std::fs::write(self.version_check_filename.as_ref().unwrap(), json_string).map_err(|e| {
                info!(
                    "VersionCheckInfo:save: Error writing current version info to file to {:?}: {e:?}",
                    self.version_check_filename
                );
                e
            });
        }
    }

    async fn refresh(&mut self) -> bool {
        debug!("VersionCheckInfo:refresh: Retrieving client information.");
        let Some(latest_release) =
            retrieve_client_release_information(&self.remote_repo_name, true).await
        else {
            info!("VersionCheckInfo:refresh: Error retrieving new release information; skipping upgrade version check.");
            return false;
        };

        let Some(latest_version_v) = latest_release[0].get("tag_name") else {
            info!("VersionCheckInformation: refresh: Error retrieving tag_name.");
            debug!("VersionCheckInformation: refresh: Error retrieving tag_name: content of latest release = {:?}", &latest_release[0]);
            return false;
        };

        // Use to_string so that it won't crash if it gets parsed as a float or something.
        self.latest_version = latest_version_v.to_string().trim_matches('"').to_string();
        info!(
            "VersionCheckInfo::refresh: latest version available = {}",
            self.latest_version
        );

        if version_is_newer(&self.latest_version, &self.local_version) {
            // If we have a new version, we need to refresh all the version to make sure
            // there isn't a critical release out there that we've missed.

            let Some(all_releases) =
                retrieve_client_release_information(&self.remote_repo_name, false).await
            else {
                info!("VersionCheckInformation: refresh: Error retrieving full releases information; skipping upgrade version check.");
                return false;
            };

            let critical_text = get_critical_text();
            info!("VersionCheckInformation: refresh: Searching for text '{critical_text}' to set critical flag.");

            self.contains_critical_fix = false;

            for release in all_releases {
                let Some(serde_json::Value::String(version)) = release.get("tag_name") else {
                    info!("VersionCheckInfo:refresh: Error extracting tag_name from release entry as string.");
                    continue;
                };

                if !version_is_newer(version, &self.local_version) {
                    // Skip any versions this one or later as they are in descending order;
                    // no need to worry about them.
                    break;
                }

                let Some(serde_json::Value::String(body)) = release.get("body") else {
                    info!("VersionCheckInformation: refresh: Error parsing release notes in release as string.");
                    continue;
                };

                if body.contains(&critical_text) {
                    info!("VersionCheckInformation: refresh: newer release {version} contains a critical fix.");
                    self.contains_critical_fix = true;
                    break;
                }
            }
        } else {
            self.contains_critical_fix = false;
        }

        info!(
            "VersionCheckInformation: refresh: Successfully perfermed new version check: {self:?}"
        );

        // Update
        self.query_time = Utc::now();

        true
    }

    /// For testing
    pub fn construct_with_options(
        local_version: &str,
        remote_repo_name: &str,
        version_check_filename: &Path,
    ) -> Self {
        Self {
            local_version: local_version.to_owned(),
            remote_repo_name: remote_repo_name.to_owned(),
            version_check_filename: Some(version_check_filename.to_path_buf()),
            ..Default::default()
        }
    }
    pub async fn load_or_query_impl(mut self) -> Option<Self> {
        if let Some(vci) = Self::load_from_file(self.version_check_filename.as_ref()) {
            let query_age = vci.query_age_in_seconds();
            if query_age < VERSION_CHECK_INTERVAL {
                info!(
                    "VersionCheckInfo:load_or_query: Query age of cache is {query_age} seconds, shorter than {VERSION_CHECK_INTERVAL}, using cached information."
                );
                // Pull the relevant information from the loaded file
                self.latest_version = vci.latest_version;
                self.contains_critical_fix = vci.contains_critical_fix;
                self.query_time = vci.query_time;
                self.inform_time = vci.inform_time;

                return Some(self);
            } else {
                info!(
                    "VersionCheckInfo:load_or_query: Query age = {query_age} seconds, longer than {VERSION_CHECK_INTERVAL}, refreshing."
                );
            }
        }

        // Now, refresh the local results; save if there are changes.
        if self.refresh().await {
            self.save();
            Some(self)
        } else {
            None
        }
    }

    pub async fn load_or_query(config: XetConfig) -> Option<Self> {
        Self::new(&config).load_or_query_impl().await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::constants::CURRENT_VERSION;
    use more_asserts::*;

    #[tokio::test]
    async fn test_retrieve_new_version_with_upgrade() {
        // Create a temp directory
        let tmp_dir = tempfile::tempdir().unwrap();
        let version_check_filename = tmp_dir.path().join("version_check_filename");

        let Some(mut version_info) = CachedQueryInstance::construct_with_options(
            "v0.0.0",
            "xet-tools",
            &version_check_filename,
        )
        .load_or_query_impl()
        .await
        else {
            eprintln!("WARNING: version check returned None when it should have returned version info; internet may not be available.");
            return;
        };

        assert!(version_info.known_new_release());

        assert!(version_info.inform_age_in_seconds() >= 1000000);
        version_info.notify_user_if_appropriate();
        assert_lt!(version_info.inform_age_in_seconds(), 10);

        // Now, pretend we've upgraded a single version, and see what has happened.  Now it should load it from the file.
        let Some(version_info_2) = CachedQueryInstance::construct_with_options(
            "v0.0.0",
            "xet-tools",
            &version_check_filename,
        )
        .load_or_query_impl()
        .await
        else {
            panic!("version check 2 returned None when it should have returned version info.");
        };

        assert_eq!(version_info.latest_version, version_info_2.latest_version);
        assert_eq!(version_info.query_time, version_info_2.query_time);
        assert_eq!(
            version_info.contains_critical_fix,
            version_info_2.contains_critical_fix
        );
        assert_eq!(version_info.inform_time, version_info_2.inform_time);

        // Now, pretend the user has upgraded.
        let Some(version_info) = CachedQueryInstance::construct_with_options(
            CURRENT_VERSION,
            "xet-tools",
            &version_check_filename,
        )
        .load_or_query_impl()
        .await
        else {
            panic!("Last version check returned None when it should have returned version info.");
        };

        assert!(!version_info.known_new_release());
        assert_lt!(version_info.query_age_in_seconds(), 10);
    }

    #[tokio::test]
    async fn test_retrieve_new_version_no_upgrade() {
        // Create a temp directory
        let tmp_dir = tempfile::tempdir().unwrap();
        let version_check_filename = tmp_dir.path().join("version_check_filename");

        // Now, query when there's no results
        let Some(version_info) = CachedQueryInstance::construct_with_options(
            CURRENT_VERSION,
            "xet-tools",
            &version_check_filename,
        )
        .load_or_query_impl()
        .await
        else {
            eprintln!("WARNING: version check returned None when it should have returned version info; internet may not be available.");
            return;
        };

        assert!(!version_info.known_new_release());
    }
}
