use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use crate::constants::CURRENT_VERSION;
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use std::time::Duration;

use reqwest::{self, ClientBuilder};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, error, info};
use version_compare::{self, Cmp};

const GITHUB_REPO_OWNER: &str = "xetdata";
const GITHUB_REPO_NAME: &str = "xet-tools";
const VERSION_CHECK_FILENAME_HOME: &str = ".xet/version_check_info";

/// The notification time for an update in seconds
const NEW_VERSION_NOTIFICATION_INTERVAL: u64 = 24 * 60 * 60;

/// The notification time for a critical update in seconds
const NEW_CRITICAL_VERSION_NOTIFICATION_INTERVAL: u64 = 2 * 60 * 60;

// The interval in which to compare
const VERSION_CHECK_INTERVAL: u64 = 24 * 60 * 60;

fn version_is_newer(other_version: &str, current_version: &str) -> bool {
    version_compare::compare(other_version, current_version)
        .map_err(|e| {
            error!("Error comparing version strings {other_version} with {current_version}");
            e
        })
        .unwrap_or(Cmp::Lt)
        == Cmp::Gt
}

fn get_critical_text() -> String {
    match std::env::var("_XET_TEST_VERSION_CHECK_CRITICAL_PATTERN") {
        Ok(v) => v,
        Err(_) => "CRITICAL".to_owned(),
    }
}

fn get_version_info_filename() -> PathBuf {
    let version_check_filename = match std::env::var("XET_UPGRADE_CHECK_FILENAME") {
        Ok(v) => v,
        Err(_) => VERSION_CHECK_FILENAME_HOME.to_owned(),
    };

    if let Some(mut path) = dirs::home_dir() {
        path.push(version_check_filename);
        return path;
    }

    if let Some(mut path) = dirs::cache_dir() {
        path.push(version_check_filename);
        return path;
    }

    // Guess it's just the local directory?
    PathBuf::from(version_check_filename)
}

fn get_current_version() -> String {
    match std::env::var("_XET_TEST_VERSION_OVERRIDE") {
        Ok(v) => v,
        Err(_) => CURRENT_VERSION.to_owned(),
    }
}

// Cache the values accross calls.  This is just to reduce the load on the github server when the
// tests here are run locally.

lazy_static! {
    static ref GIT_QUERY_CACHE: Mutex<HashMap<String, String>> = Mutex::new(HashMap::new());
}

pub async fn retrieve_client_release_information(
    remote_repo_name: &str,
    only_latest: bool,
) -> Option<Vec<serde_json::Map<String, serde_json::Value>>> {
    let url = if only_latest {
        format!(
            "https://api.github.com/repos/{GITHUB_REPO_OWNER}/{remote_repo_name}/releases/latest"
        )
    } else {
        format!("https://api.github.com/repos/{GITHUB_REPO_OWNER}/{remote_repo_name}/releases")
    };

    let mut query_table = GIT_QUERY_CACHE.lock().await;

    let release_text;

    if let Some(r_text) = query_table.get(&url) {
        release_text = r_text.clone();
    } else {
        // Actually perform the query

        let mut headers = reqwest::header::HeaderMap::new();

        headers.insert(
            reqwest::header::USER_AGENT,
            "XetData: xet-tools".parse().unwrap(),
        );
        headers.insert(
            reqwest::header::ACCEPT,
            "application/vnd.github.v3+json".parse().unwrap(),
        );

        let client = ClientBuilder::new()
            .timeout(Duration::from_millis(500))
            .build()
            .ok()?;

        // Send the GET request
        let Ok(response) = client.get(&url).headers(headers).send().await.map_err(|e| {
            info!("get_latest_client_release_version: Version check query returned error: {e:?}");
            e
        }) else {
            return None;
        };

        let Ok(q_release_text) = response.text().await.map_err(|e| {
            info!("get_latest_client_release_version: Error getting text for version query: {e:?}");
            e
        }) else {
            return None;
        };

        query_table.insert(url, q_release_text.clone());
        release_text = q_release_text;
    }

    // Slightly different parse depending on whether we're looking at all version or just the latest.
    if only_latest {
        let Ok(release) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&release_text).map_err(|e|
            {
            info!("get_latest_client_release_version: Error parsing github latest version information as json: {e:?}");
            e
            }) else {return None; } ;

        Some(vec![release])
    } else {
        let Ok(releases) = serde_json::from_str::<Vec<serde_json::Map<String, serde_json::Value>>>(&release_text).map_err(|e|
            {
            info!("get_latest_client_release_version: Error parsing github version information as json: {e:?}");
            e
            }) else {return None; } ;

        Some(releases)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VersionCheckInfo {
    latest_version: String,
    contains_critical_fix: bool,
    query_time: DateTime<Utc>,
    inform_time: DateTime<Utc>,

    #[serde(skip)]
    version_check_filename: PathBuf,

    #[serde(skip)]
    local_version: String,

    #[serde(skip)]
    remote_repo_name: String,
}

impl Default for VersionCheckInfo {
    fn default() -> Self {
        Self {
            latest_version: Default::default(),
            contains_critical_fix: false,
            query_time: Default::default(),
            inform_time: DateTime::<Utc>::MIN_UTC,
            version_check_filename: get_version_info_filename(),
            local_version: get_current_version(),
            remote_repo_name: GITHUB_REPO_NAME.to_owned(),
        }
    }
}

impl VersionCheckInfo {
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

    fn load_from_file(version_check_filename: &Path) -> Option<Self> {
        if !version_check_filename.exists() {
            info!("{version_check_filename:?} does not exist; skipping load.");
            return None;
        }

        let Ok(file_contents) = std::fs::read_to_string(version_check_filename).map_err(|e| {
            info!("Error reading version file {version_check_filename:?}: {e:?})");
            e
        }) else {
            return None;
        };

        if let Ok(mut vci) = serde_json::from_str::<VersionCheckInfo>(&file_contents).map_err(|e| {
            info!("Error decoding version file contents from {version_check_filename:?}: {e:?})");
            e
        }) {
            vci.version_check_filename = version_check_filename.to_path_buf();
            info!("Loaded version check information {vci:?}.");
            Some(vci)
        } else {
            info!("Failed to parse version check information.");
            None
        }
    }

    fn save(&self) {
        if let Some(p) = self.version_check_filename.parent() {
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
            let _ = std::fs::write(&self.version_check_filename, json_string).map_err(|e| {
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
            version_check_filename: version_check_filename.to_path_buf(),
            ..Default::default()
        }
    }
    pub async fn load_or_query_impl(mut self) -> Option<Self> {
        if let Some(vci) = Self::load_from_file(&self.version_check_filename) {
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

    pub async fn load_or_query() -> Option<Self> {
        Self::default().load_or_query_impl().await
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

        let Some(mut version_info) = VersionCheckInfo::construct_with_options(
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
        let Some(version_info_2) = VersionCheckInfo::construct_with_options(
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
        let Some(version_info) = VersionCheckInfo::construct_with_options(
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
        let Some(version_info) = VersionCheckInfo::construct_with_options(
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
