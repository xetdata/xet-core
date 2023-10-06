use std::path::{Path, PathBuf};

use crate::constants::CURRENT_VERSION;
use chrono::{DateTime, Utc};
use reqwest::{self, Client};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
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

#[derive(Deserialize)]
struct Release {
    tag_name: String,
    body: String,
}

fn version_is_newer(other_version: &str, current_version: &str) -> bool {
    version_compare::compare(other_version, current_version)
        .map_err(|e| {
            error!("Error comparing version strings {other_version} with {current_version}");
            e
        })
        .unwrap_or(Cmp::Lt)
        == Cmp::Gt
}

fn get_version_info_filename() -> PathBuf {
    let version_check_filename = match std::env::var("XET_VERSION_CHECK_FILENAME") {
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

/// Queries the github releases API to get the latest release of the xet client.
pub async fn get_newer_client_release_versions(
    local_version: &str,
    remote_repo_name: &str,
) -> Option<Vec<(String, String)>> {
    // Build the URL
    let url =
        format!("https://api.github.com/repos/{GITHUB_REPO_OWNER}/{remote_repo_name}/releases");

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::USER_AGENT,
        "XetData: xet-tools".parse().unwrap(),
    );
    headers.insert(
        reqwest::header::ACCEPT,
        "application/vnd.github.v3+json".parse().unwrap(),
    );

    let client = Client::new();
    // Send the GET request
    let Ok(response) = client.get(&url).headers(headers).send().await.map_err(|e|
        {
            info!("get_latest_client_release_version: Version check query returned error: {e:?}");
            e
        }) else { return None; };

    // Ensure the request was successful
    if let Ok(response) = response.error_for_status().map_err(|e| {
        info!("get_latest_client_release_version: Server returned error: {e:?}");
        e
    }) {
        let Ok(releases) = response.json::<Vec<Release>>().await.map_err(|e| {

            info!("Error parsing info for response query as json: {e:?}");
            e
        }) else {return None; } ;

        let mut newer_releases = Vec::new();

        // Check for all the releases that compare newer to this version string.
        for release in releases {
            let version_tag = release.tag_name.clone();

            if version_is_newer(&version_tag, local_version) {
                newer_releases.push((version_tag, release.body));
            } else {
                break;
            }
        }

        Some(newer_releases)
    } else {
        None
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VersionCheckInfo {
    latest_version: String,
    contains_critical_fix: bool,
    query_time: DateTime<Utc>,
    inform_time: Option<DateTime<Utc>>,

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
            inform_time: None,
            version_check_filename: get_version_info_filename(),
            local_version: CURRENT_VERSION.to_owned(),
            remote_repo_name: GITHUB_REPO_NAME.to_owned(),
        }
    }
}

impl VersionCheckInfo {
    fn known_new_release(&self) -> bool {
        version_is_newer(&self.latest_version, &self.local_version)
    }

    pub fn notify_user_if_appropriate(&mut self) {
        if self.known_new_release() {
            let mut notification_happened = false;

            if self.contains_critical_fix {
                if self.inform_age_in_seconds().unwrap_or(u64::MAX)
                    >= NEW_CRITICAL_VERSION_NOTIFICATION_INTERVAL
                {
                    eprintln!("\n\n**CRITICAL:**\nA new version of the Xet client tools, {}, is available at https://github.com/xetdata/xet-tools/releases and contains a critical bug fix from your current version.  It is strongly recommended to upgrade to this release immediately.\n\n",
                              self.latest_version);
                    notification_happened = true;
                }
            } else if self.inform_age_in_seconds().unwrap_or(u64::MAX)
                >= NEW_VERSION_NOTIFICATION_INTERVAL
            {
                eprintln!("\nA new version of the Xet client tools, {}, is available at https://github.com/xetdata/xet-tools/releases.\n",
                          self.latest_version);
                notification_happened = true;
            }

            if notification_happened {
                self.inform_time = Some(Utc::now());
                self.save();
            }
        }
    }

    pub fn query_age_in_seconds(&self) -> u64 {
        self.query_time
            .signed_duration_since(Utc::now())
            .num_seconds()
            .max(0) as u64
    }

    pub fn inform_age_in_seconds(&self) -> Option<u64> {
        self.inform_time
            .map(|t| t.signed_duration_since(Utc::now()).num_seconds().max(0) as u64)
    }

    fn load_from_file(version_check_filename: &Path) -> Option<Self> {
        let Ok(file_contents) = std::fs::read_to_string(version_check_filename).map_err(|e| {
                warn!("Error reading version file {version_check_filename:?}: {e:?})");
                e
            }) else {
                return None;
            };

        if let Ok(mut vci) = serde_json::from_str::<VersionCheckInfo>(&file_contents).map_err(|e| {
            warn!("Error decoding version file contents from {version_check_filename:?}: {e:?})");
            e
        }) {
            vci.version_check_filename = version_check_filename.to_path_buf();
            Some(vci)
        } else {
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
            warn!("Error serializing version info to JSON: {:?}", e);
            e
        }) {
            let _ = std::fs::write(&self.version_check_filename, json_string).map_err(|e| {
                warn!(
                    "Error writing current version info to file to {:?}: {e:?}",
                    self.version_check_filename
                );
                e
            });
        }
    }

    async fn refresh(&mut self) -> bool {
        // OK, go and retrieve the information; the current file is either out of date or so
        let Some(new_versions) = get_newer_client_release_versions(&self.local_version, &self.remote_repo_name).await else {
        // Return now without updating the known version file.
        info!("Error retrieving new release informaition; ignoring version check."); 
        return false;
    };

        let has_new_version = !new_versions.is_empty();

        // Does a newer version exist with a critical bugfix in it?
        self.latest_version = if has_new_version {
            new_versions[0].0.clone()
        } else {
            self.local_version.to_owned()
        };

        self.
               contains_critical_fix = // False if new_version is empty 
        if has_new_version { new_versions
            .iter()
            .any(|(_tag, notes)| notes.contains("CRITICAL")) } else { false };

        self.query_time = Utc::now();

        info!("Successfully perfermed new version check: {self:?}");

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
        #[allow(clippy::never_loop)]
        loop {
            // Go through all the conditions that allow us to skip querying the endpoint.
            // Yes, it means a nested loop, but I argue it's okay here.

            // Does the file exist?
            if !self.version_check_filename.exists() {
                break; // query and refresh
            }

            // Is the file old?
            let Ok(metadata) = std::fs::metadata(&self.version_check_filename).map_err(|e| {
            warn!("Warning: Error loading metadata on {:?}: {e:?}", &self.version_check_filename);
            e
        } ) else {
            break; // query and refresh
            };

            let Ok(modified_time) = metadata.modified() else {
            break; // query and refresh
            };

            if modified_time.elapsed().unwrap_or_default().as_secs() >= VERSION_CHECK_INTERVAL {
                break; // query and refresh
            }

            let Some(vci) = Self::load_from_file(&self.version_check_filename) else {
            break; // query and refresh
            };

            // Now see if the elapsed time since vci.query_time is greater than VERSION_CHECK_INTERVAL
            if vci.query_age_in_seconds() < VERSION_CHECK_INTERVAL {
                return Some(vci);
            } else {
                break; // query and refresh
            }
        }

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

        let Some(mut version_info) = VersionCheckInfo::construct_with_options("v0.0.0", "xet-tools", &version_check_filename).load_or_query_impl().await else {
            panic!("version check returned None when it should have returned version info; internet may not be available.");
        };

        assert!(version_info.known_new_release());

        assert!(version_info.inform_age_in_seconds().is_none());
        version_info.notify_user_if_appropriate();
        assert_lt!(version_info.inform_age_in_seconds().unwrap(), 10);

        // Now, pretend we've upgraded a single version, and see what has happened.  Now it should load it from the file.
        let Some(version_info_2)
            = VersionCheckInfo::construct_with_options(
                "v0.0.0", "xet-tools", &version_check_filename).load_or_query_impl().await else {

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
        let Some(version_info)
             = VersionCheckInfo::construct_with_options(
                    CURRENT_VERSION, "xet-tools", &version_check_filename).load_or_query_impl().await else {

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
        let Some(version_info) = VersionCheckInfo::construct_with_options(CURRENT_VERSION, "xet-tools", &version_check_filename).load_or_query_impl().await else {
            panic!("Last version check returned None when it should have returned version info.");
        };

        assert!(!version_info.known_new_release());
    }
}
