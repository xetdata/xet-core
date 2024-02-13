use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tabled::Tabled;

/// this is the JSON structure returned by the xetea directory listing function
#[derive(Serialize, Deserialize, Tabled, Debug)]
pub struct DirEntry {
    pub name: String,
    pub size: u64,

    /* Decode key:
    "dir" => "directory",
    "blob" => "file",
    "blob+exec" => "file",
    "link" => "symlink",
    "branch" => "branch",
    "repo" => "repo",
    _ => "file",
    */
    #[serde(rename = "type")]
    pub object_type: String,
    #[serde(rename = "githash")]
    #[tabled(skip)]
    pub git_hash: String,
    #[serde(default)]
    #[serde(rename = "lastmodified")]
    pub last_modified: String,
}

fn timestamp_from_systemtime(t: SystemTime) -> String {
    // Convert SystemTime to DateTime<Utc>
    let datetime: DateTime<Utc> = t.into();

    // Format as a UTC ISO 8601 timestamp string
    datetime.to_rfc3339()
}

impl DirEntry {
    pub fn is_dir_or_branch(&self) -> bool {
        matches!(self.object_type.as_str(), "dir" | "branch")
    }

    pub fn from_metadata(path: String, metadata: &std::fs::Metadata) -> Self {
        let ft = metadata.file_type();

        let object_type = {
            if ft.is_dir() {
                "dir".to_owned()
            } else if ft.is_symlink() {
                "link".to_owned()
            } else {
                "file".to_owned()
            }
        };

        let last_modified = timestamp_from_systemtime(metadata.modified().unwrap_or(UNIX_EPOCH));

        Self {
            name: path,
            size: metadata.len(),
            git_hash: "".to_owned(),
            object_type,
            last_modified,
        }
    }
}
