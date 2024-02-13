use std::{fs::FileType, time::UNIX_EPOCH};

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

impl DirEntry {
    pub fn is_dir_or_branch(&self) -> bool {
        matches!(self.object_type.as_str(), "dir" | "branch")
    }

    pub fn from_metadata(path: String, metadata: &std::fs::Metadata) -> Self {
        Self {
            name: path,
            size: metadata.len(),
            object_type: match metadata.file_type() {
                FileType::Directory => "dir".to_owned(),
                FileType::File => "file".to_owned(),
                FileType::Symlink => "link".to_owned(),
            },
            git_hash: "".to_owned(),
            last_modified: metadata.modified().unwrap_or(UNIX_EPOCH).to_string(),
        }
    }
}
