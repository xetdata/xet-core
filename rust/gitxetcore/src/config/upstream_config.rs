use serde::{Deserialize, Serialize};

/// A class for tracking upstream repository information in order to identify the root
/// of a repo, etc.
///
///
///

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UpstreamXetRepo {
    pub origin_type: String, // Currently, github is the main one supported.
    pub user_name: Option<String>,
    pub repo_name: Option<String>,
    pub url: Option<String>, // If present, this url is prefered.
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LocalXetRepoConfig {
    pub upstream: Option<UpstreamXetRepo>,
}
