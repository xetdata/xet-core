use serde::{Deserialize, Serialize};

/// A class for tracking upstream repository information in order to identify the root
/// of a repo, etc.
///
///
///

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UpstreamXetRepo {
    pub origin_type: String, // Currently, github is the main one supported.
    pub user_name: String,
    pub repo_name: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LocalXetRepoConfig {
    pub upstream: Option<UpstreamXetRepo>,
}
