pub mod bare_repo_commits;
mod clone;
pub mod file_tools;
mod git_process_wrapping;
mod git_repo;
mod git_xet_repo;
pub mod hook_command_entry;
pub mod merkledb_notes;
mod notes_wrapper;
mod path_processing;
pub mod repo_salt;

pub mod git_url;
pub mod git_user_config;
pub mod git_version_checks;

pub use crate::git_integration::git_xet_repo::git_repo_test_tools; // HERE
pub use clone::*;
pub use file_tools::GitTreeListing;
pub use git_process_wrapping::*;
pub use git_repo::GitRepo;
pub use git_xet_repo::GitXetRepo;
pub use notes_wrapper::GitNotesWrapper;
pub use path_processing::*;
