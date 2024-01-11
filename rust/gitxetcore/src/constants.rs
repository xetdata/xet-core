pub use common_constants::*;

// TODO: .git is not reliably the git subfolder; need to use the proper version.
pub const CAS_STAGING_SUBDIR: &str = "xet/staging";
pub const GIT_NOTES_MERKLEDB_V1_REF_SUFFIX: &str = "xet/merkledb";
pub const GIT_NOTES_MERKLEDB_V1_REF_NAME: &str = "refs/notes/xet/merkledb";
pub const GIT_NOTES_SUMMARIES_REF_SUFFIX: &str = "xet/summaries";
pub const GIT_NOTES_SUMMARIES_REF_NAME: &str = "refs/notes/xet/summaries";
pub const MERKLEDBV1_PATH_SUBDIR: &str = "xet/merkledb.db";
pub const SUMMARIES_PATH_SUBDIR: &str = "xet/summaries.db";

pub const GIT_NOTES_MERKLEDB_V2_REF_SUFFIX: &str = "xet/merkledbv2";
pub const GIT_NOTES_MERKLEDB_V2_REF_NAME: &str = "refs/notes/xet/merkledbv2";
pub const MERKLEDB_V2_CACHE_PATH_SUBDIR: &str = "xet/merkledbv2-cache";
pub const MERKLEDB_V2_SESSION_PATH_SUBDIR: &str = "xet/merkledbv2-session";

pub const GIT_NOTES_REPO_SALT_REF_SUFFIX: &str = "xet/reposalt";
pub const GIT_NOTES_REPO_SALT_REF_NAME: &str = "refs/notes/xet/reposalt";

pub const GIT_LAZY_CHECKOUT_CONFIG: &str = "xet/lazyconfig";

// This file is checked into the repo.  Path is relative to the repo root.
pub const GIT_REPO_SPECIFIC_CONFIG: &str = ".xet/config.toml";

/// The maximum git filter protocol packet size
pub const GIT_MAX_PACKET_SIZE: usize = 65516;

/// We put a limit on the pointer file size so that
/// we don't ever try to read a whole giant blob into memory when
/// trying to clean. The maximum git packet size is 65516.
/// By setting this threshold to 65515, we can ensure that reading exactly
/// 1 packet is enough to determine if it is a valid pointer file.
pub const POINTER_FILE_LIMIT: usize = GIT_MAX_PACKET_SIZE - 1;

/// If a file has size smaller than this threshold, AND if it "looks-like"
/// text, we interpret this as a text file and passthrough the file, letting
/// git handle it. See `small_file_determination.rs` for details.
///
/// We set this to be 1 less than a constant multiple of the GIT_MAX_PACKET_SIZE
/// so we can read exactly up to that multiple of packets to determine if it
/// is a small file.
pub const SMALL_FILE_THRESHOLD: usize = 4 * GIT_MAX_PACKET_SIZE - 1;

// Salt is 256-bit in length.
pub const REPO_SALT_LEN: usize = 32;
