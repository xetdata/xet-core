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

/// The maximum number of simultaneous download streams
pub const MAX_CONCURRENT_DOWNLOADS: usize = 16;
/// The maximum number of simultaneous upload streams
pub const MAX_CONCURRENT_UPLOADS: usize = 16;

/// The maximum number of simultaneous streams per prefetch call
pub const MAX_CONCURRENT_PREFETCH_DOWNLOADS: usize = 4;
/// The maximum number of simultaneous prefetches
pub const MAX_CONCURRENT_PREFETCHES: usize = 4;
/// This is the amount to download per prefetch
pub const PREFETCH_WINDOW_SIZE_BYTES: u64 = 32 * 1024 * 1024;
/// Number of historical prefetches to track
pub const PREFETCH_TRACK_COUNT: usize = 32;

/// Number of block derivations to memoize
pub const DERIVE_BLOCKS_CACHE_COUNT: usize = 512;

/// scheme for a local filesystem based CAS server
pub const LOCAL_CAS_SCHEME: &str = "local://";

/// The allowed endupoints usable with xet svc.
pub const XET_ALLOWED_ENDPOINTS: &[&str] = &["xethub.com", "xetsvc.com", "xetbeta.com"];

/// The minimum git version compatible with git xet.
pub const MINIMUM_GIT_VERSION: &str = "2.29";

/// The current version
pub const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Maximum number of entries in the file construction cache
/// which stores File Hash -> reconstruction instructions
pub const FILE_RECONSTRUCTION_CACHE_SIZE: usize = 65536;
