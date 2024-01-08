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

/// A program tag name for reporting things:
pub const XET_PROGRAM_NAME: &str = concat!("Git-Xet ", env!("CARGO_PKG_VERSION"));

/// Maximum number of entries in the file construction cache
/// which stores File Hash -> reconstruction instructions
pub const FILE_RECONSTRUCTION_CACHE_SIZE: usize = 65536;
