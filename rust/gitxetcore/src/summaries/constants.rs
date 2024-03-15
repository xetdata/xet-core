/// Only store csv summary for that of size at least 256 KiB.
/// Set to equal "SMALL_FILE_THRESHOLD" so regular users don't see unexpected behavior.
pub const CSV_SUMMARY_SIZE_THRESHOLD_MIN: usize = 256 * 1024;

/// Compute csv summary up to 50 columns.
pub const CSV_SUMMARY_COLUMN_THRESHOLD_MAX: usize = 50;

/// Set this env var to tune "CSV_SUMMARY_SIZE_THRESHOLD_MIN"
pub const CSV_SUMMARY_SIZE_THRESHOLD_MIN_ENV_VAR: &str = "XET_CSV_MIN_SIZE";
