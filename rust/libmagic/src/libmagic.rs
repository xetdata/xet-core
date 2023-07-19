use std::path::Path;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::file_types::get_summary_from_extension;
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct LibmagicSummary {
    pub file_type: String,
    pub file_type_simple: String,
    pub file_type_simple_category: String,
    pub file_type_mime: String,

    // A buffer to allow us to add more to the serialized options
    pub buffer: Option<()>,
}

// Produces a "libmagic" summary (libmagic file type results and heuristics on top of that).
// args.file can be a path to any file within the repo.
// Assumes the _real_ file contents are at the given path, not a pointer file.
// The expected use case is that this utility is called during (immediately after?) smudge.
pub fn print_libmagic_summary(file_path: &Path) -> anyhow::Result<()> {
    let result = summarize_libmagic(file_path)?;
    let content_str = serde_json::to_string_pretty(&result)
        .map_err(|_| anyhow!("Failed to serialize libmagic summary to JSON"))?;
    println!("{content_str}");
    Ok(())
}

// Produces a "libmagic" summary (libmagic file type results and heuristics on top of that).
// args.file can be a path to any file within the repo.
// Assumes the _real_ file contents are at the given path, not a pointer file.
// The expected use case is that this utility is called during (immediately after?) smudge.
pub fn summarize_libmagic(file_path: &Path) -> anyhow::Result<LibmagicSummary> {
    info!("Computing libmagic summary for {:?}", file_path);
    let os_ext = file_path.extension();
    if os_ext.is_some() {
        let ext = os_ext.unwrap().to_str();
        if ext.is_some() {
            let summary = get_summary_from_extension(ext.unwrap());
            return Ok(summary);
        }
    }
    Ok(LibmagicSummary::default())
}