pub mod analysis;
pub mod csv;
pub mod summary_type;
pub use libmagic::libmagic;
mod summaries_plumb;

pub use analysis::{FileAnalyzers, FileSummary};
pub use csv::{summarize_csv_from_reader, CSVAnalyzer};
pub use summaries_plumb::*;
pub use summary_type::SummaryType;
pub use WholeRepoSummary;
