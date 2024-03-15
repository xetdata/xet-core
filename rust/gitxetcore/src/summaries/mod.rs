pub mod analysis;
pub mod csv;
pub mod summary_type;

mod constants;
mod summaries_plumb;

pub use analysis::{FileAnalyzers, FileSummary};
pub use csv::{summarize_csv_from_reader, CSVAnalyzer};
pub use libmagic::libmagic;
pub use summaries_plumb::*;
pub use summary_type::SummaryType;
pub use WholeRepoSummary;
