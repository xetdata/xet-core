use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::diff::error::DiffError;
use crate::diff::output::SummaryDiffData;
use crate::diff::output::SummaryDiffData::Csv;
use crate::diff::processor::SummaryDiffProcessor;
use crate::summaries::analysis::FileSummary;
use crate::summaries::csv::CSVSummary;
use crate::summaries::summary_type::SummaryType;

const COLUMN_SIZE_WARNING_THRESHOLD: usize = 50;

/// Diff content for a csv diff
#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct CsvSummaryDiffContent {
    pub before: Option<CSVSummary>,
    pub after: Option<CSVSummary>,
}

/// Processes diffs of CSV Summaries, taking in [CSVSummary]s and producing a [CsvSummaryDiffContent]
/// indicating the delta between them.
///
/// Currently, this will just provide the two summaries (before + after). In the future, we can
/// change this to become more intelligent (actually identify what changed between the columns).
pub struct CsvSummaryDiffProcessor {}

impl CsvSummaryDiffProcessor {
    fn check_summary(&self, summary: &CSVSummary) {
        if summary.headers.len() > COLUMN_SIZE_WARNING_THRESHOLD {
            warn!("Summary has a lot of columns: {}", summary.headers.len())
        }
        if summary.summaries.len() != summary.headers.len() {
            warn!(
                "Summary has unequal numbers of headers ({}) and values ({}).",
                summary.headers.len(),
                summary.summaries.len()
            )
        }
    }
}

impl SummaryDiffProcessor for CsvSummaryDiffProcessor {
    type SummaryData = CSVSummary;

    fn get_type(&self) -> SummaryType {
        SummaryType::Csv
    }

    fn get_version(&self) -> u8 {
        1
    }

    fn get_data<'a>(&'a self, summary: &'a FileSummary) -> Option<&Self::SummaryData> {
        summary.csv.as_ref()
    }

    fn get_insert_diff(&self, summary: &CSVSummary) -> Result<SummaryDiffData, DiffError> {
        self.check_summary(summary);
        Ok(Csv(CsvSummaryDiffContent {
            before: None,
            after: Some(summary.clone()),
        }))
    }

    fn get_remove_diff(&self, summary: &CSVSummary) -> Result<SummaryDiffData, DiffError> {
        self.check_summary(summary);
        Ok(Csv(CsvSummaryDiffContent {
            before: Some(summary.clone()),
            after: None,
        }))
    }

    fn get_diff_impl(
        &self,
        before: &CSVSummary,
        after: &CSVSummary,
    ) -> Result<SummaryDiffData, DiffError> {
        self.check_summary(before);
        self.check_summary(after);
        Ok(Csv(CsvSummaryDiffContent {
            before: Some(before.clone()),
            after: Some(after.clone()),
        }))
    }
}
