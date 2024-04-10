use serde::{Deserialize, Serialize};
use tableau_summary::tds::TdsSummary;
use crate::diff::output::SummaryDiffData;
use crate::diff::{DiffError, SummaryDiffProcessor};
use crate::diff::output::SummaryDiffData::Tds;
use crate::summaries::{FileSummary, SummaryType};

/// Diff content for a Tds diff
#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TdsSummaryDiffContent {
    pub before: Option<TdsSummary>,
    pub after: Option<TdsSummary>,
}

/// Processes diffs of Tds Summaries, taking in [TdsSummary]s and producing a [TdsSummaryDiffContent]
/// indicating the delta between them.
///
/// Currently, this will just provide the two summaries (before + after). In the future, we can
/// change this to become more intelligent (actually identify what changed between the datasources).
pub struct TdsSummaryDiffProcessor {}


impl SummaryDiffProcessor for TdsSummaryDiffProcessor {
    type SummaryData = TdsSummary;

    fn get_type(&self) -> SummaryType {
        SummaryType::Tds
    }

    fn get_version(&self) -> u8 {
        1
    }

    fn get_data<'a>(&'a self, summary: &'a FileSummary) -> Option<&Self::SummaryData> {
        summary.additional_summaries.as_ref()
            .and_then(|ext|ext.tds.as_ref())
    }

    fn get_insert_diff(&self, summary: &TdsSummary) -> Result<SummaryDiffData, DiffError> {
        Ok(Tds(TdsSummaryDiffContent {
            before: None,
            after: Some(summary.clone()),
        }))
    }

    fn get_remove_diff(&self, summary: &TdsSummary) -> Result<SummaryDiffData, DiffError> {
        Ok(Tds(TdsSummaryDiffContent {
            before: Some(summary.clone()),
            after: None,
        }))
    }

    fn get_diff_impl(
        &self,
        before: &TdsSummary,
        after: &TdsSummary,
    ) -> Result<SummaryDiffData, DiffError> {
        Ok(Tds(TdsSummaryDiffContent {
            before: Some(before.clone()),
            after: Some(after.clone()),
        }))
    }
}
