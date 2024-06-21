use crate::diff::output::SummaryDiffData;
use crate::diff::output::SummaryDiffData::Tds;
use crate::diff::{DiffError, SummaryDiffProcessor};
use crate::summaries::{FileSummary, SummaryType};
use std::borrow::Cow;
use tableau_summary::tds::diff::schema::TdsSummaryDiffContent;
use tableau_summary::tds::TdsSummary;
use tableau_summary::twb::diff::util::DiffProducer;

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

    fn get_data<'a>(&'a self, summary: &'a FileSummary) -> Option<Cow<Self::SummaryData>> {
        summary
            .additional_summaries
            .as_ref()
            .and_then(|ext| ext.tds.as_ref())
            .and_then(TdsSummary::from_ref)
    }

    fn get_insert_diff(&self, summary: &TdsSummary) -> Result<SummaryDiffData, DiffError> {
        Ok(Tds(TdsSummaryDiffContent::new_addition(summary)))
    }

    fn get_remove_diff(&self, summary: &TdsSummary) -> Result<SummaryDiffData, DiffError> {
        Ok(Tds(TdsSummaryDiffContent::new_deletion(summary)))
    }

    fn get_diff_impl(
        &self,
        before: &TdsSummary,
        after: &TdsSummary,
    ) -> Result<SummaryDiffData, DiffError> {
        Ok(Tds(TdsSummaryDiffContent::new_diff(before, after)))
    }
}
