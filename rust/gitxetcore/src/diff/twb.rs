use std::borrow::Cow;
use tableau_summary::twb::diff::schema::TwbSummaryDiffContent;
use tableau_summary::twb::diff::util::DiffProducer;
use tableau_summary::twb::TwbSummary;

use crate::diff::output::SummaryDiffData;
use crate::diff::output::SummaryDiffData::Twb;
use crate::diff::{DiffError, SummaryDiffProcessor};
use crate::summaries::{FileSummary, SummaryType};

/// Processes diffs of Twb Summaries, taking in [TwbSummary]s and producing a [TwbSummaryDiffContent]
/// indicating the delta between them.
///
/// Currently, this will just provide the two summaries (before + after). In the future, we can
/// change this to become more intelligent (actually identify what changed between the workbooks).
pub struct TwbSummaryDiffProcessor {}

impl SummaryDiffProcessor for TwbSummaryDiffProcessor {
    type SummaryData = TwbSummary;

    fn get_type(&self) -> SummaryType {
        SummaryType::Twb
    }

    fn get_version(&self) -> u8 {
        1
    }

    fn get_data<'a>(&'a self, summary: &'a FileSummary) -> Option<Cow<Self::SummaryData>> {
        summary
            .additional_summaries
            .as_ref()
            .and_then(|ext| ext.twb.as_ref())
            .and_then(TwbSummary::from_ref)
    }

    fn get_insert_diff(&self, summary: &TwbSummary) -> Result<SummaryDiffData, DiffError> {
        Ok(Twb(TwbSummaryDiffContent::new_addition(summary)))
    }

    fn get_remove_diff(&self, summary: &TwbSummary) -> Result<SummaryDiffData, DiffError> {
        Ok(Twb(TwbSummaryDiffContent::new_deletion(summary)))
    }

    fn get_diff_impl(
        &self,
        before: &TwbSummary,
        after: &TwbSummary,
    ) -> Result<SummaryDiffData, DiffError> {
        Ok(Twb(TwbSummaryDiffContent::new_diff(before, after)))
    }
}
