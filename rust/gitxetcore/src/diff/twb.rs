use tableau_summary::twb::diff::{TwbDiffProcessor};
use tableau_summary::twb::TwbSummary;
use crate::diff::output::SummaryDiffData;
use crate::diff::{DiffError, SummaryDiffProcessor};
use crate::diff::output::SummaryDiffData::Twb;
use crate::summaries::{FileSummary, SummaryType};


/// Processes diffs of Twb Summaries, taking in [TwbSummary]s and producing a [TwbSummaryDiffContent]
/// indicating the delta between them.
///
/// Currently, this will just provide the two summaries (before + after). In the future, we can
/// change this to become more intelligent (actually identify what changed between the workbooks).
pub struct TwbSummaryDiffProcessor {
    proc: TwbDiffProcessor
}

impl TwbSummaryDiffProcessor {
    pub fn new() -> Self {
        TwbSummaryDiffProcessor {
            proc: TwbDiffProcessor {},
        }
    }
}


impl SummaryDiffProcessor for TwbSummaryDiffProcessor {
    type SummaryData = TwbSummary;

    fn get_type(&self) -> SummaryType {
        SummaryType::Twb
    }

    fn get_version(&self) -> u8 {
        1
    }

    fn get_data<'a>(&'a self, summary: &'a FileSummary) -> Option<&Self::SummaryData> {
        summary.additional_summaries.as_ref()
            .and_then(|ext|ext.twb.as_ref())
    }

    fn get_insert_diff(&self, summary: &TwbSummary) -> Result<SummaryDiffData, DiffError> {
        Ok(Twb(self.proc.get_insert_diff(summary)?))
    }

    fn get_remove_diff(&self, summary: &TwbSummary) -> Result<SummaryDiffData, DiffError> {
        Ok(Twb(self.proc.get_remove_diff(summary)?))
    }

    fn get_diff_impl(
        &self,
        before: &TwbSummary,
        after: &TwbSummary,
    ) -> Result<SummaryDiffData, DiffError> {
        Ok(Twb(self.proc.get_diff_impl(before, after)?))
    }
}
