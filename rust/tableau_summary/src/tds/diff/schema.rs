use serde::{Deserialize, Serialize};

use crate::tds::diff::schema::TdsSummaryDiffContent::V1;
use crate::tds::TdsSummary;
use crate::twb::diff::datasource::DatasourceDiff;
use crate::twb::diff::util::DiffProducer;

/// Which schema of diffs to use.
pub const DIFF_VERSION: usize = 1;

/// Diff content for a Tds diff. Currently an enum of different schema versions.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
#[serde(untagged)]
pub enum TdsSummaryDiffContent {
    V1(TdsSummaryDiffContentV1),
    None,
}

impl DiffProducer<TdsSummary> for TdsSummaryDiffContent {
    fn new_addition(item: &TdsSummary) -> Self {
        match DIFF_VERSION {
            1 => V1(TdsSummaryDiffContentV1::new_addition(item)),
            _ => TdsSummaryDiffContent::None,
        }
    }

    fn new_deletion(item: &TdsSummary) -> Self {
        match DIFF_VERSION {
            1 => V1(TdsSummaryDiffContentV1::new_deletion(item)),
            _ => TdsSummaryDiffContent::None,
        }
    }

    fn new_diff(before: &TdsSummary, after: &TdsSummary) -> Self {
        match DIFF_VERSION {
            1 => V1(TdsSummaryDiffContentV1::new_diff(before, after)),
            _ => TdsSummaryDiffContent::None,
        }
    }
}

/// V1 diff format: Detailed diffs for the datasources, worksheets, and dashboards.
#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TdsSummaryDiffContentV1 {
    pub datasource: DatasourceDiff,
}

impl DiffProducer<TdsSummary> for TdsSummaryDiffContentV1 {
    fn new_addition(summary: &TdsSummary) -> Self {
        Self {
            datasource: DatasourceDiff::new_addition(&summary.datasource),
        }
    }

    fn new_deletion(summary: &TdsSummary) -> Self {
        Self {
            datasource: DatasourceDiff::new_deletion(&summary.datasource),
        }
    }

    fn new_diff(before: &TdsSummary, after: &TdsSummary) -> Self {
        Self {
            datasource: DatasourceDiff::new_diff(&before.datasource, &after.datasource),
        }
    }
}
