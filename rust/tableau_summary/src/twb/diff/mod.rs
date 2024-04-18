use anyhow::Error;
use serde::{Deserialize, Serialize};
use crate::twb::TwbSummary;

/// Diff content for a Twb diff
#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TwbSummaryDiffContent {
    pub before: Option<TwbSummary>,
    pub after: Option<TwbSummary>,
}

pub struct TwbDiffProcessor {}

impl TwbDiffProcessor {

    pub fn get_insert_diff(&self, summary: &TwbSummary) -> Result<TwbSummaryDiffContent, Error> {
        Ok(TwbSummaryDiffContent {
            before: None,
            after: Some(summary.clone()),
        })
    }

    pub fn get_remove_diff(&self, summary: &TwbSummary) -> Result<TwbSummaryDiffContent, Error> {
        Ok(TwbSummaryDiffContent {
            before: Some(summary.clone()),
            after: None,
        })
    }

    pub fn get_diff_impl(
        &self,
        before: &TwbSummary,
        after: &TwbSummary,
    ) -> Result<TwbSummaryDiffContent, Error> {
        Ok(TwbSummaryDiffContent {
            before: Some(before.clone()),
            after: Some(after.clone()),
        })
    }
}
