use std::fmt::Debug;

use anyhow::Error;
use serde::{Deserialize, Serialize};
use crate::twb::diff::util::{DiffItem, DiffProducer};
use crate::twb::diff::worksheet::WorksheetDiff;
use crate::twb::summary::dashboard::Dashboard;
use crate::twb::summary::datasource::Datasource;

use crate::twb::TwbSummary;

pub mod worksheet;
pub mod util;

/// Diff content for a Twb diff
#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TwbSummaryDiffContentV1 {
    pub before: Option<TwbSummary>,
    pub after: Option<TwbSummary>,
}

/// Diff content for a Twb diff
#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TwbSummaryDiffContent {
    pub parse_version: DiffItem<u32>,
    pub wb_version: DiffItem<String>,
    pub datasources: Vec<DiffItem<Datasource>>,
    pub worksheets: Vec<WorksheetDiff>,
    pub dashboards: Vec<DiffItem<Dashboard>>,
}

impl DiffProducer<TwbSummary> for TwbSummaryDiffContent {

    fn new_addition(summary: &TwbSummary) -> Self {
        Self {
            parse_version: DiffItem::new_addition(&summary.parse_version),
            wb_version: DiffItem::new_addition(&summary.wb_version),
            datasources: DiffItem::new_addition_list(&summary.datasources),
            worksheets: WorksheetDiff::new_addition_list(&summary.worksheets),
            dashboards: DiffItem::new_addition_list(&summary.dashboards),
        }
    }

    fn new_deletion(summary: &TwbSummary) -> Self {
        Self {
            parse_version: DiffItem::new_deletion(&summary.parse_version),
            wb_version: DiffItem::new_deletion(&summary.wb_version),
            datasources: DiffItem::new_deletion_list(&summary.datasources),
            worksheets: WorksheetDiff::new_deletion_list(&summary.worksheets),
            dashboards: DiffItem::new_deletion_list(&summary.dashboards),
        }
    }

    fn new_diff(before: &TwbSummary, after: &TwbSummary) -> Self {
        Self {
            parse_version: DiffItem::new_diff(&before.parse_version, &after.parse_version),
            wb_version: DiffItem::new_diff(&before.wb_version, &after.wb_version),
            datasources: DiffItem::new_unique_diff_list(&before.datasources, &after.datasources, |ds|ds.name.clone()),
            worksheets: WorksheetDiff::new_unique_diff_list(&before.worksheets, &after.worksheets, |w|w.name.clone()),
            dashboards: DiffItem::new_unique_diff_list(&before.dashboards, &after.dashboards, |dash| dash.name.clone()),
        }
    }
}

pub struct TwbDiffProcessor {}

impl TwbDiffProcessor {
    pub fn get_insert_diff(&self, summary: &TwbSummary) -> Result<TwbSummaryDiffContentV1, Error> {

        Ok(TwbSummaryDiffContentV1 {
            before: None,
            after: Some(summary.clone()),
        })
    }

    pub fn get_remove_diff(&self, summary: &TwbSummary) -> Result<TwbSummaryDiffContentV1, Error> {
        Ok(TwbSummaryDiffContentV1 {
            before: Some(summary.clone()),
            after: None,
        })
    }

    pub fn get_diff_impl(
        &self,
        before: &TwbSummary,
        after: &TwbSummary,
    ) -> Result<TwbSummaryDiffContentV1, Error> {
        Ok(TwbSummaryDiffContentV1 {
            before: Some(before.clone()),
            after: Some(after.clone()),
        })
    }
}
