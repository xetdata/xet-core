use std::fmt::Debug;

use anyhow::Error;
use serde::{Deserialize, Serialize};
use crate::twb::diff::util::{diff_list, DiffItem, DiffProducer};
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
            parse_version: DiffItem::added(&summary.parse_version),
            wb_version: DiffItem::added(&summary.wb_version),
            datasources: DiffItem::added_list(&summary.datasources),
            worksheets: summary.worksheets.iter().map(WorksheetDiff::new_addition).collect(),
            dashboards: DiffItem::added_list(&summary.dashboards),
        }
    }

    fn new_deletion(summary: &TwbSummary) -> Self {
        Self {
            parse_version: DiffItem::deleted(&summary.parse_version),
            wb_version: DiffItem::deleted(&summary.wb_version),
            datasources: DiffItem::deleted_list(&summary.datasources),
            worksheets: summary.worksheets.iter().map(WorksheetDiff::new_deletion).collect(),
            dashboards: DiffItem::deleted_list(&summary.dashboards),
        }
    }

    fn new_diff(before: &TwbSummary, after: &TwbSummary) -> Self {
        Self {
            parse_version: DiffItem::compared(&before.parse_version, &after.parse_version),
            wb_version: DiffItem::compared(&before.wb_version, &after.wb_version),
            datasources: DiffItem::compare_unique_lists(&before.datasources, &after.datasources, |ds|ds.name.clone()),
            worksheets: diff_list(&before.worksheets, &after.worksheets, |w|w.name.clone()),
            dashboards: DiffItem::compare_unique_lists(&before.dashboards, &after.dashboards, |dash| dash.name.clone()),
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
