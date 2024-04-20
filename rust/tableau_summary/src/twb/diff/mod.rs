use std::fmt::Debug;

use anyhow::Error;
use serde::{Deserialize, Serialize};
use crate::twb::diff::TwbSummaryDiffContent::{V0, V1};
use crate::twb::diff::util::{DiffItem, DiffProducer};
use crate::twb::diff::worksheet::WorksheetDiff;
use crate::twb::summary::dashboard::Dashboard;
use crate::twb::summary::datasource::Datasource;

use crate::twb::TwbSummary;

pub mod worksheet;
pub mod util;

/// Diff content for a Twb diff
#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TwbSummaryDiffContentV0 {
    pub before: Option<TwbSummary>,
    pub after: Option<TwbSummary>,
}

impl DiffProducer<TwbSummary> for TwbSummaryDiffContentV0 {
    fn new_addition(item: &TwbSummary) -> Self {
        Self {
            before: None,
            after: Some(item.clone()),
        }
    }

    fn new_deletion(item: &TwbSummary) -> Self {
        Self {
            before: Some(item.clone()),
            after: None,
        }
    }

    fn new_diff(before: &TwbSummary, after: &TwbSummary) -> Self {
        Self {
            before: Some(before.clone()),
            after: Some(after.clone()),
        }
    }
}

/// Diff content for a Twb diff
#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TwbSummaryDiffContentV1 {
    pub parse_version: DiffItem<u32>,
    pub wb_version: DiffItem<String>,
    pub datasources: Vec<DiffItem<Datasource>>,
    pub worksheets: Vec<WorksheetDiff>,
    pub dashboards: Vec<DiffItem<Dashboard>>,
}

impl DiffProducer<TwbSummary> for TwbSummaryDiffContentV1 {

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

const DIFF_VERSION: usize = 1;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
#[serde(untagged)]
pub enum TwbSummaryDiffContent {
    V0(TwbSummaryDiffContentV0),
    V1(TwbSummaryDiffContentV1),
    None,
}

impl DiffProducer<TwbSummary> for TwbSummaryDiffContent {
    fn new_addition(item: &TwbSummary) -> Self {
        match DIFF_VERSION {
            0 => V0(TwbSummaryDiffContentV0::new_addition(item)),
            1 => V1(TwbSummaryDiffContentV1::new_addition(item)),
            _ => TwbSummaryDiffContent::None
        }
    }

    fn new_deletion(item: &TwbSummary) -> Self {
        match DIFF_VERSION {
            0 => V0(TwbSummaryDiffContentV0::new_deletion(item)),
            1 => V1(TwbSummaryDiffContentV1::new_deletion(item)),
            _ => TwbSummaryDiffContent::None
        }
    }

    fn new_diff(before: &TwbSummary, after: &TwbSummary) -> Self {
        match DIFF_VERSION {
            0 => V0(TwbSummaryDiffContentV0::new_diff(before, after)),
            1 => V1(TwbSummaryDiffContentV1::new_diff(before, after)),
            _ => TwbSummaryDiffContent::None
        }
    }
}

pub struct TwbDiffProcessor {}

impl TwbDiffProcessor {
    pub fn get_insert_diff(&self, summary: &TwbSummary) -> Result<TwbSummaryDiffContent, Error> {
        Ok(TwbSummaryDiffContent::new_addition(summary))
    }

    pub fn get_remove_diff(&self, summary: &TwbSummary) -> Result<TwbSummaryDiffContent, Error> {
        Ok(TwbSummaryDiffContent::new_deletion(summary))
    }

    pub fn get_diff_impl(
        &self,
        before: &TwbSummary,
        after: &TwbSummary,
    ) -> Result<TwbSummaryDiffContent, Error> {
        Ok(TwbSummaryDiffContent::new_diff(before, after))
    }
}
