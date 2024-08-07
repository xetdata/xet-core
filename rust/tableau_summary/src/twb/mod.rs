use std::borrow::Cow;
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use crate::twb::raw::dashboard::{parse_dashboards, RawDashboard};
use anyhow::anyhow;
use itertools::Itertools;
use roxmltree::Node;
use serde::{Deserialize, Serialize};

use crate::twb::raw::datasource::{parse_datasources, RawDatasource};
use crate::twb::raw::worksheet::{parse_worksheets, RawWorksheet};
use crate::twb::summary::dashboard::{Dashboard, DashboardV1};
use crate::twb::summary::datasource::{Datasource, DatasourceV1};
use crate::twb::summary::worksheet::Worksheet;
use crate::xml::XmlExt;

pub mod diff;
pub mod printer;
pub mod raw;
pub mod summary;

const PARSER_VERSION: u32 = 3;
const VERSION_KEY: &str = "version";
const CAPTION_KEY: &str = "caption";
const NAME_KEY: &str = "name";

/// Analyzes Tableau's workbook files to produce a summary
/// that can visualize the key pieces of a workbook.
#[derive(Default, Debug)]
pub struct TwbAnalyzer {
    /// for now, just store all the XML content. This may cause memory issues
    /// if the workbook file is large, in which case, we can build a streaming
    /// reader to collect the info we want.
    content_buffer: Vec<u8>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
#[repr(u32)]
pub enum TwbSummaryVersioner {
    #[default]
    Default = 0x00,
    V1(TwbSummaryV1) = 0x01,
    V2(TwbSummaryV2) = 0x02,
    V3(TwbSummaryV3) = PARSER_VERSION,
}

pub type TwbSummary = TwbSummaryV3;

impl TwbSummary {
    pub fn from_ref(summary: &TwbSummaryVersioner) -> Option<Cow<Self>> {
        match summary {
            TwbSummaryVersioner::Default => None,
            TwbSummaryVersioner::V1(s) => {
                Some(Cow::Owned(TwbSummary::from(&TwbSummaryV2::from(s))))
            }
            TwbSummaryVersioner::V2(s) => Some(Cow::Owned(TwbSummary::from(s))),
            TwbSummaryVersioner::V3(s) => Some(Cow::Borrowed(s)),
        }
    }
}

/// A summary of a Tableau Workbook File (*.twb) providing the
/// key components of a workbook.
/// V3 changes dashboards to allow multiple top-level zones.
#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
pub struct TwbSummaryV3 {
    pub wb_version: String,
    pub datasources: Vec<Datasource>,
    pub worksheets: Vec<Worksheet>,
    pub dashboards: Vec<Dashboard>,
}

impl From<&TwbSummaryV2> for TwbSummaryV3 {
    fn from(s2: &TwbSummaryV2) -> Self {
        Self {
            wb_version: s2.wb_version.clone(),
            datasources: s2.datasources.clone(),
            worksheets: s2.worksheets.clone(),
            dashboards: s2.dashboards.iter().map(Dashboard::from).collect_vec(),
        }
    }
}

/// A summary of a Tableau Workbook File (*.twb) providing the
/// key components of a workbook.
/// V2 adds in Datasource relations.
#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
pub struct TwbSummaryV2 {
    pub wb_version: String,
    pub datasources: Vec<Datasource>,
    pub worksheets: Vec<Worksheet>,
    pub dashboards: Vec<DashboardV1>,
}

impl From<&TwbSummaryV1> for TwbSummaryV2 {
    fn from(s1: &TwbSummaryV1) -> Self {
        Self {
            wb_version: s1.wb_version.clone(),
            datasources: s1.datasources.iter().map(Datasource::from).collect_vec(),
            worksheets: s1.worksheets.clone(),
            dashboards: s1.dashboards.clone(),
        }
    }
}

/// A summary of a Tableau Workbook File (*.twb) providing the
/// key components of a workbook.
///
/// repository-location indicates the views of the workbook
#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
pub struct TwbSummaryV1 {
    pub wb_version: String,
    pub datasources: Vec<DatasourceV1>,
    pub worksheets: Vec<Worksheet>,
    pub dashboards: Vec<DashboardV1>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TwbRaw {
    wb_version: String,
    datasources: Vec<RawDatasource>,
    worksheets: Vec<RawWorksheet>,
    dashboards: Vec<RawDashboard>,
}

impl TwbAnalyzer {
    pub fn new() -> Self {
        Self {
            content_buffer: vec![],
        }
    }

    pub fn process_chunk(&mut self, chunk: &[u8]) {
        self.content_buffer.extend_from_slice(chunk);
    }

    pub fn finalize(&mut self) -> anyhow::Result<Option<TwbSummaryVersioner>> {
        let mut buf = vec![];
        mem::swap(&mut self.content_buffer, &mut buf);

        // Parse XML into XML tree
        let content_string =
            String::from_utf8(buf).map_err(|e| anyhow!("parsed TWB is not UTF-8: {e:?}"))?;
        let document = roxmltree::Document::parse(&content_string)
            .map_err(|e| anyhow!("TWB content wasn't parsed as XML: {e:?}"))?;
        let root = document
            .root()
            .get_tagged_child("workbook")
            .ok_or(anyhow!("no workbook node"))?;

        // Build raw workbook model
        let mut raw_workbook = TwbRaw::try_from(root)?;
        let raw_ds_map = raw_workbook
            .datasources
            .iter()
            .map(|ds| (ds.name.clone(), ds.clone()))
            .collect::<HashMap<_, _>>();
        let raw_ds_map = Arc::new(raw_ds_map);
        raw_workbook
            .worksheets
            .iter_mut()
            .for_each(|ws| ws.table.view.datasources = raw_ds_map.clone());
        raw_workbook
            .dashboards
            .iter_mut()
            .for_each(|dash| dash.view.datasources = raw_ds_map.clone());

        // Summarize from the raw workbook model
        let datasources = raw_workbook
            .datasources
            .iter()
            .map(Datasource::from)
            .collect();
        let worksheets = raw_workbook
            .worksheets
            .iter()
            .map(Worksheet::from)
            .collect();
        let dashboards = raw_workbook
            .dashboards
            .iter()
            .map(Dashboard::from)
            .collect();
        Ok(Some(TwbSummaryVersioner::V3(TwbSummary {
            wb_version: raw_workbook.wb_version,
            datasources,
            worksheets,
            dashboards,
        })))
    }
}

impl<'a, 'b> TryFrom<Node<'a, 'b>> for TwbRaw {
    type Error = anyhow::Error;

    fn try_from(n: Node) -> anyhow::Result<Self> {
        if n.get_tag() != "workbook" {
            return Err(anyhow!(
                "trying to convert a ({}) to a top-level workbook",
                n.get_tag()
            ));
        }
        let datasources = n
            .get_tagged_child("datasources")
            .ok_or(anyhow!("no datasources for workbook"))
            .and_then(parse_datasources)?;
        let worksheets = n
            .get_tagged_child("worksheets")
            .ok_or(anyhow!("no worksheets for workbook"))
            .and_then(parse_worksheets)?;
        let dashboards = n
            .get_tagged_child("dashboards")
            .map(parse_dashboards)
            .unwrap_or(Ok(vec![]))?;

        Ok(Self {
            wb_version: n.get_attr(VERSION_KEY),
            datasources,
            worksheets,
            dashboards,
        })
    }
}
