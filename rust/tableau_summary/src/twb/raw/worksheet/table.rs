use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use once_cell::sync::Lazy;

use roxmltree::Node;
use serde::{Deserialize, Serialize};
use tracing::info;
use crate::check_tag_or_default;

use crate::twb::raw::datasource::columns::{ColumnDep, ColumnInstanceMeta, ColumnMeta, get_column_dep_map, GroupFilter};
use crate::twb::raw::datasource::RawDatasource;
use crate::twb::raw::datasource::substituter::ColumnFinder;
use crate::twb::raw::util;
use crate::twb::summary::util::strip_brackets;
use crate::xml::XmlExt;

pub const MEASURE_NAMES_COL_NAME: &str = "[:Measure Names]";
pub const MEASURE_VALUES_COL_NAME: &str = "[Multiple Values]";
pub const GENERATED_LATITUDE_NAME: &str = "[Latitude (generated)]";
pub const GENERATED_LONGITUDE_NAME: &str = "[Longitude (generated)]";

/// Special column for names of measures
static MEASURE_NAMES_COL: Lazy<ColumnDep> = Lazy::new(|| {
    ColumnDep::Column(ColumnMeta {
        name: MEASURE_NAMES_COL_NAME.to_string(),
        caption: "Measure Names".to_string(),
        role: "dimension".to_string(),
        ..Default::default()
    })
});

/// Special column for values of measures indicated by MEASURE_NAMES_COL
static MEASURE_VALUES_COL: Lazy<ColumnDep> = Lazy::new(|| {
    ColumnDep::Column(ColumnMeta {
        name: MEASURE_VALUES_COL_NAME.to_string(),
        caption: "Measure Values".to_string(),
        role: "measure".to_string(),
        ..Default::default()
    })
});

/// Special column for auto-generated latitude column
static GENERATED_LATITUDE_COL: Lazy<ColumnDep> = Lazy::new(|| {
    ColumnDep::Column(ColumnMeta {
        name: GENERATED_LATITUDE_NAME.to_string(),
        caption: "Latitude (generated)".to_string(),
        role: "measure".to_string(),
        ..Default::default()
    })
});

/// Special column for auto-generated longitude column
static GENERATED_LONGITUDE_COL: Lazy<ColumnDep> = Lazy::new(|| {
    ColumnDep::Column(ColumnMeta {
        name: GENERATED_LONGITUDE_NAME.to_string(),
        caption: "Longitude (generated)".to_string(),
        role: "measure".to_string(),
        ..Default::default()
    })
});

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct WorksheetTable {
    pub view: View,
    pub pane: Pane,
    pub rows: String,
    pub cols: String,
}

impl<'a, 'b> From<Node<'a, 'b>> for WorksheetTable {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "table");
        let pane = n.get_tagged_child("panes")
            .and_then(|ch| ch.get_tagged_child("pane"))
            .map(Pane::from)
            .unwrap_or_default();

        Self {
            view: n.get_tagged_child("view")
                .map(View::from)
                .unwrap_or_default(),
            pane,
            rows: n.get_tagged_child("rows")
                .map(|n| n.get_text())
                .unwrap_or_default(),
            cols: n.get_tagged_child("cols")
                .map(|n| n.get_text())
                .unwrap_or_default(),
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct View {
    pub sources: HashMap<String, HashMap<String, ColumnDep>>,
    pub filters: Vec<Filter>,
    #[serde(skip)]
    pub datasources: Arc<HashMap<String, RawDatasource>>,
}

impl<'a, 'b> From<Node<'a, 'b>> for View {
    fn from(n: Node) -> Self {
        if n.get_tag() != "view" && n.get_tag() != "dashboard" {
            info!("trying to convert a ({}) to a worksheet view or dashboard view", n.get_tag());
            return Self::default();
        }

        let sources = n.find_tagged_children("datasource-dependencies")
            .iter()
            .map(|n| (n.get_attr("datasource"), get_column_dep_map(*n)))
            .collect();

        let filters = n.find_tagged_children("filter")
            .into_iter().map(Filter::from)
            .collect();

        Self {
            sources,
            filters,
            datasources: Arc::new(HashMap::new()),
        }
    }
}


/// returns the caption for the column surrounded by `[]`.
fn get_captioned<'a>(caption: &str, name: &'a str) -> Cow<'a, str> {
    if caption.is_empty() {
        Cow::from(name)
    } else {
        Cow::from(format!("[{}]", caption))
    }
}

impl View {
    pub fn get_caption_for_dep<'a>(&'a self, dep: &'a ColumnDep) -> Cow<str> {
        match dep {
            ColumnDep::Column(c) => get_captioned(c.caption.as_str(), c.name.as_str()),
            ColumnDep::ColumnInstance(ci) => Cow::from(self.column_instance_caption(ci)),
            ColumnDep::Group(g) => get_captioned(g.caption.as_str(), g.name.as_str()),
            ColumnDep::Table(t) => get_captioned(t.caption.as_str(), t.name.as_str()),
        }
    }

    pub fn get_column(&self, source: &str, name: &str) -> Option<&ColumnDep> {
        if name == MEASURE_NAMES_COL_NAME {
            return Some(&MEASURE_NAMES_COL);
        } else if name == MEASURE_VALUES_COL_NAME {
            return Some(&MEASURE_VALUES_COL);
        } else if name == GENERATED_LATITUDE_NAME {
            return Some(&GENERATED_LATITUDE_COL);
        } else if name == GENERATED_LONGITUDE_NAME {
            return Some(&GENERATED_LONGITUDE_COL);
        }
        let self_source = if source.is_empty() {
            self.sources.values()
                .filter_map(|m| m.get(name))
                .next()
        } else {
            self.sources.get(&strip_brackets(source))
                .and_then(|m| m.get(name))
        };
        self_source.or_else(|| {
            self.datasources.get(&strip_brackets(source))
                .and_then(|ds| ds.get_column(name))
        })
    }

    fn column_instance_caption(&self, ci: &ColumnInstanceMeta) -> String {
        let col_name = self.find_column(&ci.source_column)
            .map(strip_brackets)
            .unwrap_or(ci.name.clone());
        let agg = match ci.derivation.as_str() {
            "None" | "" => None,
            "User" => Some("AGG".to_string()),
            "CountD" => Some("CNTD".to_string()),
            _ => Some(ci.derivation.to_uppercase()),
        };
        agg.map(|s| format!("{s}({col_name})"))
            .unwrap_or(col_name)
    }
}

impl ColumnFinder for View {
    fn find_column(&self, name: &str) -> Option<Cow<str>> {
        self.find_column_for_source("", name)
    }

    fn find_column_for_source(&self, source: &str, name: &str) -> Option<Cow<str>> {
        self.get_column(source, name)
            .map(|dep| self.get_caption_for_dep(dep))
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Filter {
    pub class: String,
    pub column: String,
    pub group_filter: Option<GroupFilter>,
    pub range: Option<(String, String)>,
}

impl<'a, 'b> From<Node<'a, 'b>> for Filter {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "filter");
        let range = n.get_tagged_child("min")
            .and_then(|min| n.get_tagged_child("max").map(|max| (min, max)))
            .map(|(min, max)| (min.get_text(), max.get_text()));

        Self {
            class: n.get_attr("class"),
            column: n.get_attr("column"),
            group_filter: n.get_tagged_child("groupfilter")
                .map(GroupFilter::from),
            range,
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Pane {
    pub mark_class: String,
    pub encodings: Vec<Encoding>,
    pub customized_tooltip: String,
}

impl<'a, 'b> From<Node<'a, 'b>> for Pane {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "pane");
        let encodings = n.get_tagged_child("encodings")
            .iter()
            .flat_map(Node::get_children)
            .filter(|n| !n.get_tag().is_empty())
            .map(Encoding::from)
            .collect();

        let customized_tooltip = n.get_tagged_child("customized-tooltip")
            .and_then(|ch| ch.get_tagged_child("formatted-text"))
            .map(util::parse_formatted_text)
            .unwrap_or_default();

        Self {
            mark_class: n.get_tagged_child("mark")
                .map(|ch| ch.get_attr("class"))
                .unwrap_or_default(),
            encodings,
            customized_tooltip,
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Encoding {
    pub enc_type: String,
    pub column: String,
}


impl<'a, 'b> From<Node<'a, 'b>> for Encoding {
    fn from(n: Node) -> Self {
        Self {
            enc_type: n.get_tag().to_string(),
            column: n.get_attr("column"),
        }
    }
}
