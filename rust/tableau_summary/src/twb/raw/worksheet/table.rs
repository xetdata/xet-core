use std::borrow::Cow;
use serde::{Deserialize, Serialize};
use roxmltree::Node;
use tracing::info;
use std::collections::HashMap;
use crate::twb::raw::datasource::columns::{ColumnDep, ColumnInstanceMeta, get_column_dep_map, GroupFilter};
use crate::twb::raw::datasource::substituter::ColumnFinder;
use crate::twb::raw::util;
use crate::twb::summary::util::strip_brackets;
use crate::xml::XmlExt;

pub const MEASURE_NAMES_COL: &str = "[:Measure Names]";
pub const MEASURE_VALUES_COL: &str = "[Multiple Values]";

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct WorksheetTable {
    pub view: View,
    pub pane: Pane,
    pub rows: String,
    pub cols: String,
}

impl<'a, 'b> From<Node<'a, 'b>> for WorksheetTable {
    fn from(n: Node) -> Self {
        if n.get_tag() != "table" {
            info!("trying to convert a ({}) to table", n.get_tag());
            return Self::default();
        }
        let pane = n.get_tagged_child("panes")
            .and_then(|ch|ch.get_tagged_child("pane"))
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
}

impl<'a, 'b> From<Node<'a, 'b>> for View {
    fn from(n: Node) -> Self {
        if n.get_tag() != "view" && n.get_tag() != "dashboard" {
            info!("trying to convert a ({}) to a worksheet view or dashboard view", n.get_tag());
            return Self::default();
        }

        let sources = n.find_tagged_children("datasource-dependencies")
            .iter().map(|n| (n.get_attr("datasource"), *n))
            .map(|(name, node)| {
                (name, get_column_dep_map(node))
            }).collect();

        let filters = n.find_tagged_children("filter")
            .into_iter().map(Filter::from)
            .collect();

        Self {
            sources,
            filters,
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
            ColumnDep::Table(_) => Cow::from("")
        }
    }

    pub fn get_column(&self, source: &str, name: &str) -> Option<&ColumnDep> {
        if source.is_empty() {
            self.sources.values()
                .filter_map(|m|m.get(name))
                .next()
        } else {
            self.sources.get(&strip_brackets(source))
                .and_then(|m| m.get(name))
        }
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
        if name == MEASURE_NAMES_COL {
            return Some(Cow::from("[Measure Names]"));
        } else if name == MEASURE_VALUES_COL {
            return Some(Cow::from("[Measure Values]"));
        }
        self.get_column(source, name)
            .map(|dep| self.get_caption_for_dep(dep))
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Filter {
    pub class: String,
    pub column: String,
    pub group_filter: Option<GroupFilter>,
    pub range: Option<(String, String)>
}

impl<'a, 'b> From<Node<'a, 'b>> for Filter {
    fn from(n: Node) -> Self {
        if n.get_tag() != "filter" {
            info!("trying to convert a ({}) to a filter", n.get_tag());
            return Self::default();
        }
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
        if n.get_tag() != "pane" {
            info!("trying to convert a ({}) to a pane", n.get_tag());
            return Self::default();
        }
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