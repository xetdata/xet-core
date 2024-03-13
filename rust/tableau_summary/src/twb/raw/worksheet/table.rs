use serde::{Deserialize, Serialize};
use roxmltree::Node;
use tracing::info;
use std::collections::HashMap;
use crate::twb::raw::datasource::columns::{ColumnDep, get_column_dep_map, GroupFilter};
use crate::twb::raw::util;
use crate::xml::XmlExt;

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
