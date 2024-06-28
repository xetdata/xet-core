use std::borrow::Cow;
use std::collections::HashMap;

use roxmltree::Node;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::check_tag_or_default;
use substituter::ColumnFinder;

use crate::twb::raw::datasource::columns::{get_column_set, ColumnDep, ColumnMeta, ColumnSet};
use crate::twb::raw::datasource::connection::Connection;
use crate::twb::raw::datasource::dep::Dep;
use crate::twb::raw::datasource::object_graph::ObjectGraph;
use crate::twb::{CAPTION_KEY, NAME_KEY, VERSION_KEY};
use crate::xml::XmlExt;

pub mod columns;
pub mod connection;
pub mod dep;
pub mod object_graph;
pub mod substituter;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct RawDatasource {
    pub name: String,
    pub version: String,
    pub caption: String,
    pub connection: Option<Connection>,
    pub column_set: ColumnSet,
    pub object_graph: ObjectGraph,
    pub dependencies: HashMap<String, Dep>,
}

impl<'a, 'b> From<Node<'a, 'b>> for RawDatasource {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "datasource");
        Self {
            name: n
                .get_maybe_attr(NAME_KEY)
                .unwrap_or_else(|| n.get_attr("formatted-name")),
            version: n.get_attr(VERSION_KEY),
            caption: n.get_attr(CAPTION_KEY),
            connection: n.get_tagged_child("connection").map(Connection::from),
            column_set: get_column_set(n),
            object_graph: n
                .get_tagged_child("_.fcp.ObjectModelEncapsulateLegacy.true...object-graph")
                .map(ObjectGraph::from)
                .unwrap_or_default(),
            dependencies: n
                .find_tagged_children("datasource-dependencies")
                .into_iter()
                .map(Dep::from)
                .map(|d| (d.name.clone(), d))
                .collect(),
        }
    }
}

pub(crate) fn parse_datasources(datasources_node: Node) -> anyhow::Result<Vec<RawDatasource>> {
    let mut datasources = datasources_node
        .find_all_tagged_descendants("datasource")
        .into_iter()
        .map(RawDatasource::from)
        .collect::<Vec<_>>();
    let captions = datasources
        .iter()
        .map(|d| (d.name.clone(), d.caption.clone()))
        .collect::<HashMap<_, _>>();
    datasources.iter_mut().for_each(|d| {
        d.update_dependent_datasource_captions(&captions);
    });
    Ok(datasources)
}

impl RawDatasource {
    pub fn find_table(&self, col_name: &str) -> String {
        if let Some(ref conn) = self.connection {
            conn.metadata_records
                .columns
                .get(col_name)
                .map(|meta| meta.table.clone())
                .unwrap_or_default()
        } else {
            String::new()
        }
    }

    pub fn get_table_aggregation(&self, table: &str) -> Option<String> {
        self.connection
            .as_ref()
            .and_then(|conn| conn.metadata_records.capabilities.get(table).cloned())
    }

    pub fn get_column(&self, name: &str) -> Option<&ColumnDep> {
        self.column_set.columns.get(name)
    }

    fn update_dependent_datasource_captions(&mut self, captions: &HashMap<String, String>) {
        self.dependencies.values_mut().for_each(|dep| {
            dep.caption = captions.get(&dep.name).cloned().unwrap_or_default();
        });
    }
}

/// returns the caption for the column surrounded by `[]`.
fn get_column_captioned(c: &ColumnMeta) -> Cow<str> {
    if c.caption.is_empty() {
        Cow::from(c.name.as_str())
    } else {
        Cow::from(format!("[{}]", c.caption))
    }
}

/// returns the caption for the dependency. Due to how dependent sources are stored,
/// this will not have brackets surrounding the string.
fn get_source_caption(dep: &Dep) -> &str {
    if dep.caption.is_empty() {
        dep.name.as_str()
    } else {
        dep.caption.as_str()
    }
}

fn strip_brackets(s: &str) -> &str {
    s.trim_start_matches('[').trim_end_matches(']')
}

impl ColumnFinder for RawDatasource {
    fn find_column(&self, name: &str) -> Option<Cow<str>> {
        self.column_set
            .columns
            .get(name)
            .and_then(ColumnDep::get_column)
            .map(get_column_captioned)
    }

    fn find_column_for_source(&self, source: &str, name: &str) -> Option<Cow<str>> {
        let dep = self.dependencies.get(strip_brackets(source))?;
        let source_name = get_source_caption(dep);
        dep.columns
            .get(name)
            .or_else(|| self.column_set.columns.get(name))
            .and_then(ColumnDep::get_column)
            .map(get_column_captioned)
            .map(|c| format!("[{}].{}", source_name, c.as_ref()))
            .map(Cow::from)
    }
}
