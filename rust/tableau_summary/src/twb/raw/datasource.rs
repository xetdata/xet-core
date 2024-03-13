use std::borrow::Cow;
use std::collections::HashMap;
use roxmltree::Node;
use serde::{Deserialize, Serialize};
use tracing::info;
use substituter::ColumnFinder;
use crate::twb::{CAPTION_KEY, NAME_KEY, VERSION_KEY};
use crate::twb::raw::datasource::columns::{ColumnDep, ColumnMeta, ColumnSet, get_column_set};
use crate::twb::raw::datasource::connection::Connection;
use crate::twb::raw::datasource::dep::Dep;
use crate::twb::raw::datasource::object_graph::ObjectGraph;
use crate::xml::XmlExt;

pub mod connection;
pub mod object_graph;
pub mod columns;
pub mod dep;
pub mod substituter;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Datasource {
    pub name: String,
    pub version: String,
    pub caption: String,
    pub connection: Option<Connection>,
    pub column_set: ColumnSet,
    pub object_graph: ObjectGraph,
    pub dependencies: HashMap<String, Dep>,
}

impl<'a, 'b> From<Node<'a, 'b>> for Datasource {
    fn from(n: Node) -> Self {
        if n.get_tag() != "datasource" {
            info!("trying to convert a ({}) to datasource", n.get_tag());
            return Self::default();
        }
        Self {
            name: n.get_maybe_attr(NAME_KEY)
                .unwrap_or_else(|| n.get_attr("formatted-name")),
            version: n.get_attr(VERSION_KEY),
            caption: n.get_attr(CAPTION_KEY),
            connection: n.get_tagged_child("connection")
                .map(Connection::from),
            column_set: get_column_set(n),
            object_graph: n.get_tagged_child("_.fcp.ObjectModelEncapsulateLegacy.true...object-graph")
                .map(ObjectGraph::from)
                .unwrap_or_default(),
            dependencies: n.find_tagged_children("datasource-dependencies")
                .into_iter()
                .map(Dep::from)
                .map(|d| (d.name.clone(), d))
                .collect(),
        }
    }
}

pub(crate) fn parse_datasources(datasources_node: Node) -> anyhow::Result<Vec<Datasource>> {
    let mut datasources = datasources_node.find_all_tagged_decendants("datasource")
        .into_iter()
        .map(Datasource::from)
        .collect::<Vec<_>>();
    let captions = datasources.iter()
        .map(|d| (d.name.clone(), d.caption.clone()))
        .collect::<HashMap<_, _>>();
    datasources.iter_mut().for_each(|d| {
        d.update_dependent_datasource_captions(&captions);
    });
    Ok(datasources)
}

impl Datasource {
    pub fn find_table(&self, col_name: &str) -> String {
        if let Some(ref conn) = self.connection {
            conn.metadata_records.columns.get(col_name)
                .map(|meta| meta.table.clone())
                .unwrap_or_default()
        } else {
            String::new()
        }
    }

    pub fn get_table_aggregation(&self, table: &str) -> Option<String> {
        self.connection.as_ref()
            .and_then(|conn| conn.metadata_records.capabilities.get(table)
                .cloned())
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
    s.trim_start_matches('[')
        .trim_end_matches(']')
}

impl ColumnFinder for Datasource {

    fn find_column(&self, name: &str) -> Option<Cow<str>> {
        self.column_set.columns.get(name)
            .and_then(ColumnDep::get_column)
            .map(get_column_captioned)
    }

    fn find_column_for_source(&self, source: &str, name: &str) -> Option<Cow<str>> {
        if let Some(dep) = self.dependencies.get(strip_brackets(source)) {
            let source_name = get_source_caption(dep);
            return dep.columns.get(name)
                .or_else(|| self.column_set.columns.get(name))
                .and_then(ColumnDep::get_column)
                .map(get_column_captioned)
                .map(|c|format!("[{}].{}", source_name, c.as_ref()))
                .map(Cow::from);
        }
        None
    }

}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;
    use super::*;

    #[test]
    fn test_parse_worksheet() {
        let mut file = File::open("src/Superstore.twb").unwrap();
        let mut s = String::new();
        let _ = file.read_to_string(&mut s).unwrap();
        let doc = roxmltree::Document::parse(&s).unwrap();
        let root = doc.root();
        let root = root.find_all_tagged_decendants("workbook")[0];
        let datasources = root.get_tagged_child("datasources").unwrap();
        let data = parse_datasources(datasources).unwrap();
        let s = serde_json::to_string(&data).unwrap();
        println!("{s}");
        assert_eq!("federated.0a01cod1oxl83l1f5yves1cfciqo", data[2].name);
        assert!(data[2].connection.is_some());
    }

}
