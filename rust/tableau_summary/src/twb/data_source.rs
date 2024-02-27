use std::collections::HashMap;
use roxmltree::Node;
use serde::{Deserialize, Serialize};
use tracing::info;
use columns::ColumnDep;
use crate::twb::{CAPTION_KEY, NAME_KEY, VERSION_KEY};
use crate::twb::data_source::columns::{ColumnSet, get_column_set};
use crate::twb::data_source::connection::Connection;
use crate::twb::xml::XmlExt;

pub mod connection;
pub mod model;
pub mod object_graph;
pub mod columns;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Datasource {
    name: String,
    version: String,
    caption: String,
    connection: Option<Connection>,
    column_set: ColumnSet,
}

impl<'a, 'b> From<Node<'a, 'b>> for Datasource {
    fn from(n: Node) -> Self {
        if n.get_tag() != "datasource" {
            info!("trying to convert a ({}) to datasource", n.get_tag());
            return Self::default();
        }
        let name = n.get_maybe_attr(NAME_KEY)
            .unwrap_or_else(|| n.get_attr("formatted-name"));
        let connection = n.get_tagged_child("connection")
            .map(Connection::from);
        let column_set = get_column_set(n);
        Self {
            name,
            version: n.get_attr(VERSION_KEY),
            caption: n.get_attr(CAPTION_KEY),
            connection,
            column_set,
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ConnectionMeta {
    filename: Option<String>,
    class: String,
    // TODO: we might want to parse more pieces of this section (e.g. `_.fcp.*` elements)
    //       some might contain column names in the files.
}


pub(crate) fn parse_datasources(datasources_node: Node) -> anyhow::Result<Vec<Datasource>> {
    Ok(datasources_node.find_all_tagged_decendants("datasource")
        .into_iter()
        .map(Datasource::from)
        .collect())
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
