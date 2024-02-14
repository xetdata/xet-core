use roxmltree::Node;
use serde::{Deserialize, Serialize};

use crate::twb::{CAPTION_KEY, NAME_KEY, VERSION_KEY};
use crate::twb::xml::XmlExt;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct DataSourceMeta {
    name: String,
    version: String,
    caption: String,
    connection: Option<ConnectionMeta>,
    columns: Vec<ColumnMeta>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ConnectionMeta {
    filename: Option<String>,
    class: String,
    // TODO: we might want to parse more pieces of this section (e.g. `_.fcp.*` elements)
    //       some might contain column names in the files.
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ColumnMeta {
    name: String,
    caption: String,
    // maybe enum of types?
    data_type: String,
    // maybe enum of [dimension/measure]
    role: String,
    formula: Option<String>,
    value: Option<String>,
}

pub(crate) fn parse_datasources(datasources_node: Node) -> anyhow::Result<Vec<DataSourceMeta>> {
    Ok(datasources_node.find_all_tagged_decendants("datasource")
        .into_iter()
        .map(parse_datasource)
        .collect())
}

fn parse_datasource(node: Node) -> DataSourceMeta {
    let mut source = DataSourceMeta {
        version: node.get_attr(VERSION_KEY),
        caption: node.get_attr(CAPTION_KEY),
        name: node.get_attr(NAME_KEY),
        ..Default::default()
    };
    for child_node in node.children() {
        match child_node.tag_name().name() {
            "connection" => {
                let named_connections = child_node.find_all_tagged_decendants("named-connection");
                if !named_connections.is_empty() {
                    let child_connection = named_connections[0].find_all_tagged_decendants("connection");
                    if !child_connection.is_empty() {
                        source.connection = Some(ConnectionMeta {
                            filename: child_connection[0].get_maybe_attr("filename"),
                            class: child_connection[0].get_attr("class"),
                        });
                    }
                }
            }
            "column" => {
                let col = parse_column_meta(child_node);
                source.columns.push(col);
            }
            _ => {}
        }
    }
    source
}

pub fn parse_column_meta(n: Node) -> ColumnMeta {
    let mut col = ColumnMeta {
        name: n.get_attr(NAME_KEY),
        caption: n.get_attr(CAPTION_KEY),
        data_type: n.get_attr("datatype"),
        role: n.get_attr("role"),
        ..Default::default()
    };
    let _fmt = n.get_attr("default-format");
    col.value = n.get_maybe_attr("value");
    let calc_nodes = n.find_all_tagged_decendants("calculation");
    if !calc_nodes.is_empty() {
        col.formula = calc_nodes[0].get_maybe_attr("formula");
    }
    col
}
