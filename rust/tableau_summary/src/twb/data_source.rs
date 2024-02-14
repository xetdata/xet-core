use roxmltree::Node;
use serde::{Deserialize, Serialize};
use crate::twb::{CAPTION_KEY, NAME_KEY, VERSION_KEY, xml};

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
    Ok(xml::get_nodes_with_tags(datasources_node, "datasource")
        .into_iter()
        .map(parse_datasource)
        .collect())
}

fn parse_datasource(node: Node) -> DataSourceMeta {
    let mut source = DataSourceMeta {
        version: xml::get_attr(node, VERSION_KEY),
        caption: xml::get_attr(node, CAPTION_KEY),
        name: xml::get_attr(node, NAME_KEY),
        ..Default::default()
    };
    for child_node in node.children() {
        match child_node.tag_name().name() {
            "connection" => {
                let named_connections = xml::get_nodes_with_tags(child_node, "named-connection");
                if !named_connections.is_empty() {
                    let child_connection = xml::get_nodes_with_tags(named_connections[0], "connection");
                    if !child_connection.is_empty() {
                        source.connection = Some(ConnectionMeta {
                            filename: xml::get_maybe_attr(child_connection[0], "filename"),
                            class: xml::get_attr(child_connection[0], "class"),
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
        name: xml::get_attr(n, NAME_KEY),
        caption: xml::get_attr(n, CAPTION_KEY),
        data_type: xml::get_attr(n, "datatype"),
        role: xml::get_attr(n, "role"),
        ..Default::default()
    };
    col.value = xml::get_maybe_attr(n, "value");
    let calc_nodes = xml::get_nodes_with_tags(n, "calculation");
    if !calc_nodes.is_empty() {
        col.formula = xml::get_maybe_attr(calc_nodes[0], "formula");
    }
    col
}
