use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use roxmltree::Node;
use tracing::info;
use error_printer::ErrorPrinter;
use itertools::Itertools;
use crate::twb::{CAPTION_KEY, NAME_KEY};
use crate::twb::xml::XmlExt;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Connection {
    named_connections: HashMap<String, NamedConnection>,
    relations: Relations,
    cols: ColMapping,
    metadata_records: MetadataRecords,
}

impl<'a, 'b> From<Node<'a, 'b>> for Connection {
    fn from(n: Node) -> Self {
        if n.get_tag() != "connection" {
            info!("trying to convert a ({}) to a top-level connection", n.get_tag());
            return Self::default();
        }
        let named_connections = n.get_tagged_child("named-connections")
            .into_iter()
            .flat_map(|c|c.find_tagged_children("named-connection"))
            .map(NamedConnection::from)
            .map(|nc| (nc.name.clone(), nc))
            .collect();
        let relations = n.get_tagged_child("_.fcp.ObjectModelEncapsulateLegacy.true...relation")
            .map(Relations::from)
            .unwrap_or_default();
        let cols = n.get_tagged_child("cols")
            .map(ColMapping::from)
            .unwrap_or_default();
        let metadata_records = n.get_tagged_child("metadata-records")
            .map(MetadataRecords::from)
            .unwrap_or_default();
        Self {
            named_connections,
            relations,
            cols,
            metadata_records,
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct NamedConnection {
    name: String,
    caption: String,
    class: String,
    filename: Option<String>,
}

impl<'a, 'b> From<Node<'a, 'b>> for NamedConnection {
    fn from(n: Node) -> Self {
        if n.get_tag() != "named-connection" {
            info!("trying to convert a ({}) to a named connection", n.get_tag());
            return Self::default();
        }
        let (class, filename) = n.get_tagged_child("connection")
            .map(|c| {
                let class = c.get_attr("class");
                let filename = c.get_maybe_attr("filename")
                    .map(|filename| c.get_maybe_attr("directory")
                            .map(|dir| format!("{dir}/{filename}"))
                            .unwrap_or(filename)
                    );
                (class, filename)
            }).unwrap_or_default();
        Self {
            name: n.get_attr(NAME_KEY),
            caption: n.get_attr(CAPTION_KEY),
            class,
            filename,
        }
    }
}



#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Relations {
    tables: HashMap<String, Table>,
    joins: HashMap<String, Join>,
    unions: HashMap<String, Union>,
}

#[derive(Default)]
pub enum Relation {
    #[default]
    Unknown,
    Table(Table),
    Join(Join),
    Union(Union),
}

impl Relation {
    fn get_type(&self) -> &'static str {
        match self {
            Relation::Unknown => "unknown",
            Relation::Table(_) => "table",
            Relation::Join(_) => "join",
            Relation::Union(_) => "union",
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Table {
    name: String,
    connection: String,
    columns: HashMap<String, TableColumn>
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Join {
    join_type: String,
    clause: Clause,
    tables: HashMap<String, Table>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Union {
    name: String,
    all: bool,
    union_table: Table,
    tables: HashMap<String, Table>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Clause {
    clause_type: String,
    expression: Expression,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Expression {
    op: String,
    expressions: Vec<Expression>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TableColumn {
    name: String,
    datatype: String,
    ordinal: usize,
}

impl<'a, 'b> From<Node<'a, 'b>> for Relations {
    fn from(n: Node) -> Self {
        if n.get_tag() != "_.fcp.ObjectModelEncapsulateLegacy.true...relation" {
            info!("trying to convert a ({}) to a relations node", n.get_tag());
            return Self::default();
        }
        let rel_type = n.get_attr("type");

        match rel_type.as_str() {
            "collection" => {
                let mut tables = HashMap::new();
                let mut joins = HashMap::new();
                let mut unions = HashMap::new();

                let relations = n.find_tagged_children("relation")
                    .into_iter()
                    .map(Relation::from)
                    .collect::<Vec<_>>();
                for relation in relations {
                    match relation {
                        Relation::Unknown => {}
                        Relation::Table(t) => {
                            tables.insert(t.name.clone(), t);
                        }
                        Relation::Join(j) => {
                            for k in j.tables.keys() {
                                joins.insert(k.to_owned(), j.clone());
                            }
                        }
                        Relation::Union(u) => {
                            unions.insert(u.name.clone(), u.clone());
                            for k in u.tables.keys() {
                                unions.insert(k.to_owned(), u.clone());
                            }
                        }
                    }
                }
                Self {
                    tables,
                    joins,
                    unions,
                }
            },
            "table" => {
                let table: Table = n.into();
                let tables = HashMap::from([(table.name.clone(), table)]);
                Self {
                    tables,
                    ..Default::default()
                }
            },
            _ => {
                info!("unknown type: {rel_type} for relations node");
                Self::default()
            }
        }

    }
}

impl<'a, 'b> From<Node<'a, 'b>> for Relation {
    fn from(n: Node) -> Self {
        if n.get_tag() != "relation" {
            info!("trying to convert a ({}) to a relation", n.get_tag());
            return Self::default();
        }
        let rel_type = n.get_attr("type");
        match rel_type.as_str() {
            "table" => Self::Table(n.into()),
            "join" => Self::Join(n.into()),
            "union" => Self::Union(n.into()),
            _ => {
                info!("unknown type: {rel_type} for relation");
                Self::default()
            }
        }

    }
}

impl<'a, 'b> From<Node<'a, 'b>> for Union {
    fn from(n: Node) -> Self {
        if n.get_tag() != "relation" {
            info!("trying to convert a ({}) to a union relation", n.get_tag());
            return Self::default();
        }
        let tables = n.find_tagged_children("relation")
            .into_iter()
            .map(Table::from)
            .map(|table| (table.name.clone(), table))
            .collect();
        Self {
            name: n.get_attr(NAME_KEY),
            all: n.get_attr("all").parse().unwrap_or_default(),
            union_table: Table::from(n),
            tables,
        }
    }
}

impl<'a, 'b> From<Node<'a, 'b>> for Join {
    fn from(n: Node) -> Self {
        if n.get_tag() != "relation" {
            info!("trying to convert a ({}) to a join relation", n.get_tag());
            return Self::default();
        }
        let tables = n.find_tagged_children("relation")
            .into_iter()
            .map(Table::from)
            .map(|table| (table.name.clone(), table))
            .collect();
        let clause = n.get_tagged_child("clause")
            .map(|c| Clause {
                clause_type: c.get_attr("type"),
                expression: c.get_tagged_child("expression").map(Expression::from).unwrap_or_default(),
            }).unwrap_or_default();
        Self {
            join_type: n.get_attr("join"),
            clause,
            tables,
        }
    }
}

impl<'a, 'b> From<Node<'a, 'b>> for Expression {
    fn from(n: Node) -> Self {
        if n.get_tag() != "expression" {
            info!("trying to convert a ({}) to an expression", n.get_tag());
            return Self::default();
        }
        let expressions = n.find_tagged_children("expression")
            .into_iter()
            .map(Expression::from)
            .collect();

        Self {
            op: n.get_attr("op"),
            expressions,
        }
    }
}


impl<'a, 'b> From<Node<'a, 'b>> for Table {
    fn from(n: Node) -> Self {
        if n.get_tag() != "relation" && n.get_tag() != "_.fcp.ObjectModelEncapsulateLegacy.true...relation" {
            info!("trying to convert a ({}) to a table relation", n.get_tag());
            return Self::default();
        }
        let columns = n.get_tagged_child("columns")
            .into_iter()
            .flat_map(|c|c.find_tagged_children("column"))
            .map(TableColumn::from)
            .map(|col| (col.name.clone(), col))
            .collect();
        Self {
            name: n.get_attr(NAME_KEY),
            connection: n.get_attr("connection"),
            columns,
        }
    }
}

impl<'a, 'b> From<Node<'a, 'b>> for TableColumn {
    fn from(n: Node) -> Self {
        if n.get_tag() != "column" {
            info!("trying to convert a ({}) to a column", n.get_tag());
            return Self::default();
        }
        let ordinal = n.get_maybe_attr("ordinal")
            .map(|s|s.parse()
                .log_error("ordinal not a number")
                .unwrap_or_default())
            .unwrap_or_default();
        Self {
            name: n.get_attr(NAME_KEY),
            datatype: n.get_attr("datatype"),
            ordinal,
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ColMapping {
    cols: HashMap<String, (String, String)>,
}

impl<'a, 'b> From<Node<'a, 'b>> for ColMapping {
    fn from(n: Node) -> Self {
        if n.get_tag() != "cols" {
            info!("trying to convert a ({}) to cols", n.get_tag());
            return Self::default();
        }
        let cols = n.find_tagged_children("map")
            .into_iter()
            .flat_map(map_node_to_kv)
            .collect();
        Self {
            cols,
        }
    }
}

fn map_node_to_kv(n: Node) -> Option<(String, (String, String))> {
    let key = n.get_attr("key");
    let val = n.get_attr("value");
    let val_parts = parse_identifiers(&val);
    let val_tuple = if let Some((x, y)) = val_parts.into_iter().collect_tuple() {
        (x, y)
    } else {
        info!("found col value: {val} that didn't parse into 2 parts");
        return None;
    };
    Some((key, val_tuple))
}

// Given a string like `[foo].[bar]` return ["foo", "bar"]
// TODO: see what tds / twb files do when the table or column has a `.` in the name
//       currently, we just assume it is not escaped or anything
//       (e.g. "[foo.1].[bar.txt]" -> ["foo.1", "bar.txt"])
pub fn parse_identifiers(s: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current_entry = String::new();
    let mut inside_brackets = false;

    for c in s.chars() {
        match c {
            '[' => {
                inside_brackets = true;
            }
            ']' => {
                inside_brackets = false;
                result.push(current_entry.clone());
                current_entry.clear();
            }
            '.' if inside_brackets => {
                current_entry.push('.');
            }
            '.' if !inside_brackets => {},
            _ => {
                current_entry.push(c);
            }
        }
    }

    result

}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct MetadataRecords {
    // table -> aggregation
    capabilities: HashMap<String, String>,
    // name -> metadata
    columns: HashMap<String, ColumnMetadata>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ColumnMetadata {
    name: String,
    table: String,
    datatype: String,
    ordinal: usize,
    aggregation: String,
    logical_table_id: String,
}

impl<'a, 'b> From<Node<'a, 'b>> for MetadataRecords {
    fn from(n: Node) -> Self {
        if n.get_tag() != "metadata-records" {
            info!("trying to convert a ({}) to metadata-records", n.get_tag());
            return Self::default();
        }
        let mut capabilities = HashMap::new();
        let mut columns = HashMap::new();
        n.find_tagged_children("metadata-record")
            .into_iter()
            .for_each(|c| {
                let class = c.get_attr("class");
                match class.as_str() {
                    "capability" => {
                        let agg = get_text_from_child(c, "aggregation");
                        let name = get_text_from_child(c, "parent-name");
                        capabilities.insert(name, agg);
                    },
                    "column" => {
                        let col = ColumnMetadata::from(c);
                        columns.insert(col.name.clone(), col);
                    },
                    _ => {
                        info!("found metadata for unknown class: {class}");
                    }
                }
            });
        Self {
            capabilities,
            columns,
        }
    }
}

impl<'a, 'b> From<Node<'a, 'b>> for ColumnMetadata {
    fn from(n: Node) -> Self {
        if n.get_tag() != "metadata-record" {
            info!("trying to convert a ({}) to metadata-record", n.get_tag());
            return Self::default();
        }
        Self {
            name: get_text_from_child(n, "local-name"),
            table: get_text_from_child(n, "parent-name"),
            datatype: get_text_from_child(n, "local-type"),
            ordinal: get_text_from_child(n, "ordinal").parse::<usize>().unwrap_or_default(),
            aggregation: get_text_from_child(n, "aggregation"),
            logical_table_id: get_text_from_child(n, "_.fcp.ObjectModelEncapsulateLegacy.true...object-id"),
        }
    }
}

fn get_text_from_child(n: Node, tag: &str) -> String {
    n.get_tagged_child(tag)
        .and_then(|a| a.text())
        .map(str::to_string)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;
    use super::*;

    const ATHENA: &str = "src/twb/federated.1lrlw0o15zi8pr18d68sa1qpugsd.tds";
    const SUPERSTORE: &str = "src/twb/Sample - Superstore.tds";
    const SUPERSTORE_WB: &str = "src/Superstore.twb";
    const BOOKSTORE: &str = "src/federated.1i49ou20iq1y321232eee18hvwey.tds";

    fn setup_logging() {
        tracing_subscriber::fmt::init();
    }

    #[test]
    fn test_connection_parse() {
        setup_logging();
        let mut file = File::open(BOOKSTORE).unwrap();
        let mut s = String::new();
        let _ = file.read_to_string(&mut s).unwrap();
        let doc = roxmltree::Document::parse(&s).unwrap();
        let root = doc.root();
        let root = root.find_all_tagged_decendants("datasource")[0];
        let connection = root.get_tagged_child("connection").unwrap();
        let connection_info = Connection::from(connection);
        let s = serde_json::to_string(&connection_info).unwrap();
        println!("{s}");
    }

    #[test]
    fn test_workbook_connection_parse() {
        setup_logging();
        let mut file = File::open(SUPERSTORE_WB).unwrap();
        let mut s = String::new();
        let _ = file.read_to_string(&mut s).unwrap();
        let doc = roxmltree::Document::parse(&s).unwrap();
        let root = doc.root();
        let root = root.find_all_tagged_decendants("workbook")[0];
        let sources = root.get_tagged_child("datasources").unwrap();
        let connections = sources.find_tagged_children("datasource")
            .into_iter()
            .flat_map(|d| d.get_tagged_child("connection"))
            .map(Connection::from)
            .collect::<Vec<_>>();
        let s = serde_json::to_string(&connections).unwrap();
        println!("{s}");
    }


    #[test]
    fn test() {
        let vars = vec!["[foo].[bar]", "[foo].[baz]"];
        let conv = |s: &str| {
            let val_parts = parse_identifiers(s);
            let val_tuple = if let Some((x, y)) = val_parts.into_iter().collect_tuple() {
                (x, y)
            } else {
                info!("found col value: {s} that didn't parse into 2 parts");
                return None;
            };
            Some(val_tuple)
        };
        let entries = vars.into_iter()
            .flat_map(conv)
            .collect::<Vec<_>>();
        assert_eq!(2, entries.len());
        assert_eq!(("foo".to_string(), "bar".to_string()), entries[0]);
        assert_eq!(("foo".to_string(), "baz".to_string()), entries[1]);
    }
}
