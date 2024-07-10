use crate::check_tag_or_default;
use crate::twb::raw::datasource::substituter::ColumnFinder;
use crate::twb::summary::util;
use crate::twb::{CAPTION_KEY, NAME_KEY};
use crate::xml::XmlExt;
use error_printer::ErrorPrinter;
use itertools::Itertools;
use once_cell::sync::Lazy;
use regex::Regex;
use roxmltree::Node;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use tracing::info;

/// Tableau uses the following tag to indicate the Relations section of the connection.
const TABLEAU_RELATION_TAG: &str = "_.fcp.ObjectModelEncapsulateLegacy.true...relation";

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Connection {
    pub named_connections: HashMap<String, NamedConnection>,
    pub relations: Relations,
    pub cols: ColMapping,
    pub metadata_records: MetadataRecords,
}

impl<'a, 'b> From<Node<'a, 'b>> for Connection {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "connection");
        let named_connections = n
            .get_tagged_child("named-connections")
            .into_iter()
            .flat_map(|c| c.find_tagged_children("named-connection"))
            .map(NamedConnection::from)
            .map(|nc| (nc.name.clone(), nc))
            .collect();
        let relations = n
            .get_tagged_child("_.fcp.ObjectModelEncapsulateLegacy.true...relation")
            .map(Relations::from)
            .unwrap_or_default();
        let cols = n
            .get_tagged_child("cols")
            .map(ColMapping::from)
            .unwrap_or_default();
        let metadata_records = n
            .get_tagged_child("metadata-records")
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
    pub name: String,
    pub caption: String,
    pub class: String,
    pub filename: Option<String>,
}

impl<'a, 'b> From<Node<'a, 'b>> for NamedConnection {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "named-connection");
        let (class, filename) = n
            .get_tagged_child("connection")
            .map(|c| {
                let class = c.get_attr("class");
                let filename = c.get_maybe_attr("filename").map(|filename| {
                    c.get_maybe_attr("directory")
                        .map(|dir| format!("{dir}/{filename}"))
                        .unwrap_or(filename)
                });
                (class, filename)
            })
            .unwrap_or_default();
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
    pub tables: HashMap<String, Table>,
    pub joins: HashMap<String, Join>,
    pub unions: HashMap<String, Union>,
}

impl<'a, 'b> From<Node<'a, 'b>> for Relations {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, TABLEAU_RELATION_TAG);
        let rel_type = n.get_attr("type");

        match rel_type.as_str() {
            "collection" => {
                let mut tables = HashMap::new();
                let mut joins = HashMap::new();
                let mut unions = HashMap::new();

                let relations = n
                    .find_tagged_children("relation")
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
            }
            "table" => {
                let table: Table = n.into();
                let tables = HashMap::from([(table.name.clone(), table)]);
                Self {
                    tables,
                    ..Default::default()
                }
            }
            _ => {
                info!("unknown type: {rel_type} for relations node");
                Self::default()
            }
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub enum Relation {
    #[default]
    Unknown,
    Table(Table),
    Join(Join),
    Union(Union),
}

impl<'a, 'b> From<Node<'a, 'b>> for Relation {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "relation");
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

impl Relation {
    fn get_columns(&self) -> Vec<&HashMap<String, TableColumn>> {
        match self {
            Relation::Unknown => vec![],
            Relation::Table(t) => vec![&t.columns],
            Relation::Join(j) => j.tables.values().map(|t| &t.columns).collect_vec(),
            Relation::Union(u) => vec![&u.union_table.columns],
        }
    }
}

impl ColumnFinder for Relation {
    fn find_column(&self, name: &str) -> Option<Cow<str>> {
        let name = util::strip_brackets(name);
        self.get_columns()
            .iter()
            .filter_map(|m| m.get(&name))
            .map(|c| Cow::Borrowed(c.name.as_str()))
            .next()
    }

    fn find_column_for_source(&self, _source: &str, name: &str) -> Option<Cow<str>> {
        self.find_column(name)
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Table {
    pub name: String,
    pub connection: String,
    pub columns: HashMap<String, TableColumn>,
}

impl<'a, 'b> From<Node<'a, 'b>> for Table {
    fn from(n: Node) -> Self {
        if n.get_tag() != "relation" && n.get_tag() != TABLEAU_RELATION_TAG {
            info!("trying to convert a ({}) to a table relation", n.get_tag());
            return Self::default();
        }
        let columns = n
            .get_tagged_child("columns")
            .into_iter()
            .flat_map(|c| c.find_tagged_children("column"))
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

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Join {
    pub join_type: String,
    pub clause: Clause,
    pub tables: HashMap<String, Table>,
}

impl<'a, 'b> From<Node<'a, 'b>> for Join {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "relation");
        let tables = n
            .find_tagged_children("relation")
            .into_iter()
            .map(Table::from)
            .map(|table| (table.name.clone(), table))
            .collect();
        let clause = n
            .get_tagged_child("clause")
            .map(Clause::from)
            .unwrap_or_default();
        Self {
            join_type: n.get_attr("join"),
            clause,
            tables,
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Union {
    pub name: String,
    pub all: bool,
    pub union_table: Table,
    pub tables: HashMap<String, Table>,
}

impl<'a, 'b> From<Node<'a, 'b>> for Union {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "relation");
        let tables = n
            .find_tagged_children("relation")
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

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Clause {
    pub clause_type: String,
    pub expression: Expression,
}

impl<'a, 'b> From<Node<'a, 'b>> for Clause {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "clause");
        Self {
            clause_type: n.get_attr("type"),
            expression: n
                .get_tagged_child("expression")
                .map(Expression::from)
                .unwrap_or_default(),
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Expression {
    pub op: String,
    pub expressions: Vec<Expression>,
}

impl<'a, 'b> From<Node<'a, 'b>> for Expression {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "expression");
        let expressions = n
            .find_tagged_children("expression")
            .into_iter()
            .map(Expression::from)
            .collect();

        Self {
            op: n.get_attr("op"),
            expressions,
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TableColumn {
    pub name: String,
    pub datatype: String,
    pub ordinal: usize,
}

impl<'a, 'b> From<Node<'a, 'b>> for TableColumn {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "column");
        let ordinal = n
            .get_maybe_attr("ordinal")
            .map(|s| {
                s.parse()
                    .log_error("ordinal not a number")
                    .unwrap_or_default()
            })
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
    pub cols: HashMap<String, (String, String)>,
}

impl<'a, 'b> From<Node<'a, 'b>> for ColMapping {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "cols");
        let cols = n
            .find_tagged_children("map")
            .into_iter()
            .flat_map(map_node_to_kv)
            .collect();
        Self { cols }
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

/// Regex to extract the `[...]` items from a string.
static IDENTIFIER_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\[([^]]+)]").unwrap() // tests ensure that regex string is always valid
});

// Given a string like `[foo].[bar]` return ["foo", "bar"]
pub fn parse_identifiers(s: &str) -> Vec<String> {
    IDENTIFIER_REGEX
        .captures_iter(s)
        .map(|cap| cap[1].to_string())
        .collect()
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct MetadataRecords {
    // table -> aggregation
    pub capabilities: HashMap<String, String>,
    // name -> metadata
    pub columns: HashMap<String, ColumnMetadata>,
}

impl<'a, 'b> From<Node<'a, 'b>> for MetadataRecords {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "metadata-records");
        let mut capabilities = HashMap::new();
        let mut columns = HashMap::new();
        for c in n.find_tagged_children("metadata-record").into_iter() {
            let class = c.get_attr("class");
            match class.as_str() {
                "capability" => {
                    let agg = get_text_from_child(c, "aggregation");
                    let name = get_text_from_child(c, "parent-name");
                    capabilities.insert(name, agg);
                }
                "column" | "measure" => {
                    let col = ColumnMetadata::from(c);
                    columns.insert(col.name.clone(), col);
                }
                _ => {
                    info!("found metadata for unknown class: {class}");
                }
            }
        }
        Self {
            capabilities,
            columns,
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ColumnMetadata {
    pub name: String,
    pub table: String,
    pub datatype: String,
    pub ordinal: usize,
    pub aggregation: String,
    pub logical_table_id: String,
}

impl<'a, 'b> From<Node<'a, 'b>> for ColumnMetadata {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "metadata-record");
        Self {
            name: get_text_from_child(n, "local-name"),
            table: get_text_from_child(n, "parent-name"),
            datatype: get_text_from_child(n, "local-type"),
            ordinal: get_text_from_child(n, "ordinal")
                .parse::<usize>()
                .unwrap_or_default(),
            aggregation: get_text_from_child(n, "aggregation"),
            logical_table_id: get_text_from_child(
                n,
                "_.fcp.ObjectModelEncapsulateLegacy.true...object-id",
            ),
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
    use super::*;

    #[test]
    fn test_parse_identifiers() {
        let vars = vec!["[foo].[bar]", "[foo].[baz]", "[a.txt].[col.b]"];
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
        let entries = vars.into_iter().flat_map(conv).collect::<Vec<_>>();
        assert_eq!(3, entries.len());
        assert_eq!(("foo".to_string(), "bar".to_string()), entries[0]);
        assert_eq!(("foo".to_string(), "baz".to_string()), entries[1]);
        assert_eq!(("a.txt".to_string(), "col.b".to_string()), entries[2]);
    }
}
