use roxmltree::Node;
use serde::{Deserialize, Serialize};
use tracing::info;
use crate::twb::{CAPTION_KEY, NAME_KEY, VERSION_KEY};
use crate::twb::datasource::columns::{ColumnDep, ColumnSet, get_column_set};
use crate::twb::datasource::connection::Connection;
use crate::twb::datasource::object_graph::ObjectGraph;
use crate::twb::xml::XmlExt;

pub mod connection;
pub mod model;
pub mod object_graph;
pub mod columns;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Datasource {
    pub name: String,
    pub version: String,
    pub caption: String,
    pub connection: Option<Connection>,
    pub column_set: ColumnSet,
    pub object_graph: ObjectGraph,
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
        }
    }
}

pub(crate) fn parse_datasources(datasources_node: Node) -> anyhow::Result<Vec<Datasource>> {
    Ok(datasources_node.find_all_tagged_decendants("datasource")
        .into_iter()
        .map(Datasource::from)
        .collect())
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
}

impl ColumnFinder for Datasource {

    fn find_column(&self, name: &str) -> Option<&str> {
        self.column_set.columns.get(name)
            .and_then(ColumnDep::get_column)
            .map(|c| if c.caption.is_empty() {
                c.name.as_str()
            } else {
                c.caption.as_str()
            })
    }

    fn find_column_for_table(&self, table: &str, name: &str) -> Option<&str> {
        // TODO
        None
    }

}

pub struct Substituter<'a, T: ColumnFinder> {
    pub finder: &'a T,
}

pub trait ColumnFinder {
    /// Given the name of a column, find the column's display name.
    /// If there is no such column, returns None.
    fn find_column(&self, name: &str) -> Option<&str>;
    /// Given the logical table name and the column name, get the display name for the
    /// column.
    fn find_column_for_table(&self, table: &str, name: &str) -> Option<&str>;
}

impl<'a, T: ColumnFinder> Substituter<'a, T> {
    /// Given the parameterized string, replace any referenced columns with their Caption'ed representation
    /// e.g. if the column: [Calc_12345] has the caption: 'Orders made', then given the string:
    /// "CEIL([Calc_12345])" we will output: "CEIL([Orders made])".
    pub fn substitute_columns(&self, s: &str) -> Option<String> {
        let mut result = String::new();
        let mut token = String::new();
        let mut col_token = String::new();
        let mut is_temp = false;
        let mut token_pend = false;
        let mut had_dot = false;

        for ch in s.chars() {
            match ch {
                '[' => {
                    if is_temp {
                        info!("found string: {s} with `[` inside of another `[`");
                        return None;
                    }
                    if token_pend && !had_dot {
                        // we have a `<token>[...`, flush the token and start a new one
                        let col= self.finder.find_column(&token)
                            .unwrap_or(&token);
                        result.push_str(col);
                        token.clear();
                        token_pend = false;
                        token.push(ch);
                    } else if token_pend { // && had_dot
                        // we are now parsing a column for the parsed table token
                        had_dot = false;
                        col_token.push(ch);
                    } else {
                        // new token
                        token.push(ch);
                    }
                    is_temp = true;
                },
                ']' => {
                    if !is_temp {
                        info!("found string: {s} with unescaped `]`");
                        return None;
                    }
                    if token_pend {
                        // we already have a table, so we are now closing the column
                        col_token.push(ch);
                        if let Some(var) = self.finder.find_column_for_table(&token, &col_token) {
                            result.push_str(var);
                        } else {
                            result.push_str(&format!("{token}.{col_token}"));
                        }
                        token.clear();
                        col_token.clear();
                        token_pend = false;
                    } else {
                        token.push(ch);
                        token_pend = true;

                    }
                    is_temp = false;
                },
                '.' => {
                    if is_temp {
                        // inside of either token or col_token
                        if token_pend {
                            col_token.push(ch);
                        } else {
                            token.push(ch);
                        }
                    } else if token_pend && had_dot {
                        // we have `<token>..` flush the token and both dots.
                        let col= self.finder.find_column(&token)
                            .unwrap_or(&token);
                        result.push_str(col);
                        token.clear();
                        token_pend = false;
                        result.push_str("..");
                    } else if token_pend { // && !had_dot
                        // we have `<token>.` we are now expecting the col_token to fill up.
                        had_dot = true;
                    } else { // !token_pend
                        result.push(ch);
                    }
                },
                _ => {
                    if is_temp {
                        // inside of either token or col_token
                        if token_pend {
                            col_token.push(ch);
                        } else {
                            token.push(ch);
                        }
                    } else if token_pend {
                        // we have: <token><ch>, so flush token
                        let col= self.finder.find_column(&token)
                            .unwrap_or(&token);
                        result.push_str(col);
                        token.clear();
                        token_pend = false;
                        result.push(ch);
                    } else {
                        result.push(ch);
                    }
                }
            }
        }
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
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

    impl ColumnFinder for HashMap<(&str, &str), &str> {
        fn find_column(&self, name: &str) -> Option<&str> {
            self.iter()
                .filter_map(|((_, col), sub)| (*col == name).then_some(*sub))
                .next()
        }

        fn find_column_for_table(&self, table: &str, name: &str) -> Option<&str> {
            self.get(&(table, name)).copied()
        }
    }

    #[test]
    fn test_substituter() {
        let m = HashMap::from([
            (("[t1]", "[col1]"), "[val1]"),
            (("[t1]", "[col2]"), "[val2]"),
            (("[t2]", "[col3]"), "[val3]"),
            (("[t3]", "[col3]"), "[val4]"),
            (("[t4.csv]", "[col.1]"), "[val.3]"),
        ]);

        let sub = Substituter {
            finder: &m,
        };

        let cases = [
            ("OP([col1])", "OP([val1])"),
            ("Call([t1].[col2]) + 4", "Call([val2]) + 4"),
            ("Call([t1].[col2]) + [t3].[col3][t1].[col1]", "Call([val2]) + [val4][val1]"),
            ("SUB([col2],[col1]) + 3.55 - [t4.csv].[col.1]", "SUB([val2],[val1]) + 3.55 - [val.3]"),
        ];

        for (pre, post) in cases {
            let res = sub.substitute_columns(pre).unwrap();
            assert_eq!(post, res.as_str());
        }

    }
}
