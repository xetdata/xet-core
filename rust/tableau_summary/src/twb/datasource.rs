use std::borrow::Cow;
use std::collections::HashMap;
use roxmltree::Node;
use serde::{Deserialize, Serialize};
use tracing::info;
use crate::twb::{CAPTION_KEY, NAME_KEY, VERSION_KEY};
use crate::twb::datasource::columns::{ColumnDep, ColumnMeta, ColumnSet, get_column_set};
use crate::twb::datasource::connection::Connection;
use crate::twb::datasource::dep::Dep;
use crate::twb::datasource::object_graph::ObjectGraph;
use crate::twb::xml::XmlExt;

pub mod connection;
pub mod model;
pub mod object_graph;
pub mod columns;
pub mod dep;

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

/// returns the caption for the dependency
fn get_table_captioned(dep: &Dep) -> &str {
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

    fn find_column_for_table(&self, table: &str, name: &str) -> Option<Cow<str>> {
        if let Some(dep) = self.dependencies.get(strip_brackets(table)) {
            let tname = get_table_captioned(dep);
            return dep.columns.get(name)
                .or_else(|| self.column_set.columns.get(name))
                .and_then(ColumnDep::get_column)
                .map(get_column_captioned)
                .map(|c|format!("[{}].{}", tname, c.as_ref()))
                .map(Cow::from);
        }
        None
    }

}

pub struct Substituter<'a, T: ColumnFinder> {
    pub finder: &'a T,
}

pub trait ColumnFinder {
    /// Given the name of a column, find the column's display name.
    /// If there is no such column, returns None.
    fn find_column(&self, name: &str) -> Option<Cow<str>>;
    /// Given the logical table name and the column name, get the display name for the
    /// column.
    fn find_column_for_table(&self, table: &str, name: &str) -> Option<Cow<str>>;
}

impl<'a, T: ColumnFinder> Substituter<'a, T> {
    /// Given the parameterized string, replace any referenced columns with their Caption'ed representation
    /// e.g. if the column: `[Calc_12345]` has the caption: `'Orders made'`, then given the string:
    /// `"CEIL([Calc_12345])"` we will output: `"CEIL([Orders made])"`.
    pub fn substitute_columns(&self, s: &str) -> (Option<String>, Vec<(String, String)>) {
        let mut result = String::new();
        let mut token = String::new();
        let mut col_token = String::new();
        let mut is_temp = false;
        let mut token_pend = false;
        let mut had_dot = false;
        let mut dependencies: Vec<(String, String)> = vec![];

        for ch in s.chars() {
            match ch {
                '[' => {
                    if is_temp {
                        info!("found string: {s} with `[` inside of another `[`");
                        return (None, vec![]);
                    }
                    if token_pend && !had_dot {
                        // we have a `<token>[...`, flush the token and start a new one
                        let col= self.finder.find_column(&token)
                            .unwrap_or(Cow::from(&token));
                        result.push_str(col.as_ref());
                        dependencies.push(("".to_string(), token));
                        token = String::new();
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
                        return (None, vec![]);
                    }
                    if token_pend {
                        // we already have a table, so we are now closing the column
                        col_token.push(ch);
                        if let Some(var) = self.finder.find_column_for_table(&token, &col_token) {
                            result.push_str(var.as_ref());
                        } else {
                            result.push_str(&format!("{token}.{col_token}"));
                        }
                        dependencies.push((token, col_token));
                        token = String::new();
                        col_token = String::new();
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
                            .unwrap_or(Cow::from(&token));
                        result.push_str(col.as_ref());
                        dependencies.push(("".to_string(), token));
                        token = String::new();
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
                            .unwrap_or(Cow::from(&token));
                        result.push_str(col.as_ref());
                        dependencies.push(("".to_string(), token));
                        token = String::new();
                        token_pend = false;
                        result.push(ch);
                    } else {
                        result.push(ch);
                    }
                }
            }
        }
        (Some(result), dependencies)
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
        fn find_column(&self, name: &str) -> Option<Cow<str>> {
            self.iter()
                .filter_map(|((_, col), sub)| (*col == name).then_some(*sub))
                .map(Cow::from)
                .next()
        }

        fn find_column_for_table(&self, table: &str, name: &str) -> Option<Cow<str>> {
            self.get(&(table, name)).copied().map(Cow::from)
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
            let (res, _) = sub.substitute_columns(pre);
            let res = res.unwrap();
            assert_eq!(post, res.as_str());
        }

    }
}
