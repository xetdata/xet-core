use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use crate::twb::datasource::{Datasource, Substituter};

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct WorkbookDatasource {
    name: String,
    version: String,
    // #[serde(skip_serializing_if = "Vec::is_empty")]
    tables: Vec<Table>,
    // #[serde(skip_serializing_if = "Option::is_none")]
    added_columns: Option<Table>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Table {
    name: String,
    // #[serde(skip_serializing_if = "Vec::is_empty")]
    dimensions: Vec<Column>,
    // #[serde(skip_serializing_if = "Vec::is_empty")]
    measures: Vec<Column>,
}

// TODO: can't skip serialization or else bincode serialization (used for db file) will
//       blow up.
#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Column {
    name: String,
    // maybe enum of types?
    datatype: String,
    generated: bool,
    // #[serde(skip_serializing_if = "Option::is_none")]
    formula: Option<String>,
    // #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    // #[serde(skip_serializing_if = "Vec::is_empty")]
    drilldown: Vec<Column>,
    // #[serde(skip)]
    table: Option<String>,
    // #[serde(skip)]
    is_dimension: bool,
}

impl Column {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

impl From<&Datasource> for WorkbookDatasource {
    fn from(source: &Datasource) -> Self {
        let name = get_name_or_caption(&source.name, &source.caption);
        let (tables, added_columns) = parse_tables(source);
        Self {
            name,
            version: source.version.clone(),
            tables,
            added_columns,
        }
    }
}

fn parse_tables(datasource: &Datasource) -> (Vec<Table>, Option<Table>) {
    let substituter = Substituter {
        finder: datasource,
    };
    let mut columns = HashMap::new();
    let mut dep_columns = HashMap::new();
    let mut any_geo = false;

    let drill_columns = datasource.column_set.drill_paths
        .iter()
        .map(|d| {
            columns.insert(d.name.clone(), Column {
                name: d.name.clone(),
                datatype: "drilldown".to_string(),
                is_dimension: true,
                drilldown: vec![Column::default(); d.fields.len()],
                ..Default::default()
            });
            d
        })
        .flat_map(|d| d.fields.iter().enumerate()
            .map(|(i, f)| (f, (d.name.as_str(), i))))
        .collect::<HashMap<_, _>>();

    datasource.column_set.columns
        .values()
        .filter_map(|col| col.get_column())
        .filter(|c| !c.hidden)
        .for_each(|col| {
            let (formula, dep_cols) = col.formula
                .as_ref()
                .map(|f| substituter.substitute_columns(f))
                .unwrap_or((col.formula.clone(), vec![]));
            // if there are no dependencies, then we find the table this column belongs to
            // or else, we will need to use dependencies to identify the table (if any).
            let table = dep_cols.is_empty()
                .then(|| datasource.find_table(&col.name));
            if !dep_cols.is_empty() {
                dep_columns.insert(col.name.clone(), dep_cols);
            }
            let datatype = col.aggregate_from
                .as_ref()
                .and_then(|f| datasource.column_set.columns.get(f))
                .and_then(|agg_col| agg_col.get_column())
                .map(|agg_col_meta| agg_col_meta.datatype.clone())
                .unwrap_or(col.datatype.clone());

            if datatype == "geo" {
                any_geo = true;
            }
            let c = Column {
                name: get_name_or_caption(&col.name, &col.caption),
                datatype,
                table: table.clone(),
                is_dimension: col.role == "dimension",
                formula,
                value: col.value.clone(),
                drilldown: vec![],
                generated: false,
            };
            if let Some((drill_col, idx)) = drill_columns.get(&col.name) {
                if let Some(col) = columns.get_mut(*drill_col) {
                    if *idx >= col.drilldown.len() {
                        error!("BUG: drilldown not sized properly");
                        return;
                    }
                    col.drilldown[*idx] = c;
                    if col.table.is_none() {
                        col.table = table;
                    }
                } else {
                    info!("Found drilldown: {drill_col} not in the column map");
                }
            } else {
                columns.insert(col.name.clone(), c);
            }
        });

    // try to add any unchanged columns not found in the column_set (i.e. those in metadata)
    let m = datasource.connection
        .as_ref()
        .map(|c| &c.metadata_records.columns)
        .unwrap_or(&HashMap::new())
        .iter()
        .filter(|(k, _)| !columns.contains_key(*k))
        .filter(|(k, _)| !drill_columns.contains_key(*k))
        .map(|(k, c)| (k.clone(), Column {
            name: strip_brackets(k),
            datatype: c.datatype.clone(),
            generated: false,
            formula: None,
            value: None,
            drilldown: vec![],
            table: Some(c.table.clone()),
            is_dimension: !matches!(c.datatype.as_str(), "integer" | "real"),
        })).collect::<HashMap<_, _>>();
    columns.extend(m);


    // update tables based on dependent columns
    for col in dep_columns.keys() {
        _ = update_table_from_deps(&mut columns, &dep_columns, col)
    }

    if any_geo {
        let lat_measure = Column {
            name: "Latitude (generated)".to_string(),
            datatype: "geo".to_string(),
            generated: true,
            table: Some(String::default()),
            is_dimension: false,
            ..Default::default()
        };
        let lon_measure = Column {
            name: "Longitude (generated)".to_string(),
            datatype: "geo".to_string(),
            generated: true,
            table: Some(String::default()),
            is_dimension: false,
            ..Default::default()
        };
        columns.insert(lat_measure.name.clone(), lat_measure);
        columns.insert(lon_measure.name.clone(), lon_measure);
    }
    let mut tables: HashMap<String, HashMap<bool, Vec<Column>>> = HashMap::new();

    for (_, col) in columns.into_iter() {
        let table_name = &col.table.clone().unwrap_or_default();
        if let Some(col_map) = tables.get_mut(table_name) {
            if let Some(col_list) = col_map.get_mut(&col.is_dimension) {
                col_list.push(col);
            } else {
                col_map.insert(col.is_dimension, vec![col]);
            }
        } else {
            let col_map = HashMap::from([(col.is_dimension, vec![col])]);
            tables.insert(table_name.clone(), col_map);
        }
    }

    let tlen = tables.len();
    info!("create vec with capacity: {tlen}");
    let mut table_list = Vec::with_capacity(tables.len());
    let mut added_table = None;

    for (name, mut col_map) in tables {
        let mut dimensions = col_map.remove(&true).unwrap_or_default();
        dimensions.sort_by_key(Column::get_name);
        let mut measures = col_map.remove(&false).unwrap_or_default();
        if let Some(agg) = datasource.get_table_aggregation(&name) {
            let name_str = strip_brackets(&name);
            measures.push(Column {
                name: format!("{name_str} ({agg})"),
                datatype: "numeric".to_string(),
                generated: true,
                ..Default::default()
            });
        }
        measures.sort_by_key(Column::get_name);
        let table = Table {
            name: strip_brackets(&name),
            dimensions,
            measures,
        };
        if name.is_empty() {
            added_table = Some(table);
        } else {
            table_list.push(table);
        }
    }
    table_list.sort_by_key(|t|t.name.clone());

    (table_list, added_table)
}

fn update_table_from_deps(columns: &mut HashMap<String, Column>, dep_columns: &HashMap<String, Vec<(String, String)>>, col: &str) -> Option<String> {
    let candidate = if let Some(col_meta) = columns.get_mut(col) {
        if col_meta.table.is_some() {
            return col_meta.table.clone()
        }
        // no table, generate and update the table, but first, update this column to ""
        // in case there is an unexpected cycle.
        col_meta.table = Some("".to_string());
        let mut candidate = None;
        for (ds, c) in dep_columns.get(col).unwrap_or(&vec![]) {
            if !ds.is_empty() {
                // dependent on foreign datasource, col has no table
                return Some("".to_string());
            }
            if let Some(dep_table) = update_table_from_deps(columns, dep_columns, c) {
                candidate = match candidate {
                    None => Some(dep_table),
                    Some(t) if t == dep_table => Some(t),
                    _ => Some("".to_string()), // Some(t) if t != dep_table
                }
            }
        }
        if candidate.is_none() {
            candidate = Some("".to_string());
        }
        candidate
    } else {
        None
    };
    if let Some(col_meta) = columns.get_mut(col) {
        col_meta.table = candidate.clone();
    }
    candidate
}

pub fn get_name_or_caption(name: &str, caption: &str) -> String {
    let s = if caption.is_empty() {
        name
    } else {
        caption
    };
    strip_brackets(s)
}

fn strip_brackets(s: &str) -> String {
    s.trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned()
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;
    use crate::twb::datasource::parse_datasources;
    use crate::twb::xml::XmlExt;
    use super::*;

    #[test]
    fn test_build() {
        let mut file = File::open("src/Superstore.twb").unwrap();
        let mut s = String::new();
        let _ = file.read_to_string(&mut s).unwrap();
        let doc = roxmltree::Document::parse(&s).unwrap();
        let root = doc.root();
        let root = root.find_all_tagged_decendants("workbook")[0];
        let datasources = root.get_tagged_child("datasources").unwrap();
        let data = parse_datasources(datasources).unwrap()
            .iter()
            .map(WorkbookDatasource::from)
            .collect::<Vec<_>>();
        let s = serde_json::to_string(&data).unwrap();
        println!("{s}");
        assert_eq!("Sales Commission", data[2].name);
    }
}
