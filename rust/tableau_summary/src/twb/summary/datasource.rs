use std::collections::HashMap;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::twb::raw::datasource::connection::Expression;
use crate::twb::raw::datasource::object_graph::{Relationship, TableauObject};
use crate::twb::raw::datasource::substituter::ColumnFinder;
use crate::twb::raw::datasource::{substituter, RawDatasource};
use crate::twb::summary::util;

#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
pub struct DatasourceV1 {
    pub name: String,
    pub version: String,
    pub tables: Vec<Table>,
    pub added_columns: Option<Table>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
pub struct Datasource {
    pub name: String,
    pub version: String,
    pub tables: Vec<Table>,
    pub added_columns: Option<Table>,
    pub relations: Vec<TableRelationship>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
pub struct Table {
    pub name: String,
    pub dimensions: Vec<Column>,
    pub measures: Vec<Column>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
pub struct Column {
    pub name: String,
    pub datatype: String,
    pub generated: bool,
    pub formula: Option<String>,
    pub value: Option<String>,
    pub drilldown: Vec<Column>,
    pub table: Option<String>,
    pub is_dimension: bool,
}

impl Column {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

impl From<&RawDatasource> for DatasourceV1 {
    fn from(source: &RawDatasource) -> Self {
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

impl From<&RawDatasource> for Datasource {
    fn from(source: &RawDatasource) -> Self {
        let name = get_name_or_caption(&source.name, &source.caption);
        let (tables, added_columns) = parse_tables(source);
        let relations = parse_relationships(source);
        Self {
            name,
            version: source.version.clone(),
            tables,
            added_columns,
            relations,
        }
    }
}

impl From<&DatasourceV1> for Datasource {
    fn from(d1: &DatasourceV1) -> Self {
        Self {
            name: d1.name.clone(),
            version: d1.version.clone(),
            tables: d1.tables.clone(),
            added_columns: d1.added_columns.clone(),
            relations: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
pub struct TableRelationship {
    pub table1: String,
    pub table2: String,
    pub condition: String,
}

fn parse_tables(datasource: &RawDatasource) -> (Vec<Table>, Option<Table>) {
    // Map<col_name, Column>
    let mut columns = HashMap::new();
    // Map<col_name, Vec<(datasource, dep_col_name)>
    let mut dep_columns = HashMap::new();
    let mut any_geo = false;

    // setup drill columns:
    // - update columns map with the drill columns populated with an empty list of drilldown columns
    // - create a lookup Map<col, (drill_col, idx)>
    let drill_columns = datasource
        .column_set
        .drill_paths
        .iter()
        .map(|d| {
            columns.insert(
                d.name.clone(),
                Column {
                    name: d.name.clone(),
                    datatype: "drilldown".to_string(),
                    is_dimension: true,
                    drilldown: vec![Column::default(); d.fields.len()],
                    ..Default::default()
                },
            );
            d
        })
        .flat_map(|d| {
            d.fields
                .iter()
                .enumerate()
                .map(|(i, f)| (f, (d.name.as_str(), i)))
        })
        .collect::<HashMap<_, _>>();

    // Go through column_set and update `colums` Map with column data
    datasource
        .column_set
        .columns
        .values()
        .filter_map(|col| col.get_column()) // only columns, no column_instances or groups
        .filter(|c| !c.hidden) // no hidden columns
        .for_each(|col| {
            let (formula, dep_cols) = col
                .formula
                .as_ref()
                .map(|f| substituter::substitute_columns(datasource, f))
                .unwrap_or((col.formula.clone(), vec![]));
            // if there are no dependencies, then we find the table this column belongs to
            // or else, we will need to use dependencies to identify the table (if any).
            let table = dep_cols
                .is_empty()
                .then(|| datasource.find_table(&col.name));
            if !dep_cols.is_empty() {
                dep_columns.insert(col.name.clone(), dep_cols);
            }
            // If this column is aggregated from a different column, we try to match the
            // datatype of that column instead of the one specified.
            let datatype = col
                .aggregate_from
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
            // Check to see if this column is part of a drilldown. If so, insert into the
            // drilldown's column list instead of the map.
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
    let m = datasource
        .connection
        .as_ref()
        .map(|c| &c.metadata_records.columns)
        .unwrap_or(&HashMap::new())
        .iter()
        .filter(|(k, _)| !columns.contains_key(*k) && !drill_columns.contains_key(*k))
        .map(|(k, c)| {
            (
                k.clone(),
                Column {
                    name: util::strip_brackets(k),
                    datatype: c.datatype.clone(),
                    generated: false,
                    formula: None,
                    value: None,
                    drilldown: vec![],
                    table: Some(c.table.clone()),
                    is_dimension: !matches!(c.datatype.as_str(), "integer" | "real"),
                },
            )
        })
        .collect::<HashMap<_, _>>();
    columns.extend(m);

    // update tables based on dependent columns
    for col in dep_columns.keys() {
        _ = update_table_from_deps(&mut columns, &dep_columns, col)
    }

    // If we have any geographical columns, we should create the auto-generated
    // lat/lon metrics.
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
    // group columns by table and is_dimension:
    // Map<table_display_name, Map<is_dimension, Vec<column>>>
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

    // Build list of logical tables in the datasource
    let mut table_list = Vec::with_capacity(tables.len());
    // For calculations without a logical table
    let mut added_table = None;

    for (name, mut col_map) in tables {
        // expect to display dimensions and measures in a sorted way.
        let mut dimensions = col_map.remove(&true).unwrap_or_default();
        dimensions.sort_by_key(Column::get_name);
        let mut measures = col_map.remove(&false).unwrap_or_default();
        // add the table-level auto-generated aggregation "measure"
        if let Some(agg) = datasource.get_table_aggregation(&name) {
            let name_str = util::strip_brackets(&name);
            measures.push(Column {
                name: format!("{name_str} ({agg})"),
                datatype: "numeric".to_string(),
                generated: true,
                ..Default::default()
            });
        }
        measures.sort_by_key(Column::get_name);
        let table = Table {
            name: util::strip_brackets(&name),
            dimensions,
            measures,
        };
        if name.is_empty() {
            // this "table" is for extra calculations.
            added_table = Some(table);
        } else {
            table_list.push(table);
        }
    }
    // table list should be sorted
    table_list.sort_by_key(|t| t.name.clone());

    (table_list, added_table)
}

fn parse_relationships(datasource: &RawDatasource) -> Vec<TableRelationship> {
    datasource
        .object_graph
        .relationships
        .iter()
        .map(|relationship| TableRelationship {
            table1: find_table_name(&datasource.object_graph.objects, &relationship.id1),
            table2: find_table_name(&datasource.object_graph.objects, &relationship.id2),
            condition: parse_expression(datasource, relationship, 0, &relationship.expression),
        })
        .collect_vec()
}

fn find_table_name(tables: &HashMap<String, TableauObject>, id: &String) -> String {
    tables
        .get(id)
        .filter(|&obj| !obj.caption.is_empty())
        .map(|obj| obj.caption.clone())
        .unwrap_or(id.clone())
}

fn parse_expression(
    source: &RawDatasource,
    root_relation: &Relationship,
    side: u8,
    expression: &Expression,
) -> String {
    let op = expression.op.as_str();
    match op {
        "=" | "<=" | "<" | ">=" | ">" | "<>" | "AND" | "OR" => {
            if expression.expressions.len() != 2 {
                warn!(
                    "Expression op: {op} doesn't have 2 operators ({})",
                    expression.expressions.len()
                );
                return "".to_string();
            }
            let op1 = &expression.expressions[0];
            let op2 = &expression.expressions[1];
            format!(
                "{} {op} {}",
                parse_expression(source, root_relation, 1, op1),
                parse_expression(source, root_relation, 2, op2)
            )
        }
        _ => {
            // op is a column name. Try to find first from the datasource column map, then from the
            // logical table in the root_relation.
            // This order is preferred since the column map will have any aliases/captions for the column
            // vs the raw column name.
            if let Some(col) = source.find_column(op) {
                // returned column could just be the name and thus have brackets.
                util::strip_brackets(col)
            } else if side == 0 {
                warn!("Ambiguous relationship with only one side");
                util::strip_brackets(op)
            } else {
                let table_id = match side {
                    1 => &root_relation.id1,
                    2 => &root_relation.id2,
                    _ => {
                        return util::strip_brackets(op);
                    }
                };
                if let Some(object_relation) = source
                    .object_graph
                    .objects
                    .get(table_id)
                    .map(|t| &t.relation)
                {
                    if let Some(col) = object_relation.find_column(op) {
                        return col.to_string();
                    }
                }
                // May want to search the datasource metadata columns, but stripping the brackets
                // is probably sufficient as a display name
                warn!("Could not resolve {op}");
                util::strip_brackets(op)
            }
        }
    }
}

/// Given the map of columns, the dependencies between columns, and a column name,
/// recursively try to identify the logical table that the column should belong to.
/// A calculated column should be assigned to some table if "all" transitive dependent
/// columns are part of that table. If there are any mismatches, then the column is
/// assigned to the `""` table (i.e. it is an added calculation).
/// In addition to updating the Column object with the table, we also return the
/// table to aid any callers.
fn update_table_from_deps(
    columns: &mut HashMap<String, Column>,
    dep_columns: &HashMap<String, Vec<(String, String)>>,
    col: &str,
) -> Option<String> {
    let candidate = if let Some(col_meta) = columns.get_mut(col) {
        if col_meta.table.is_some() {
            return col_meta.table.clone();
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
    let s = if caption.is_empty() { name } else { caption };
    util::strip_brackets(s)
}
