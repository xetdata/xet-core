use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct WorkbookDatasource {
    name: String,
    version: String,
    tables: Vec<Table>,
    added_columns: Table,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Table {
    name: String,
    dimensions: Vec<Column>,
    measures: Vec<Column>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Column {
    name: String,
    // maybe enum of types?
    data_type: String,
    formula: Option<String>,
    value: Option<String>,
    drilldown: Vec<Column>,
}
