use serde::{Deserialize, Serialize};
use crate::twb::diff::util::{ChangeMap, ChangeState, DiffItem, DiffProducer};
use crate::twb::summary::datasource::{Column, Datasource, Table, TableRelationship};

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct DatasourceDiff {
    pub status: ChangeState,
    pub changes: ChangeMap,
    pub name: DiffItem<String>,
    pub version: DiffItem<String>,
    pub tables: Vec<TableDiff>,
    pub added_columns: Option<TableDiff>,
    pub relations: Vec<DiffItem<TableRelationship>>,
}

impl DatasourceDiff {
    fn calculate_change_map(&mut self) {
        self.changes.update(&self.name);
        self.changes.update(&self.version);
        self.tables.iter()
            .for_each(|t|self.changes.merge(&t.changes));
        self.added_columns.iter()
            .for_each(|t| self.changes.merge(&t.changes));
        self.changes.update_list(&self.relations);
    }
}

impl DiffProducer<Datasource> for DatasourceDiff {
    fn new_addition(item: &Datasource) -> Self {
        let mut diff = DatasourceDiff {
            status: ChangeState::Add,
            changes: ChangeMap::default(),
            name: DiffItem::new_addition(&item.name),
            version: DiffItem::new_addition(&item.version),
            tables: TableDiff::new_addition_list(&item.tables),
            added_columns: item.added_columns.as_ref().map(TableDiff::new_addition),
            relations: DiffItem::new_addition_list(&item.relations),
        };
        diff.calculate_change_map();
        diff
    }

    fn new_deletion(item: &Datasource) -> Self {
        let mut diff = DatasourceDiff {
            status: ChangeState::Delete,
            changes: ChangeMap::default(),
            name: DiffItem::new_deletion(&item.name),
            version: DiffItem::new_deletion(&item.version),
            tables: TableDiff::new_deletion_list(&item.tables),
            added_columns: item.added_columns.as_ref().map(TableDiff::new_deletion),
            relations: DiffItem::new_deletion_list(&item.relations),
        };
        diff.calculate_change_map();
        diff
    }

    fn new_diff(before: &Datasource, after: &Datasource) -> Self {
        let added_columns = match (&before.added_columns, &after.added_columns) {
            (None, None) => None,
            (Some(bt), None) => Some(TableDiff::new_deletion(bt)),
            (None, Some(at)) => Some(TableDiff::new_addition(at)),
            (Some(bt), Some(at)) => Some(TableDiff::new_diff(bt, at))
        };
        let mut diff = DatasourceDiff {
            status: ChangeState::Change,
            changes: ChangeMap::default(),
            name: DiffItem::new_diff(&before.name,&after.name),
            version: DiffItem::new_diff(&before.version, &after.version),
            tables: TableDiff::new_diff_list(&before.tables, &after.tables),
            added_columns,
            relations: DiffItem::new_unique_diff_list(&before.relations, &after.relations, |r| format!("{}_{}", r.table1, r.table2)),
        };
        diff.calculate_change_map();
        if diff.changes.is_empty() {
            diff.status = ChangeState::None
        }
        diff
    }
}


#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TableDiff {
    pub status: ChangeState,
    pub changes: ChangeMap,
    pub name: DiffItem<String>,
    pub dimensions: Vec<ColumnDiff>,
    pub measures: Vec<ColumnDiff>,
}

impl TableDiff {
    fn calculate_change_map(&mut self) {
        self.changes.update(&self.name);
        self.dimensions.iter()
            .for_each(|d| self.changes.merge(&d.changes));
        self.measures.iter()
            .for_each(|m| self.changes.merge(&m.changes));
    }
}

impl DiffProducer<Table> for TableDiff {
    fn new_addition(item: &Table) -> Self {
        let mut diff = TableDiff {
            status: ChangeState::Add,
            changes: ChangeMap::default(),
            name: DiffItem::new_addition(&item.name),
            dimensions: ColumnDiff::new_addition_list(&item.dimensions),
            measures: ColumnDiff::new_addition_list(&item.measures),
        };
        diff.calculate_change_map();
        diff
    }

    fn new_deletion(item: &Table) -> Self {
        let mut diff = TableDiff {
            status: ChangeState::Delete,
            changes: ChangeMap::default(),
            name: DiffItem::new_deletion(&item.name),
            dimensions: ColumnDiff::new_deletion_list(&item.dimensions),
            measures: ColumnDiff::new_deletion_list(&item.measures),
        };
        diff.calculate_change_map();
        diff
    }

    fn new_diff(before: &Table, after: &Table) -> Self {
        let mut diff = TableDiff {
            status: ChangeState::Change,
            changes: ChangeMap::default(),
            name: DiffItem::new_diff(&before.name, &after.name),
            dimensions: ColumnDiff::new_diff_list(&before.dimensions, &after.dimensions),
            measures: ColumnDiff::new_diff_list(&before.measures, &after.measures),
        };
        diff.calculate_change_map();
        if diff.changes.is_empty() {
            diff.status = ChangeState::None
        }
        diff
    }
}


#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ColumnDiff {
    pub status: ChangeState,
    pub changes: ChangeMap,
    pub name: DiffItem<String>,
    pub datatype: DiffItem<String>,
    pub generated: DiffItem<bool>,
    pub formula: DiffItem<Option<String>>,
    pub value: DiffItem<Option<String>>,
    pub drilldown: Vec<ColumnDiff>,
    pub table: DiffItem<Option<String>>,
    pub is_dimension: DiffItem<bool>,
}

impl ColumnDiff {
    fn calculate_change_map(&mut self) {
        let mut c = ChangeMap::default();
        c.update(&self.name);
        c.update(&self.datatype);
        c.update(&self.generated);
        c.update_option(&self.formula);
        c.update_option(&self.value);
        c.update(&self.is_dimension);
        c.update_option(&self.table);
        self.changes.increment_change(c.get_most_changes());
        self.drilldown.iter()
            .for_each(|t| self.changes.merge(&t.changes));
    }
}

impl DiffProducer<Column> for ColumnDiff {
    fn new_addition(item: &Column) -> Self {
        let mut diff = ColumnDiff {
            status: ChangeState::Add,
            changes: ChangeMap::default(),
            name: DiffItem::new_addition(&item.name),
            datatype: DiffItem::new_addition(&item.datatype),
            generated: DiffItem::new_addition(&item.generated),
            formula: DiffItem::new_addition(&item.formula),
            value: DiffItem::new_addition(&item.value),
            drilldown: ColumnDiff::new_addition_list(&item.drilldown),
            table: DiffItem::new_addition(&item.table),
            is_dimension: DiffItem::new_addition(&item.is_dimension),
        };
        diff.calculate_change_map();
        diff
    }

    fn new_deletion(item: &Column) -> Self {
        let mut diff = ColumnDiff {
            status: ChangeState::Delete,
            changes: ChangeMap::default(),
            name: DiffItem::new_deletion(&item.name),
            datatype: DiffItem::new_deletion(&item.datatype),
            generated: DiffItem::new_deletion(&item.generated),
            formula: DiffItem::new_deletion(&item.formula),
            value: DiffItem::new_deletion(&item.value),
            drilldown: ColumnDiff::new_deletion_list(&item.drilldown),
            table: DiffItem::new_deletion(&item.table),
            is_dimension: DiffItem::new_deletion(&item.is_dimension),
        };
        diff.calculate_change_map();
        diff
    }

    fn new_diff(before: &Column, after: &Column) -> Self {
        let mut diff = ColumnDiff {
            status: ChangeState::Change,
            changes: ChangeMap::default(),
            name: DiffItem::new_diff(&before.name, &after.name),
            datatype: DiffItem::new_diff(&before.datatype, &after.datatype),
            generated: DiffItem::new_diff(&before.generated, &after.generated),
            formula: DiffItem::new_diff(&before.formula, &after.formula),
            value: DiffItem::new_diff(&before.value, &after.value),
            drilldown: ColumnDiff::new_diff_list(&before.drilldown, &after.drilldown),
            table: DiffItem::new_diff(&before.table, &after.table),
            is_dimension: DiffItem::new_diff(&before.is_dimension, &after.is_dimension),
        };
        diff.calculate_change_map();
        if diff.changes.is_empty() {
            diff.status = ChangeState::None
        }
        diff
    }
}
