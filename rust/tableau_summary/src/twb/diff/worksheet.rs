use serde::{Deserialize, Serialize};
use crate::twb::diff::util::{ChangeMap, ChangeState, DiffItem, DiffProducer};
use crate::twb::summary::worksheet::{Filter, Item, Mark, Table, Worksheet};

#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
pub struct WorksheetDiff {
    pub status: ChangeState,
    pub changes: ChangeMap,
    pub name: DiffItem<String>,
    pub title: DiffItem<String>,
    pub thumbnail: DiffItem<Option<String>>,
    pub table: TableDiff,
}

impl WorksheetDiff {
    fn calculate_change_map(&mut self) {
        self.changes.update(&self.name);
        self.changes.update(&self.title);
        self.changes.update_option(&self.thumbnail);
        self.changes.merge(&self.table.changes);
    }
}

impl DiffProducer<Worksheet> for WorksheetDiff {

    fn new_addition(summary: &Worksheet) -> Self {
        let mut diff = Self {
            status: ChangeState::Add,
            changes: ChangeMap::default(),
            name: DiffItem::new_addition(&summary.name),
            title: DiffItem::new_addition(&summary.title),
            thumbnail: DiffItem::new_addition(&summary.thumbnail),
            table: TableDiff::new_addition(&summary.table),
        };
        diff.calculate_change_map();
        diff
    }

    fn new_deletion(summary: &Worksheet) -> Self {
        let mut diff = Self {
            status: ChangeState::Delete,
            changes: ChangeMap::default(),
            name: DiffItem::new_deletion(&summary.name),
            title: DiffItem::new_deletion(&summary.title),
            thumbnail: DiffItem::new_deletion(&summary.thumbnail),
            table: TableDiff::new_deletion(&summary.table),
        };
        diff.calculate_change_map();
        diff
    }

    fn new_diff(before: &Worksheet, after: &Worksheet) -> Self {
        let mut diff = Self {
            status: ChangeState::Change,
            changes: ChangeMap::default(),
            name: DiffItem::new_diff(&before.name, &after.name),
            title: DiffItem::new_diff(&before.title, &after.title),
            thumbnail: DiffItem::new_diff(&before.thumbnail, &after.thumbnail),
            table: TableDiff::new_diff(&before.table, &after.table),
        };
        diff.calculate_change_map();
        if diff.changes.is_empty() {
            diff.status = ChangeState::None
        }
        diff
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
pub struct TableDiff {
    pub status: ChangeState,
    pub changes: ChangeMap,
    pub rows: Vec<DiffItem<Item>>,
    pub cols: Vec<DiffItem<Item>>,
    pub filters: Vec<DiffItem<Filter>>,
    pub mark_class: DiffItem<String>,
    pub marks: Vec<DiffItem<Mark>>,
    pub measure_values: Vec<DiffItem<String>>,
    pub tooltip: DiffItem<String>,
}

impl TableDiff {
    fn calculate_change_map(&mut self) {
        self.changes.update_list(&self.rows);
        self.changes.update_list(&self.cols);
        self.changes.update_list(&self.filters);
        self.changes.update(&self.mark_class);
        self.changes.update_list(&self.marks);
        self.changes.update_list(&self.measure_values);
        self.changes.update(&self.tooltip);
    }
}

impl DiffProducer<Table> for TableDiff {

    fn new_addition(summary: &Table) -> Self {
        let mut diff = Self {
            status: ChangeState::Add,
            changes: ChangeMap::default(),
            rows: DiffItem::new_addition_list(&summary.rows),
            cols: DiffItem::new_addition_list(&summary.cols),
            filters: DiffItem::new_addition_list(&summary.filters),
            mark_class: DiffItem::new_addition(&summary.mark_class),
            marks: DiffItem::new_addition_list(&summary.marks),
            measure_values: DiffItem::new_addition_list(&summary.measure_values),
            tooltip: DiffItem::new_addition(&summary.tooltip),
        };
        diff.calculate_change_map();
        diff
    }

    fn new_deletion(summary: &Table) -> Self {
        let mut diff = Self {
            status: ChangeState::Delete,
            changes: ChangeMap::default(),
            rows: DiffItem::new_deletion_list(&summary.rows),
            cols: DiffItem::new_deletion_list(&summary.cols),
            filters: DiffItem::new_deletion_list(&summary.filters),
            mark_class: DiffItem::new_deletion(&summary.mark_class),
            marks: DiffItem::new_deletion_list(&summary.marks),
            measure_values: DiffItem::new_deletion_list(&summary.measure_values),
            tooltip: DiffItem::new_deletion(&summary.tooltip),
        };
        diff.calculate_change_map();
        diff
    }

    fn new_diff(before: &Table, after: &Table) -> Self {
        let mut diff = Self {
            status: ChangeState::Change,
            changes: ChangeMap::default(),
            rows: DiffItem::new_diff_list(&before.rows, &after.rows),
            cols: DiffItem::new_diff_list(&before.cols, &after.cols),
            filters: DiffItem::new_diff_list(&before.filters, &after.filters),
            mark_class: DiffItem::new_diff(&before.mark_class, &after.mark_class),
            marks: DiffItem::new_diff_list(&before.marks, &after.marks),
            measure_values: DiffItem::new_diff_list(&before.measure_values, &after.measure_values),
            tooltip: DiffItem::new_diff(&before.tooltip, &after.tooltip),
        };
        diff.calculate_change_map();
        if diff.changes.is_empty() {
            diff.status = ChangeState::None;
        }
        diff
    }
}


