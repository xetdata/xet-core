use serde::{Deserialize, Serialize};
use crate::twb::diff::util::{ChangeState, DiffItem, DiffProducer};
use crate::twb::summary::worksheet::{Filter, Item, Mark, Table, Worksheet};

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct WorksheetDiff {
    pub status: ChangeState,
    pub num_changes: usize,
    pub name: DiffItem<String>,
    pub title: DiffItem<String>,
    pub thumbnail: DiffItem<Option<String>>,
    pub table: TableDiff,
}

impl WorksheetDiff {
    fn update_num_changes(&mut self) {
        if self.name.has_diff() {
            self.num_changes += 1;
        }
        if self.title.has_diff() {
            self.num_changes += 1;
        }
        if self.thumbnail.has_option_diff() {
            self.num_changes += 1;
        }
        self.num_changes += self.table.num_changes;
    }
}

impl DiffProducer<Worksheet> for WorksheetDiff {

    fn new_addition(summary: &Worksheet) -> Self {
        let mut diff = Self {
            status: ChangeState::Add,
            num_changes: 0,
            name: DiffItem::added(&summary.name),
            title: DiffItem::added(&summary.title),
            thumbnail: DiffItem::added(&summary.thumbnail),
            table: TableDiff::new_addition(&summary.table),
        };
        diff.update_num_changes();
        diff
    }

    fn new_deletion(summary: &Worksheet) -> Self {
        let mut diff = Self {
            status: ChangeState::Delete,
            num_changes: 0,
            name: DiffItem::deleted(&summary.name),
            title: DiffItem::deleted(&summary.title),
            thumbnail: DiffItem::deleted(&summary.thumbnail),
            table: TableDiff::new_deletion(&summary.table),
        };
        diff.update_num_changes();
        diff
    }

    fn new_diff(before: &Worksheet, after: &Worksheet) -> Self {
        let mut diff = Self {
            status: ChangeState::Change,
            num_changes: 0,
            name: DiffItem::compared(&before.name, &after.name),
            title: DiffItem::compared(&before.title, &after.title),
            thumbnail: DiffItem::compared(&before.thumbnail, &after.thumbnail),
            table: TableDiff::new_diff(&before.table, &after.table),
        };
        diff.update_num_changes();
        if diff.num_changes == 0 {
            diff.status = ChangeState::None
        }
        diff
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TableDiff {
    pub status: ChangeState,
    pub num_changes: usize,
    pub rows: Vec<DiffItem<Item>>,
    pub cols: Vec<DiffItem<Item>>,
    pub filters: Vec<DiffItem<Filter>>,
    pub mark_class: DiffItem<String>,
    pub marks: Vec<DiffItem<Mark>>,
    pub measure_values: Vec<DiffItem<String>>,
    pub tooltip: DiffItem<String>,
}

impl TableDiff {
    fn update_num_changes(&mut self) {
        self.num_changes += DiffItem::num_list_diffs(&self.rows);
        self.num_changes += DiffItem::num_list_diffs(&self.cols);
        self.num_changes += DiffItem::num_list_diffs(&self.filters);
        if self.mark_class.has_diff() {
            self.num_changes += 1;
        }
        self.num_changes += DiffItem::num_list_diffs(&self.marks);
        self.num_changes += DiffItem::num_list_diffs(&self.measure_values);
        if self.tooltip.has_diff() {
            self.num_changes += 1;
        }
    }
}

impl DiffProducer<Table> for TableDiff {

    fn new_addition(summary: &Table) -> Self {
        let mut diff = Self {
            status: ChangeState::Add,
            num_changes: 0,
            rows: DiffItem::added_list(&summary.rows),
            cols: DiffItem::added_list(&summary.cols),
            filters: DiffItem::added_list(&summary.filters),
            mark_class: DiffItem::added(&summary.mark_class),
            marks: DiffItem::added_list(&summary.marks),
            measure_values: DiffItem::added_list(&summary.measure_values),
            tooltip: DiffItem::added(&summary.tooltip),
        };
        diff.update_num_changes();
        diff
    }

    fn new_deletion(summary: &Table) -> Self {
        let mut diff = Self {
            status: ChangeState::Delete,
            num_changes: 0,
            rows: DiffItem::deleted_list(&summary.rows),
            cols: DiffItem::deleted_list(&summary.cols),
            filters: DiffItem::deleted_list(&summary.filters),
            mark_class: DiffItem::deleted(&summary.mark_class),
            marks: DiffItem::deleted_list(&summary.marks),
            measure_values: DiffItem::deleted_list(&summary.measure_values),
            tooltip: DiffItem::deleted(&summary.tooltip),
        };
        diff.update_num_changes();
        diff
    }

    fn new_diff(before: &Table, after: &Table) -> Self {
        let mut diff = Self {
            status: ChangeState::Change,
            num_changes: 0,
            rows: DiffItem::compare_lists(&before.rows, &after.rows),
            cols: DiffItem::compare_lists(&before.cols, &after.cols),
            filters: DiffItem::compare_lists(&before.filters, &after.filters),
            mark_class: DiffItem::compared(&before.mark_class, &after.mark_class),
            marks: DiffItem::compare_lists(&before.marks, &after.marks),
            measure_values: DiffItem::compare_lists(&before.measure_values, &after.measure_values),
            tooltip: DiffItem::compared(&before.tooltip, &after.tooltip),
        };
        diff.update_num_changes();
        if diff.num_changes == 0 {
            diff.status = ChangeState::None;
        }
        diff
    }
}


