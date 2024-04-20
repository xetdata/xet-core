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
            name: DiffItem::new_addition(&summary.name),
            title: DiffItem::new_addition(&summary.title),
            thumbnail: DiffItem::new_addition(&summary.thumbnail),
            table: TableDiff::new_addition(&summary.table),
        };
        diff.update_num_changes();
        diff
    }

    fn new_deletion(summary: &Worksheet) -> Self {
        let mut diff = Self {
            status: ChangeState::Delete,
            num_changes: 0,
            name: DiffItem::new_deletion(&summary.name),
            title: DiffItem::new_deletion(&summary.title),
            thumbnail: DiffItem::new_deletion(&summary.thumbnail),
            table: TableDiff::new_deletion(&summary.table),
        };
        diff.update_num_changes();
        diff
    }

    fn new_diff(before: &Worksheet, after: &Worksheet) -> Self {
        let mut diff = Self {
            status: ChangeState::Change,
            num_changes: 0,
            name: DiffItem::new_diff(&before.name, &after.name),
            title: DiffItem::new_diff(&before.title, &after.title),
            thumbnail: DiffItem::new_diff(&before.thumbnail, &after.thumbnail),
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
            rows: DiffItem::new_addition_list(&summary.rows),
            cols: DiffItem::new_addition_list(&summary.cols),
            filters: DiffItem::new_addition_list(&summary.filters),
            mark_class: DiffItem::new_addition(&summary.mark_class),
            marks: DiffItem::new_addition_list(&summary.marks),
            measure_values: DiffItem::new_addition_list(&summary.measure_values),
            tooltip: DiffItem::new_addition(&summary.tooltip),
        };
        diff.update_num_changes();
        diff
    }

    fn new_deletion(summary: &Table) -> Self {
        let mut diff = Self {
            status: ChangeState::Delete,
            num_changes: 0,
            rows: DiffItem::new_deletion_list(&summary.rows),
            cols: DiffItem::new_deletion_list(&summary.cols),
            filters: DiffItem::new_deletion_list(&summary.filters),
            mark_class: DiffItem::new_deletion(&summary.mark_class),
            marks: DiffItem::new_deletion_list(&summary.marks),
            measure_values: DiffItem::new_deletion_list(&summary.measure_values),
            tooltip: DiffItem::new_deletion(&summary.tooltip),
        };
        diff.update_num_changes();
        diff
    }

    fn new_diff(before: &Table, after: &Table) -> Self {
        let mut diff = Self {
            status: ChangeState::Change,
            num_changes: 0,
            rows: DiffItem::new_diff_list(&before.rows, &after.rows),
            cols: DiffItem::new_diff_list(&before.cols, &after.cols),
            filters: DiffItem::new_diff_list(&before.filters, &after.filters),
            mark_class: DiffItem::new_diff(&before.mark_class, &after.mark_class),
            marks: DiffItem::new_diff_list(&before.marks, &after.marks),
            measure_values: DiffItem::new_diff_list(&before.measure_values, &after.measure_values),
            tooltip: DiffItem::new_diff(&before.tooltip, &after.tooltip),
        };
        diff.update_num_changes();
        if diff.num_changes == 0 {
            diff.status = ChangeState::None;
        }
        diff
    }
}


