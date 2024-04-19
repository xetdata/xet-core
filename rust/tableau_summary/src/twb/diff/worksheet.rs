use serde::{Deserialize, Serialize};
use crate::twb::diff::util::{ChangeState, DiffItem, DiffProducer};
use crate::twb::summary::worksheet::{Filter, Item, Mark, Table, Worksheet};

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct WorksheetDiff {
    pub status: ChangeState,
    pub name: DiffItem<String>,
    pub title: DiffItem<String>,
    pub thumbnail: DiffItem<Option<String>>,
    pub table: TableDiff,
}

impl DiffProducer<Worksheet> for WorksheetDiff {

    fn new_addition(summary: &Worksheet) -> Self {
        Self {
            status: ChangeState::Add,
            name: DiffItem::added(&summary.name),
            title: DiffItem::added(&summary.title),
            thumbnail: DiffItem::added(&summary.thumbnail),
            table: TableDiff::new_addition(&summary.table),
        }
    }

    fn new_deletion(summary: &Worksheet) -> Self {
        Self {
            status: ChangeState::Delete,
            name: DiffItem::deleted(&summary.name),
            title: DiffItem::deleted(&summary.title),
            thumbnail: DiffItem::deleted(&summary.thumbnail),
            table: TableDiff::new_deletion(&summary.table),
        }
    }

    fn new_diff(before: &Worksheet, after: &Worksheet) -> Self {
        let mut diff = Self {
            status: ChangeState::Change,
            name: DiffItem::compared(&before.name, &after.name),
            title: DiffItem::compared(&before.title, &after.title),
            thumbnail: DiffItem::compared(&before.thumbnail, &after.thumbnail),
            table: TableDiff::new_diff(&before.table, &after.table),
        };
        if diff.name.is_unchanged()
            && diff.title.is_unchanged()
            && diff.thumbnail.is_unchanged()
            && diff.table.status == ChangeState::None {
            diff.status = ChangeState::None
        }
        diff
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TableDiff {
    pub status: ChangeState,
    pub rows: Vec<DiffItem<Item>>,
    pub cols: Vec<DiffItem<Item>>,
    pub filters: Vec<DiffItem<Filter>>,
    pub mark_class: DiffItem<String>,
    pub marks: Vec<DiffItem<Mark>>,
    pub measure_values: Vec<DiffItem<String>>,
    pub tooltip: DiffItem<String>,
}

impl DiffProducer<Table> for TableDiff {

    fn new_addition(summary: &Table) -> Self {
        Self {
            status: ChangeState::Add,
            rows: DiffItem::added_list(&summary.rows),
            cols: DiffItem::added_list(&summary.cols),
            filters: DiffItem::added_list(&summary.filters),
            mark_class: DiffItem::added(&summary.mark_class),
            marks: DiffItem::added_list(&summary.marks),
            measure_values: DiffItem::added_list(&summary.measure_values),
            tooltip: DiffItem::added(&summary.tooltip),
        }
    }

    fn new_deletion(summary: &Table) -> Self {
        Self {
            status: ChangeState::Delete,
            rows: DiffItem::deleted_list(&summary.rows),
            cols: DiffItem::deleted_list(&summary.cols),
            filters: DiffItem::deleted_list(&summary.filters),
            mark_class: DiffItem::deleted(&summary.mark_class),
            marks: DiffItem::deleted_list(&summary.marks),
            measure_values: DiffItem::deleted_list(&summary.measure_values),
            tooltip: DiffItem::deleted(&summary.tooltip),
        }
    }

    fn new_diff(before: &Table, after: &Table) -> Self {
        let mut diff = Self {
            status: ChangeState::Change,
            rows: DiffItem::compare_unique_lists(&before.rows, &after.rows, |r| r.name.clone()),
            cols: DiffItem::compare_unique_lists(&before.cols, &after.cols, |c| c.name.clone()),
            filters: DiffItem::compare_unique_lists(&before.filters, &after.filters, |f| f.item.name.clone()),
            mark_class: DiffItem::compared(&before.mark_class, &after.mark_class),
            marks: DiffItem::compare_unique_lists(&before.marks, &after.marks, |m| m.item.name.clone()),
            measure_values: DiffItem::compare_unique_lists(&before.measure_values, &after.measure_values, String::clone),
            tooltip: DiffItem::compared(&before.tooltip, &after.tooltip),
        };
        if DiffItem::is_list_unchanged(&diff.rows)
            && DiffItem::is_list_unchanged(&diff.cols)
            && DiffItem::is_list_unchanged(&diff.filters)
            && diff.mark_class.is_unchanged()
            && DiffItem::is_list_unchanged(&diff.marks)
            && DiffItem::is_list_unchanged(&diff.measure_values)
            && diff.tooltip.is_unchanged() {
            diff.status = ChangeState::None;
        }
        diff

    }
}


