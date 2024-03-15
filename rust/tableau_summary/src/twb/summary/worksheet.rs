use serde::{Deserialize, Serialize};
use crate::twb::raw::datasource::columns::{ColumnDep, GroupFilter};
use crate::twb::raw::datasource::substituter;
use crate::twb::raw::worksheet::RawWorksheet;
use crate::twb::raw::worksheet::table::{Encoding, MEASURE_NAMES_COL_NAME, View};
use crate::twb::summary::util::{strip_brackets, strip_quotes};

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Worksheet {
    name: String,
    title: String,
    thumbnail: Option<String>,
    table: Table,
}

impl From<&RawWorksheet> for Worksheet {
    fn from(raw_wks: &RawWorksheet) -> Self {
        Self {
            name: raw_wks.name.clone(),
            title: raw_wks.title.clone(),
            thumbnail: raw_wks.thumbnail.clone(),
            table: table_from_worksheet(raw_wks),
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Table {
    rows: Vec<Item>,
    cols: Vec<Item>,
    filters: Vec<Filter>,
    mark_class: String,
    marks: Vec<Mark>,
    measure_values: Vec<String>,
    tooltip: String,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Filter {
    item: Item,
    range: Option<(String, String)>,
    // TODO: we might want to show more info, like the sort order or categorical filtering.
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Mark {
    class: String,
    item: Item,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Item {
    name: String,
    is_discrete: bool,
}

/// Takes `s` representing some string with columns in it and converts it to a list
/// of dependent columns' display names.
fn to_dep_list(view: &View, s: &str) -> Vec<Item> {
    substituter::substitute_columns(view, s).1
        .into_iter()
        .filter_map(|(ds, col)| view.get_column(&ds, &col))
        .map(|dep| (view.get_caption_for_dep(dep), is_dimension(dep)))
        .map(|(caption, is_discrete)| Item {
            name: strip_brackets(caption),
            is_discrete,
        })
        .collect()
}

fn is_dimension(dep: &ColumnDep) -> bool {
    match dep {
        ColumnDep::Column(c) => c.role == "dimension",
        ColumnDep::ColumnInstance(ci) => {
            !matches!(ci.col_type.as_str(), "quantitative")
        },
        _ => false
    }
}

pub fn get_name_discrete(view: &View, s: &str) -> (String, bool) {
    let (display, dep_col) = substituter::substitute_columns(view, s);
    let name = display
        .map(strip_brackets)
        .unwrap_or(s.to_string());
    let is_discrete = dep_col.into_iter()
        .next()
        .and_then(|(ds, col)| view.get_column(&ds, &col))
        .map(is_dimension)
        .unwrap_or_else(|| s.contains(MEASURE_NAMES_COL_NAME));
    (name, is_discrete)
}

fn get_mark(view: &View, enc: &Encoding) -> Option<Mark> {
    let class = enc.enc_type.clone();
    if class == "geometry" {
        return None;
    }
    let (name, is_discrete) = get_name_discrete(view, &enc.column);
    Some(Mark {
        class,
        item: Item {
            name,
            is_discrete,
        },
    })
}

fn extract_measure_columns(g: &GroupFilter) -> Vec<String> {
    if g.function == "union" {
        g.sub_filters.iter()
            .filter_map(|g2| g2.member.clone())
            .map(strip_quotes)
            .collect::<Vec<_>>()
    } else {
        vec![]
    }
}

fn get_filters_and_measure_values(view: &View) -> (Vec<Filter>, Vec<String>) {
    let mut measure_filter = None;
    let mut filters = vec![];
    for f in view.filters.iter() {
        if f.column.contains(MEASURE_NAMES_COL_NAME) {
            measure_filter = Some(f);
        }
        let (name, is_discrete) = get_name_discrete(view, &f.column);
        filters.push(Filter {
            item: Item {
                name,
                is_discrete,
            },
            range: f.range.clone(),
        });
    }
    let measure_values = measure_filter
        .and_then(|f| f.group_filter.as_ref())
        .into_iter()
        .flat_map(extract_measure_columns)
        .map(|col| get_name_discrete(view, &col).0)
        .collect();
    (filters, measure_values)
}

fn table_from_worksheet(worksheet: &RawWorksheet) -> Table {
    let raw_table = &worksheet.table;
    let view = &raw_table.view;

    let marks = raw_table.pane.encodings.iter()
        .filter_map(|encoding| get_mark(view, encoding))
        .collect();
    let (filters, measure_values) = get_filters_and_measure_values(view);

    let (tooltip, _) = substituter::substitute_columns(view, &raw_table.pane.customized_tooltip);
    let tooltip = tooltip.unwrap_or_else(|| raw_table.pane.customized_tooltip.clone());

    Table {
        rows: to_dep_list(view, &raw_table.rows),
        cols: to_dep_list(view, &raw_table.cols),
        filters,
        mark_class: raw_table.pane.mark_class.clone(),
        marks,
        measure_values,
        tooltip,
    }
}
