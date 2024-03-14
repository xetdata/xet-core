use serde::{Deserialize, Serialize};
use crate::twb::raw::datasource::columns::{ColumnDep, GroupFilter};
use crate::twb::raw::datasource::substituter::{ColumnFinder, Substituter};
use crate::twb::raw::worksheet::RawWorksheet;
use crate::twb::raw::worksheet::table::{Encoding, MEASURE_NAMES_COL, View};
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
    rows: Vec<String>,
    cols: Vec<String>,
    filters: Vec<Filter>,
    mark_class: String,
    marks: Vec<Mark>,
    measure_values: Vec<String>,
    tooltip: String,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Filter {
    name: String,
    is_discrete: bool,
    range: Option<(String, String)>,
    // TODO: we might want to show more info, like the sort order or categorical filtering.
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Mark {
    class: String,
    name: String,
    is_discrete: bool,
}

/// Takes `s` representing some string with columns in it and converts it to a list
/// of dependent columns' display names.
fn to_dep_list(view: &View, s: &str) -> Vec<String> {
    let substituter = Substituter {
        finder: view,
    };
    substituter.substitute_columns(s).1
        .into_iter()
        .filter_map(|(ds, col)| if ds.is_empty() {
            view.find_column(&col)
        } else {
            view.find_column_for_source(&ds, &col)
        })
        .map(strip_brackets)
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
    let substituter = Substituter {
        finder: view,
    };
    let (display, dep_col) = substituter.substitute_columns(s);
    let name = display
        .map(strip_brackets)
        .unwrap_or(s.to_string());
    let is_discrete = dep_col.into_iter()
        .next()
        .and_then(|(ds, col)| view.get_column(&ds, &col))
        .map(is_dimension)
        .unwrap_or_else(|| s.contains(MEASURE_NAMES_COL));
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
        name,
        is_discrete,
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
        if f.column.contains(MEASURE_NAMES_COL) {
            measure_filter = Some(f);
        }
        let (name, is_discrete) = get_name_discrete(view, &f.column);
        filters.push(Filter {
            name,
            is_discrete,
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

    let sub = Substituter {
        finder: view,
    };
    let (tooltip, _) = sub.substitute_columns(&raw_table.pane.customized_tooltip);
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
