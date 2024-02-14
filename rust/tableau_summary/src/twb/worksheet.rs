use serde::{Deserialize, Serialize};
use roxmltree::Node;
use std::collections::HashMap;
use crate::twb::data_source::{ColumnMeta, parse_column_meta};
use crate::twb::{NAME_KEY, xml};
use crate::twb::worksheet::ColumnDep::{Column, ColumnInstance};
use crate::twb::xml::{find_single_tagged_node, get_attr, get_nodes_with_tags};

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct WorksheetMeta {
    name: String,
    title: String,
    view: Option<String>,
    data: DataDependencies,
    rows: String,
    cols: String,
    subtotals: Vec<String>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct DataDependencies {
    sources: HashMap<String, Vec<ColumnDep>>,
    filters: Vec<String>
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum ColumnDep {
    Column(ColumnMeta),
    ColumnInstance(ColumnInstanceMeta),
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ColumnInstanceMeta {
    name: String,
    source_column: String,
    col_type: String,
    derivation: String,
}


pub(crate) fn parse_worksheets(worksheets_node: Node) -> anyhow::Result<Vec<WorksheetMeta>> {
    Ok(xml::get_nodes_with_tags(worksheets_node, "worksheet")
        .into_iter()
        .map(parse_worksheet)
        .collect())
}

fn parse_worksheet(node: Node) -> WorksheetMeta {
    let name = get_attr(node, NAME_KEY);
    let title = find_single_tagged_node(node, "title")
        .into_iter().flat_map(|ch| get_nodes_with_tags(ch, "run"))
        .map(|n| n.text().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("");
    let view = find_single_tagged_node(node, "repository-location")
        .map(|loc_node| {
            get_attr(loc_node, "id")
        });
    let rows = find_single_tagged_node(node, "rows")
        .and_then(|n| n.text())
        .map(str::to_owned)
        .unwrap_or_default();
    let cols = find_single_tagged_node(node, "cols")
        .and_then(|n| n.text())
        .map(str::to_owned)
        .unwrap_or_default();
    let subtotals = find_single_tagged_node(node, "subtotals")
        .into_iter().flat_map(|ch| get_nodes_with_tags(ch, "column"))
        .map(|n| n.text().unwrap_or_default())
        .map(str::to_owned)
        .collect();
    let data = find_single_tagged_node(node, "view")
        .map(get_worksheet_data)
        .unwrap_or_default();

    WorksheetMeta {
        name,
        title,
        view,
        data,
        rows,
        cols,
        subtotals,
    }
}

pub fn get_worksheet_data(view_node: Node) -> DataDependencies {
    let source_to_caption = get_nodes_with_tags(view_node, "datasource")
        .into_iter().map(|n| (get_attr(n, NAME_KEY), get_attr(n, "caption")))
        .filter(|(k, v)| !v.is_empty())
        .collect::<HashMap<_, _>>();
    let sources = get_nodes_with_tags(view_node, "datasource-dependencies")
        .iter().map(|n| (get_attr(*n, "datasource"), *n))
        .map(|(name, node)| {
            (source_to_caption.get(&name).cloned().unwrap_or(name),
             get_column_dep_list(node))
        })
        .collect::<HashMap<_, _>>();
    let filters = get_nodes_with_tags(view_node, "filter")
        .into_iter().map(|n| get_attr(n, "column"))
        .collect();
    DataDependencies {
        sources,
        filters,
    }
}

fn get_column_dep_list(node: Node) -> Vec<ColumnDep> {
    let column_iter = get_nodes_with_tags(node, "column")
        .into_iter().map(|n| parse_column_meta(n))
        .map(Column);
    get_nodes_with_tags(node, "column-instance")
        .into_iter().map(|n| parse_column_instance(n))
        .map(ColumnInstance)
        .chain(column_iter)
        .collect()
}

fn parse_column_instance(n: Node) -> ColumnInstanceMeta {
    ColumnInstanceMeta {
        name: get_attr(n, NAME_KEY),
        source_column: get_attr(n, "column"),
        derivation: get_attr(n, "derivation"),
        col_type: get_attr(n, "type"),
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;
    use super::*;

    #[test]
    fn test_parse_worksheet() {
        let mut file = File::open("src/Superstore.twb").unwrap();
        let mut s = String::new();
        let _ = file.read_to_string(&mut s).unwrap();
        let doc = roxmltree::Document::parse(&s).unwrap();
        let root = doc.root();
        let sheet_node = get_nodes_with_tags(root, "worksheet")
            .into_iter()
            .find(|n| get_attr(*n, "name") == "What If Forecast")
            .unwrap();
        let data = parse_worksheet(sheet_node);
        let s = serde_json::to_string(&data).unwrap();
        println!("{s}");
        assert_eq!("What If Forecast", data.name)
    }
}
