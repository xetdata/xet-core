use std::collections::HashMap;

use roxmltree::Node;
use serde::{Deserialize, Serialize};

use crate::twb::data_source::{ColumnMeta, parse_column_meta};
use crate::twb::NAME_KEY;
use crate::twb::worksheet::ColumnDep::{Column, ColumnInstance};
use crate::twb::xml::XmlExt;

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
    filters: Vec<String>,
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
    Ok(worksheets_node.find_all_tagged_decendants("worksheet")
        .into_iter()
        .map(parse_worksheet)
        .collect())
}

fn parse_worksheet(node: Node) -> WorksheetMeta {
    let name = node.get_attr(NAME_KEY);
    let title = node.get_tagged_decendant("title")
        .into_iter().flat_map(|ch| ch.find_all_tagged_decendants("run"))
        .map(|n| n.text().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("");
    let view = node.get_tagged_decendant("repository-location")
        .map(|loc_node| {
            loc_node.get_attr("id")
        });
    let rows = node.get_tagged_decendant("rows")
        .and_then(|n| n.text())
        .map(str::to_owned)
        .unwrap_or_default();
    let cols = node.get_tagged_decendant("cols")
        .and_then(|n| n.text())
        .map(str::to_owned)
        .unwrap_or_default();
    let subtotals = node.get_tagged_decendant("subtotals")
        .into_iter().flat_map(|ch| ch.find_all_tagged_decendants("column"))
        .map(|n| n.text().unwrap_or_default())
        .map(str::to_owned)
        .collect();
    let data = node.get_tagged_decendant("view")
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
    let source_to_caption = view_node.find_all_tagged_decendants("datasource")
        .into_iter().map(|n| (n.get_attr(NAME_KEY), n.get_attr("caption")))
        .filter(|(k, v)| !v.is_empty())
        .collect::<HashMap<_, _>>();
    let sources = view_node.find_all_tagged_decendants("datasource-dependencies")
        .iter().map(|n| (n.get_attr("datasource"), *n))
        .map(|(name, node)| {
            (source_to_caption.get(&name).cloned().unwrap_or(name),
             get_column_dep_list(node))
        })
        .collect::<HashMap<_, _>>();
    let filters = view_node.find_all_tagged_decendants("filter")
        .into_iter().map(|n| n.get_attr("column"))
        .collect();
    DataDependencies {
        sources,
        filters,
    }
}

fn get_column_dep_list(node: Node) -> Vec<ColumnDep> {
    let column_iter = node.find_all_tagged_decendants("column")
        .into_iter().map(|n| parse_column_meta(n))
        .map(Column);
    node.find_all_tagged_decendants("column-instance")
        .into_iter().map(|n| parse_column_instance(n))
        .map(ColumnInstance)
        .chain(column_iter)
        .collect()
}

fn parse_column_instance(n: Node) -> ColumnInstanceMeta {
    ColumnInstanceMeta {
        name: n.get_attr(NAME_KEY),
        source_column: n.get_attr("column"),
        derivation: n.get_attr("derivation"),
        col_type: n.get_attr("type"),
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
        let sheet_node = root.find_all_tagged_decendants("worksheet")
            .into_iter()
            .find(|n| n.get_attr("name") == "What If Forecast")
            .unwrap();
        let data = parse_worksheet(sheet_node);
        let s = serde_json::to_string(&data).unwrap();
        println!("{s}");
        assert_eq!("What If Forecast", data.name)
    }
}
