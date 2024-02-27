use std::collections::HashMap;

use roxmltree::Node;
use serde::{Deserialize, Serialize};

use crate::twb::datasource::columns::{ColumnDep, get_column_dep_map, GroupFilter};
use crate::twb::NAME_KEY;
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
    sources: HashMap<String, HashMap<String, ColumnDep>>,
    filters: Vec<Filter>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Filter {
    class: String,
    column: String,
    group_filter: Option<GroupFilter>,
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
    let sources = view_node.find_tagged_children("datasource-dependencies")
        .iter().map(|n| (n.get_attr("datasource"), *n))
        .map(|(name, node)| {
            (name, get_column_dep_map(node))
        }).collect();

    let filters = view_node.find_tagged_children("filter")
        .into_iter().map(Filter::from)
        .collect();
    DataDependencies {
        sources,
        filters,
    }
}

impl<'a, 'b> From<Node<'a, 'b>> for Filter {
    fn from(n: Node) -> Self {
        if n.get_tag() != "filter" {
            return Self::default();
        }
        Self {
            class: n.get_attr("class"),
            column: n.get_attr("column"),
            group_filter: n.get_tagged_child("groupfilter")
                .map(GroupFilter::from),
        }
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
