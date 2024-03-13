use roxmltree::Node;
use serde::{Deserialize, Serialize};
use tracing::info;
use table::WorksheetTable;

use crate::twb::NAME_KEY;
use crate::twb::raw::util;
use crate::xml::XmlExt;

pub mod table;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct RawWorksheet {
    pub name: String,
    pub title: String,
    pub thumbnail: Option<String>,
    pub table: WorksheetTable,
}

impl<'a, 'b> From<Node<'a, 'b>> for RawWorksheet {
    fn from(n: Node) -> Self {
        if n.get_tag() != "worksheet" {
            info!("trying to convert a ({}) to worksheet", n.get_tag());
            return Self::default();
        }
        let title = n.get_tagged_child("layout-options")
            .and_then(|ch| ch.get_tagged_child("title"))
            .map(util::parse_formatted_text)
            .unwrap_or_default();

        Self {
            name: n.get_attr(NAME_KEY),
            title,
            thumbnail: n.get_tagged_child("repository-location")
                .map(|ch|ch.get_attr("id")),
            table: n.get_tagged_child("table")
                .map(WorksheetTable::from)
                .unwrap_or_default(),
        }
    }
}

pub fn parse_worksheets(worksheets_node: Node) -> anyhow::Result<Vec<RawWorksheet>> {
    Ok(worksheets_node.find_tagged_children("worksheet")
        .into_iter()
        .map(RawWorksheet::from)
        .collect())
}
