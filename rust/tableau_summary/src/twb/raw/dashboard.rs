use roxmltree::Node;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::twb::NAME_KEY;
use crate::twb::raw::util::parse_formatted_text;
use crate::twb::raw::worksheet::table::View;
use crate::xml::XmlExt;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct RawDashboard {
    pub name: String,
    pub title: String,
    // The thumbnail here is an identifier for a View attached to the Dashboard
    // This is stored in the `repository-location` tag and the actual thumbnail
    // PNG can be pulled using the ID in conjunction with a get_workbook call.
    pub thumbnail: Option<String>,
    pub view: View,
    pub zones: Zone,
}

impl<'a, 'b> From<Node<'a, 'b>> for RawDashboard {
    fn from(n: Node) -> Self {
        if n.get_tag() != "dashboard" {
            info!("trying to convert a ({}) to a dashboard", n.get_tag());
            return Self::default();
        }
        let title = n.get_tagged_child("layout-options")
            .and_then(|ch| ch.get_tagged_child("title"))
            .map(parse_formatted_text)
            .unwrap_or_default();

        Self {
            name: n.get_attr(NAME_KEY),
            title,
            thumbnail: n.get_tagged_child("repository-location")
                .map(|ch|ch.get_attr("id")),
            view: View::from(n),
            zones: n.get_tagged_child("zones")
                .and_then(|ch|ch.get_tagged_child("zone"))
                .map(Zone::from)
                .unwrap_or_default(),
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Zone {
    pub zone_type: String,
    pub param: Option<String>,
    pub name: Option<String>,
    pub text: Option<String>,
    pub sub_zones: Vec<Zone>,
}

impl<'a, 'b> From<Node<'a, 'b>> for Zone {
    fn from(n: Node) -> Self {
        if n.get_tag() != "zone" {
            info!("trying to convert a ({}) to a zone", n.get_tag());
            return Self::default();
        }
        Self {
            zone_type: n.get_attr("type-v2"),
            param: n.get_maybe_attr("param"),
            name: n.get_maybe_attr(NAME_KEY),
            text: n.get_tagged_child("formatted-text")
                .map(parse_formatted_text),
            sub_zones: n.children()
                .filter(|ch| ch.has_tag_name("zone"))
                .map(Zone::from)
                .collect(),
        }
    }
}

pub fn parse_dashboards(dashboards_node: Node) -> anyhow::Result<Vec<RawDashboard>> {
    Ok(dashboards_node.find_tagged_children("dashboard")
        .into_iter()
        .map(RawDashboard::from)
        .collect())
}
