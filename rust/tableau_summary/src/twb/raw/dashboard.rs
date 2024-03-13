use roxmltree::Node;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::twb::NAME_KEY;
use crate::twb::raw::util::parse_formatted_text;
use crate::twb::raw::worksheet::table::View;
use crate::xml::XmlExt;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct RawDashboard {
    name: String,
    title: String,
    thumbnail: Option<String>,
    view: View,
    zones: Zone,
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
    zone_type: String,
    param: Option<String>,
    name: Option<String>,
    text: Option<String>,
    sub_zones: Vec<Zone>,
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
