use roxmltree::Node;
use serde::{Deserialize, Serialize};

use crate::twb::NAME_KEY;
use crate::twb::raw::worksheet::{DataDependencies, get_worksheet_data};
use crate::xml::XmlExt;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct DashboardMeta {
    name: String,
    title: String,
    view: Option<String>,
    data: DataDependencies,
    zones: Zone,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Zone {
    zone_type: String,
    param: Option<String>,
    name: Option<String>,
    text: String,
    sub_zones: Vec<Zone>,
}

pub fn parse_dashboards(dashboards_node: Node) -> anyhow::Result<Vec<DashboardMeta>> {
    Ok(dashboards_node.find_all_tagged_decendants("dashboard")
        .into_iter()
        .map(parse_dashboard)
        .collect())
}

fn parse_dashboard(node: Node) -> DashboardMeta {
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
    let data = get_worksheet_data(node);
    let zones = node.get_tagged_decendant("zones")
        .map(build_zone)
        .unwrap_or_default();
    DashboardMeta {
        name,
        title,
        view,
        data,
        zones,
    }
}

fn build_zone(n: Node) -> Zone {
    let zone_type = n.get_attr("type-v2");
    let param = n.get_maybe_attr("param");
    let name = n.get_maybe_attr(NAME_KEY);
    let text = n.get_tagged_decendant("formatted-text")
        .into_iter().flat_map(|ch| ch.find_all_tagged_decendants("run"))
        .map(|n| n.text().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("");
    let sub_zones = n.children()
        .filter(|ch| ch.has_tag_name("zone"))
        .map(build_zone)
        .collect();
    Zone {
        zone_type,
        param,
        name,
        text,
        sub_zones,
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;

    use super::*;

    #[test]
    fn test_get_dashboard() {
        let mut file = File::open("src/Superstore.twb").unwrap();
        let mut s = String::new();
        let _ = file.read_to_string(&mut s).unwrap();
        let doc = roxmltree::Document::parse(&s).unwrap();
        let root = doc.root();
        let dash_node = root.find_all_tagged_decendants("dashboard")
            .into_iter()
            .find(|n| n.get_attr("name") == "Commission Model")
            .unwrap();
        let data = parse_dashboard(dash_node);
        let s = serde_json::to_string(&data).unwrap();
        println!("{s}");
        assert_eq!("Commission Model", data.name)
    }
}
