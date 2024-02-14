use roxmltree::Node;
use serde::{Deserialize, Serialize};
use crate::twb::worksheet::{DataDependencies, get_worksheet_data};
use crate::twb::{NAME_KEY, xml};
use crate::twb::xml::{find_single_tagged_node, get_attr, get_maybe_attr, get_nodes_with_tags};

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
    sub_zones: Vec<Zone>,

}

fn parse_dashboards(dashboards_node: Node) -> anyhow::Result<Vec<DashboardMeta>> {
    Ok(xml::get_nodes_with_tags(dashboards_node, "dashboard")
        .into_iter()
        .map(parse_dashboard)
        .collect())
}

fn parse_dashboard(node: Node) -> DashboardMeta {
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
    let data = get_worksheet_data(node);
    let zones = find_single_tagged_node(node, "zones")
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
    let zone_type = get_attr(n, "type-v2");
    let param = get_maybe_attr(n, "param");
    let sub_zones = n.children()
        .filter(|ch| ch.has_tag_name("zone"))
        .map(build_zone)
        .collect();
    Zone {
        zone_type,
        param,
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
        let dash_node = get_nodes_with_tags(root, "dashboard")
            .into_iter()
            .find(|n| get_attr(*n, "name") == "Commission Model")
            .unwrap();
        let data = parse_dashboard(dash_node);
        let s = serde_json::to_string(&data).unwrap();
        println!("{s}");
        assert_eq!("Commision Model", data.name)
    }
}
