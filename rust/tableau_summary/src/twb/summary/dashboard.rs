use serde::{Deserialize, Serialize};
use tracing::info;
use crate::twb::raw::dashboard::RawDashboard;
use crate::twb::raw;
use crate::twb::raw::worksheet::table::View;
use crate::twb::summary::worksheet::get_name_discrete;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Dashboard {
    name: String,
    title: String,
    thumbnail: Option<String>,
    sheets: Vec<String>,
    zones: Zone,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Zone {
    zone_type: String,
    name: String,
    sub_zones: Vec<Zone>,
    is_sheet: bool,
}


impl From<&RawDashboard> for Dashboard {
    fn from(dashboard: &RawDashboard) -> Self {
        let (sheets, zones) = build_zones(dashboard);
        Self {
            name: dashboard.name.clone(),
            title: dashboard.title.clone(),
            thumbnail: dashboard.thumbnail.clone(),
            sheets,
            zones,
        }
    }
}

fn build_zones(dashboard: &RawDashboard) -> (Vec<String>, Zone) {
    let mut sheets = vec![];
    let zone = build_zone(&dashboard.zones, &dashboard.view, &dashboard.title, &mut sheets);
    (sheets, zone)
}

fn build_zone(zone: &raw::dashboard::Zone, view: &View, title: &str, sheets: &mut Vec<String>) -> Zone {
    let (name, zone_type) = get_name_type(zone, view, title);
    let is_sheet = zone_type == "sheet";
    if is_sheet {
        sheets.push(name.clone());
    }
    let sub_zones = zone.sub_zones.iter()
        .map(|z| build_zone(z, view, title, sheets))
        .collect();

    Zone {
        zone_type,
        name,
        sub_zones,
        is_sheet,
    }
}


fn get_name_type(z: &raw::dashboard::Zone, view: &View, title: &str) -> (String, String) {
    let mut ztype = z.zone_type.clone();
    let name = match z.zone_type.as_str() {
        "layout-flow" => {
            ztype = z.param.as_ref().map(|p|format!("{}-{}", z.zone_type, p))
                .unwrap_or(z.zone_type.clone());
            match &z.param {
                Some(x) if x == "vert" => "Vertical Container".to_string(),
                Some(x) if x == "horz" => "Horizontal Container".to_string(),
                _ => "Container".to_string(),
            }
        },
        "layout-basic" => {
            "Tiled".to_string()
        },
        "text" => {
            z.text.clone().unwrap_or("text".to_string())
        },
        "title" => {
            title.to_string()
        },
        "paramctrl" => {
            if let Some(ref text) = z.text {
                text.to_string()
            } else if let Some(ref param) = z.param {
                let (n, _) = get_name_discrete(view, param);
                n
            } else {
                "Param".to_string()
            }
        },
        "empty" => {
            "Blank".to_string()
        },
        "color" => {
            "Color Legend".to_string()
        },
        "filter" => {
            if let Some(ref param) = z.param {
                let (n , _) = get_name_discrete(view, param);
                n
            } else {
                "Filter".to_string()
            }
        }
        "" => {
            ztype = "sheet".to_string();
            if let Some(ref zname) = z.name {
                zname.to_string()
            } else {
                "Sheet".to_string()
            }
        },
        _ => {
            info!("Unknown zone type: {}", z.zone_type);
            "Unknown".to_string()
        },
    };

    (name, ztype)
}

