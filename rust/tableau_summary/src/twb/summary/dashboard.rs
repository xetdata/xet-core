use crate::twb::raw;
use crate::twb::raw::dashboard::RawDashboard;
use crate::twb::raw::datasource::substituter;
use crate::twb::raw::worksheet::table::View;
use crate::twb::summary::worksheet::get_name_discrete;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
pub struct DashboardV1 {
    pub name: String,
    pub title: String,
    pub thumbnail: Option<String>,
    pub sheets: Vec<String>,
    pub zones: Zone,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
pub struct Dashboard {
    pub name: String,
    pub title: String,
    pub thumbnail: Option<String>,
    pub sheets: Vec<String>,
    pub zones: Vec<Zone>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
pub struct Zone {
    pub zone_type: String,
    pub name: String,
    pub sub_zones: Vec<Zone>,
    pub is_sheet: bool,
}

impl From<&RawDashboard> for Dashboard {
    fn from(dashboard: &RawDashboard) -> Self {
        let (sheets, zones) = build_zones(dashboard);
        let view = &dashboard.view;
        let (maybe_title, _) = substituter::substitute_columns(view, &dashboard.title);
        Self {
            name: dashboard.name.clone(),
            title: maybe_title.unwrap_or_else(|| dashboard.title.clone()),
            thumbnail: dashboard.thumbnail.clone(),
            sheets,
            zones,
        }
    }
}

impl From<&DashboardV1> for Dashboard {
    fn from(dashboard: &DashboardV1) -> Self {
        Self {
            name: dashboard.name.clone(),
            title: dashboard.title.clone(),
            thumbnail: dashboard.thumbnail.clone(),
            sheets: dashboard.sheets.clone(),
            zones: vec![dashboard.zones.clone()],
        }
    }
}

fn build_zones(dashboard: &RawDashboard) -> (Vec<String>, Vec<Zone>) {
    let mut sheets = vec![];
    let zones = dashboard
        .zones
        .iter()
        .map(|z| build_zone(z, &dashboard.view, &dashboard.title, &mut sheets))
        .collect_vec();
    (sheets, zones)
}

fn build_zone(
    zone: &raw::dashboard::Zone,
    view: &View,
    title: &str,
    sheets: &mut Vec<String>,
) -> Zone {
    let (name, zone_type) = get_name_type(zone, view, title);
    let is_sheet = zone_type == "sheet";
    if is_sheet {
        sheets.push(name.clone());
    }
    let sub_zones = zone
        .sub_zones
        .iter()
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
            ztype = z
                .param
                .as_ref()
                .map(|p| format!("{}-{}", z.zone_type, p))
                .unwrap_or(z.zone_type.clone());
            match &z.param {
                Some(x) if x == "vert" => "Vertical Container".to_string(),
                Some(x) if x == "horz" => "Horizontal Container".to_string(),
                _ => "Container".to_string(),
            }
        }
        "layout-basic" => "Tiled".to_string(),
        "text" => z.text.clone().unwrap_or("text".to_string()),
        "title" => title.to_string(),
        "paramctrl" => {
            if let Some(ref text) = z.text {
                text.to_string()
            } else if let Some(ref param) = z.param {
                let (n, _) = get_name_discrete(view, param);
                n
            } else {
                "Param".to_string()
            }
        }
        "empty" => "Blank".to_string(),
        "color" => "Color Legend".to_string(),
        "filter" => {
            if let Some(ref param) = z.param {
                let (n, _) = get_name_discrete(view, param);
                n
            } else {
                "Filter".to_string()
            }
        }
        "bitmap" => z
            .param
            .as_ref()
            .and_then(|p| p.split('/').last())
            .unwrap_or("Image")
            .to_string(),
        "" => {
            ztype = "sheet".to_string();
            if let Some(ref zname) = z.name {
                zname.to_string()
            } else {
                "Sheet".to_string()
            }
        }
        _ => {
            info!("Unknown zone type: {}", z.zone_type);
            "Unknown".to_string()
        }
    };

    (name, ztype)
}
