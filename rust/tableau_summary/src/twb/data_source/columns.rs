use roxmltree::Node;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use tracing::info;
use crate::twb::{CAPTION_KEY, NAME_KEY};
use crate::twb::data_source::columns::ColumnDep::{Column, ColumnInstance, Group, Table};
use crate::twb::data_source::connection::parse_identifiers;
use crate::twb::xml::XmlExt;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ColumnSet {
    columns: HashMap<String, ColumnDep>,
    drill_paths: Vec<DrillPath>,
}

pub fn get_column_set(n: Node) -> ColumnSet {
    let drill_paths = n.get_tagged_child("drill-paths")
        .into_iter()
        .flat_map(|c| c.find_tagged_children("drill-path"))
        .map(DrillPath::from)
        .collect();
    ColumnSet {
        columns: get_column_dep_map(n),
        drill_paths,
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum ColumnDep {
    Column(ColumnMeta),
    ColumnInstance(ColumnInstanceMeta),
    Group(GroupMeta),
    Table(TableType)
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ColumnMeta {
    name: String,
    caption: String,
    // maybe enum of types?
    datatype: String,
    // maybe enum of [dimension/measure]
    role: String,
    formula: Option<String>,
    value: Option<String>,
    hidden: bool,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ColumnInstanceMeta {
    name: String,
    source_column: String,
    col_type: String,
    derivation: String,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct GroupMeta {
    name: String,
    caption: String,
    hidden: bool,
    filter: Option<GroupFilter>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct GroupFilter {
    function: String,
    level: String,
    member: Option<String>,
    sub_filters: Vec<GroupFilter>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TableType {
    name: String,
    caption: String,
}

pub fn get_column_dep_map(node: Node) -> HashMap<String, ColumnDep> {
    node.children()
        .map(ColumnDep::try_from)
        .filter_map(Result::ok)
        .map(get_kv)
        .collect()
}

fn get_kv(d: ColumnDep) -> (String, ColumnDep) {
    let name = match &d {
        Column(x) => &x.name,
        ColumnInstance(x) => &x.name,
        Group(x) => &x.name,
        Table(x) => &x.name,
    };
    (name.to_owned(), d)
}

impl<'a, 'b> TryFrom<Node<'a, 'b>> for ColumnDep {
    type Error = ();

    fn try_from(n: Node) -> Result<Self, Self::Error> {
        Ok(match n.get_tag() {
            "column" => Column(n.into()),
            "column-instance" => ColumnInstance(n.into()),
            "group" => Group(n.into()),
            "_.fcp.ObjectModelTableType.true...column" => Table(n.into()),
            _ => {return Err(())}
        })
    }
}

impl<'a, 'b> From<Node<'a, 'b>> for ColumnMeta {
    fn from(n: Node) -> Self {
        if n.get_tag() != "column" {
            info!("trying to convert a ({}) to column", n.get_tag());
            return Self::default();
        }
        let datatype = n.get_attr("datatype");
        let datatype = n.get_maybe_attr("semantic-role")
            .and_then(get_type_from_semantic_role)
            .unwrap_or(datatype);

        Self {
            name: n.get_attr(NAME_KEY),
            caption: n.get_attr(CAPTION_KEY),
            datatype,
            role: n.get_attr("role"),
            formula: n.get_tagged_child("calculation")
                .and_then(|calc|calc.get_maybe_attr("formula")),
            value: n.get_maybe_attr("value"),
            hidden: n.get_attr("hidden").parse::<bool>().unwrap_or_default(),
        }
    }
}

fn get_type_from_semantic_role(role: String) -> Option<String> {
    let identifiers = parse_identifiers(&role);
    if identifiers.len() < 1 {
        return None;
    }
    match identifiers[0].to_lowercase().as_str() {
        "country" | "state" | "city" | "zipcode" => Some("geo".to_string()),
        _ => None
    }
}

impl<'a, 'b> From<Node<'a, 'b>> for ColumnInstanceMeta {
    fn from(n: Node) -> Self {
        if n.get_tag() != "column-instance" {
            info!("trying to convert a ({}) to column-instance", n.get_tag());
            return Self::default();
        }
        Self {
            name: n.get_attr(NAME_KEY),
            source_column: n.get_attr("column"),
            derivation: n.get_attr("derivation"),
            col_type: n.get_attr("type"),
        }
    }
}

impl<'a, 'b> From<Node<'a, 'b>> for GroupMeta {
    fn from(n: Node) -> Self {
        if n.get_tag() != "group" {
            info!("trying to convert a ({}) to group", n.get_tag());
            return Self::default();
        }
        Self {
            name: n.get_attr(NAME_KEY),
            caption: n.get_attr(CAPTION_KEY),
            hidden: n.get_attr("hidden").parse::<bool>().unwrap_or_default(),
            filter: n.get_tagged_child("groupfilter")
                .map(|n|n.into()),
        }
    }
}

impl<'a, 'b> From<Node<'a, 'b>> for GroupFilter {
    fn from(n: Node) -> Self {
        if n.get_tag() != "groupfilter" {
            info!("trying to convert a ({}) to groupfilter", n.get_tag());
            return Self::default();
        }
        Self {
            function: n.get_attr("function"),
            level: n.get_attr("level"),
            member: n.get_maybe_attr("member"),
            sub_filters: n.find_tagged_children("groupfilter")
                .into_iter()
                .map(GroupFilter::from)
                .collect(),
        }
    }
}

impl<'a, 'b> From<Node<'a, 'b>> for TableType {
    fn from(n: Node) -> Self {
        if n.get_tag() != "_.fcp.ObjectModelTableType.true...column" {
            info!("trying to convert a ({}) to _.fcp.ObjectModelTableType.true...column", n.get_tag());
            return Self::default();
        }
        let name = n.get_attr(NAME_KEY);
        let ids = parse_identifiers(&name);
        let name = if ids.len() != 2 {
            name
        } else {
            ids[1].clone()
        };
        Self {
            name,
            caption: n.get_attr(CAPTION_KEY),
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct DrillPath {
    name: String,
    fields: Vec<String>,
}

impl<'a, 'b> From<Node<'a, 'b>> for DrillPath {
    fn from(n: Node) -> Self {
        if n.get_tag() != "drill-path" {
            info!("trying to convert a ({}) to drill-path", n.get_tag());
            return Self::default();
        }
        let fields = n.find_tagged_children("field")
            .into_iter()
            .map(|c|c.get_text())
            .collect();
        Self {
            name: n.get_attr(NAME_KEY),
            fields,
        }
    }
}




