use roxmltree::Node;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use tracing::info;
use crate::twb::{CAPTION_KEY, NAME_KEY};
use crate::twb::datasource::columns::ColumnDep::{Column, ColumnInstance, Group, Table};
use crate::twb::datasource::connection::parse_identifiers;
use crate::twb::xml::XmlExt;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ColumnSet {
    pub columns: HashMap<String, ColumnDep>,
    pub drill_paths: Vec<DrillPath>,
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
    pub name: String,
    pub caption: String,
    // maybe enum of types?
    pub datatype: String,
    // maybe enum of [dimension/measure]
    pub role: String,
    pub formula: Option<String>,
    pub value: Option<String>,
    pub hidden: bool,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ColumnInstanceMeta {
    pub name: String,
    pub source_column: String,
    pub col_type: String,
    pub derivation: String,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct GroupMeta {
    pub name: String,
    pub caption: String,
    pub hidden: bool,
    pub filter: Option<GroupFilter>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct GroupFilter {
    pub function: String,
    pub level: String,
    pub member: Option<String>,
    pub sub_filters: Vec<GroupFilter>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TableType {
    pub name: String,
    pub caption: String,
}

pub fn get_column_dep_map(node: Node) -> HashMap<String, ColumnDep> {
    node.children()
        .map(ColumnDep::try_from)
        .filter_map(Result::ok)
        .map(ColumnDep::into_name_kv)
        .collect()
}

impl ColumnDep {
    pub fn get_column(&self) -> Option<&ColumnMeta> {
        if let Column(m) = self {
            Some(m)
        } else {
            None
        }
    }

    fn into_name_kv(self) -> (String, Self) {
        let name = match &self {
            Column(x) => &x.name,
            ColumnInstance(x) => &x.name,
            Group(x) => &x.name,
            Table(x) => &x.name,
        };
        (name.to_owned(), self)
    }
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
    if identifiers.is_empty() {
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
    pub name: String,
    pub fields: Vec<String>,
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




