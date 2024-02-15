use std::collections::HashMap;
use roxmltree::Node;
use serde::{Deserialize, Serialize};

use crate::twb::{CAPTION_KEY, NAME_KEY, VERSION_KEY};
use crate::twb::data_source::ColumnDep::{Column, ColumnInstance, Group};
use crate::twb::xml::XmlExt;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct DataSourceMeta {
    name: String,
    version: String,
    caption: String,
    connection: Option<ConnectionMeta>,
    columns: HashMap<String, ColumnDep>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ConnectionMeta {
    filename: Option<String>,
    class: String,
    // TODO: we might want to parse more pieces of this section (e.g. `_.fcp.*` elements)
    //       some might contain column names in the files.
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum ColumnDep {
    Column(ColumnMeta),
    ColumnInstance(ColumnInstanceMeta),
    Group(GroupMeta),
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ColumnMeta {
    name: String,
    caption: String,
    // maybe enum of types?
    data_type: String,
    // maybe enum of [dimension/measure]
    role: String,
    formula: Option<String>,
    value: Option<String>,
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


pub(crate) fn parse_datasources(datasources_node: Node) -> anyhow::Result<Vec<DataSourceMeta>> {
    Ok(datasources_node.find_all_tagged_decendants("datasource")
        .into_iter()
        .map(parse_datasource)
        .collect())
}

fn parse_datasource(node: Node) -> DataSourceMeta {
    DataSourceMeta {
        version: node.get_attr(VERSION_KEY),
        caption: node.get_attr(CAPTION_KEY),
        name: node.get_attr(NAME_KEY),
        connection: get_connection_metadata(node),
        columns: get_column_dep_map(node),
    }
}

fn get_connection_metadata(node: Node) -> Option<ConnectionMeta> {
    node.get_tagged_child("connection")
        .and_then(|n| n.get_tagged_decendant("named_connection"))
        .and_then(|n| n.get_tagged_child("connection"))
        .map(|n| ConnectionMeta {
            filename: n.get_maybe_attr("filename"),
            class: n.get_attr("class"),
        })
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
            _ => {return Err(())}
        })
    }
}

impl<'a, 'b> From<Node<'a, 'b>> for ColumnMeta {
    fn from(n: Node) -> Self {
        if n.get_tag() != "column" {
            return Self::default();
        }
        Self {
            name: n.get_attr(NAME_KEY),
            caption: n.get_attr(CAPTION_KEY),
            data_type: n.get_attr("datatype"),
            role: n.get_attr("role"),
            formula: n.get_tagged_child("calculation")
                .and_then(|calc|calc.get_maybe_attr("formula")),
            value: n.get_maybe_attr("value"),
        }
    }
}

impl<'a, 'b> From<Node<'a, 'b>> for ColumnInstanceMeta {
    fn from(n: Node) -> Self {
        if n.get_tag() != "column-instance" {
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
