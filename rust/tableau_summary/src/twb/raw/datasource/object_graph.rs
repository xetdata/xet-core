use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use roxmltree::Node;
use tracing::info;
use crate::twb::raw::datasource::connection::{Expression, Join, Relation, Table, Union};
use crate::xml::XmlExt;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ObjectGraph {
    pub objects: HashMap<String, TableauObject>,
    pub relationships: Vec<Relationship>,
}

impl<'a, 'b> From<Node<'a, 'b>> for ObjectGraph {
    fn from(n: Node) -> Self {
        if n.get_tag() != "_.fcp.ObjectModelEncapsulateLegacy.true...object-graph" {
            info!("trying to convert a ({}) to an object graph", n.get_tag());
            return Self::default();
        }
        let objects = n.get_tagged_child("objects")
            .into_iter()
            .flat_map(|objs| objs.find_tagged_children("object"))
            .map(TableauObject::from)
            .map(|o| (o.id.clone(), o))
            .collect();
        let relationships = n.get_tagged_child("relationships")
            .into_iter()
            .flat_map(|rels| rels.find_tagged_children("relationship"))
            .map(Relationship::from)
            .collect();
        Self {
            objects,
            relationships,
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TableauObject {
    pub id: String,
    pub caption: String,
    pub table: Option<Table>,
    pub join: Option<Join>,
    pub union: Option<Union>,
}

impl<'a, 'b> From<Node<'a, 'b>> for TableauObject {
    fn from(n: Node) -> Self {
        if n.get_tag() != "object" {
            info!("trying to convert a ({}) to an object", n.get_tag());
            return Self::default();
        }
        let relation = n.get_tagged_child("properties")
            .and_then(|c| c.get_tagged_child("relation"))
            .map(Relation::from)
            .unwrap_or_default();

        let mut obj = Self {
            id: n.get_attr("id"),
            caption: n.get_attr("caption"),
            table: None,
            join: None,
            union: None,
        };
        match relation {
            Relation::Unknown => {
                info!("unknown relation for object");
            }
            Relation::Table(t) => {
                obj.table = Some(t);
            }
            Relation::Join(j) => {
                obj.join = Some(j);
            }
            Relation::Union(u) => {
                obj.union = Some(u);
            }
        }
        obj
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Relationship {
    pub expression: Expression,
    pub id1: String,
    pub id2: String,
}

impl<'a, 'b> From<Node<'a, 'b>> for Relationship {
    fn from(n: Node) -> Self {
        if n.get_tag() != "relationship" {
            info!("trying to convert a ({}) to a relationship", n.get_tag());
            return Self::default();
        }
        Self {
            expression: n.get_tagged_child("expression").map(Expression::from).unwrap_or_default(),
            id1: n.get_tagged_child("first-end-point").map(|c|c.get_attr("object-id")).unwrap_or_default(),
            id2: n.get_tagged_child("second-end-point").map(|c|c.get_attr("object-id")).unwrap_or_default(),
        }
    }
}
