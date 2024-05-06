use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use roxmltree::Node;
use tracing::info;
use crate::check_tag_or_default;
use crate::twb::raw::datasource::connection::{Expression, Relation};
use crate::xml::XmlExt;

/// Tableau's custom tag for the ObjectGraph section
pub const TABLEAU_OBJECT_GRAPH_TAG: &str = "_.fcp.ObjectModelEncapsulateLegacy.true...object-graph";

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct ObjectGraph {
    pub objects: HashMap<String, TableauObject>,
    pub relationships: Vec<Relationship>,
}

impl<'a, 'b> From<Node<'a, 'b>> for ObjectGraph {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, TABLEAU_OBJECT_GRAPH_TAG);
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
    pub relation: Relation,
}

impl<'a, 'b> From<Node<'a, 'b>> for TableauObject {
    fn from(n: Node) -> Self {
        check_tag_or_default!(n, "object");
        let relation = n.get_tagged_child("properties")
            .and_then(|c| c.get_tagged_child("relation"))
            .map(Relation::from)
            .unwrap_or_default();

        Self {
            id: n.get_attr("id"),
            caption: n.get_attr("caption"),
            relation,
        }
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
        check_tag_or_default!(n, "relationship");
        Self {
            expression: n.get_tagged_child("expression").map(Expression::from).unwrap_or_default(),
            id1: n.get_tagged_child("first-end-point").map(|c|c.get_attr("object-id")).unwrap_or_default(),
            id2: n.get_tagged_child("second-end-point").map(|c|c.get_attr("object-id")).unwrap_or_default(),
        }
    }
}
