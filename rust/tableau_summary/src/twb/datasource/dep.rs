use std::collections::HashMap;
use roxmltree::Node;
use serde::{Deserialize, Serialize};
use tracing::info;
use crate::twb::datasource::columns::{ColumnDep, get_column_dep_map};
use crate::xml::XmlExt;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Dep {
    pub name: String,
    pub caption: String,
    pub columns: HashMap<String, ColumnDep>,
}

impl<'a, 'b> From<Node<'a, 'b>> for Dep {
    fn from(n: Node<'a, 'b>) -> Self {
        if n.get_tag() != "datasource-dependencies" {
            info!("trying to convert a ({}) to datasource dependency", n.get_tag());
            return Self::default();
        }
        Self {
            name: n.get_attr("datasource"),
            caption: String::new(),
            columns: get_column_dep_map(n),
        }
    }
}
