use std::collections::HashMap;
use roxmltree::Node;
use serde::{Deserialize, Serialize};
use tracing::info;
use crate::check_tag_or_default;
use crate::twb::raw::datasource::columns::{ColumnDep, get_column_dep_map};
use crate::xml::XmlExt;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct Dep {
    pub name: String,
    pub caption: String,
    pub columns: HashMap<String, ColumnDep>,
}

impl<'a, 'b> From<Node<'a, 'b>> for Dep {
    fn from(n: Node<'a, 'b>) -> Self {
        check_tag_or_default!(n, "datasource-dependencies");
        Self {
            name: n.get_attr("datasource"),
            caption: String::new(),
            columns: get_column_dep_map(n),
        }
    }
}
