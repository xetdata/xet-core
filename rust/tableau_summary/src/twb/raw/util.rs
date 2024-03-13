use roxmltree::Node;
use itertools::Itertools;
use crate::xml::XmlExt;

pub fn parse_formatted_text(n: Node) -> String {
    n.find_all_tagged_decendants("run")
        .iter()
        .map(Node::get_text)
        .join("")
}
