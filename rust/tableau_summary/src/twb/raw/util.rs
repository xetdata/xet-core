use itertools::Itertools;
use roxmltree::Node;

use crate::xml::XmlExt;

pub fn parse_formatted_text(n: Node) -> String {
    n.find_all_tagged_descendants("run")
        .iter()
        .map(Node::get_text)
        .join("")
}

pub mod macros {
    /// Macro to help wih validating a node's tag is correct for use in From<Node> implmementations.
    /// If the tag is incorrect, an `info!` log is written and Self::default is returned.
    #[macro_export]
    macro_rules! check_tag_or_default {
        ($n:expr, $s:expr) => {
            if $n.get_tag() != $s {
                info!("trying to convert a ({}) to {}", $n.get_tag(), $s);
                return Self::default();
            }
        }
    }
}


