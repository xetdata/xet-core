use itertools::Itertools;
use roxmltree::Node;
use url::Url;

use crate::xml::XmlExt;

/// Tableau Public urls in the repository location are special
/// and encode the name of the view differently.
const TABLEAU_PUBLIC_HOST: &str = "public.tableau.com";

pub fn parse_formatted_text(n: Node) -> String {
    n.find_all_tagged_descendants("run")
        .iter()
        .map(Node::get_text)
        .join("")
}

pub fn repository_location_to_thumbnail_name(n: Node) -> String {
    let id = n.get_attr("id");
    let derived_from = n.get_attr("derived-from");
    let (view_name, hostname) = Url::parse(&derived_from)
        .map(|u| (
            u.path_segments()
                .and_then(|s| s.last())
                .map(String::from),
            u.host_str()
                .map(str::to_string)
        )
        ).unwrap_or((None, None));
    if let Some(hostname) = hostname {
        // the `view_name` for Tableau Public urls is a number (i.e. the site's id),
        // we need the actual name of the view, which seems to be stored
        // in the `id` attribute instead.
        if hostname.contains(TABLEAU_PUBLIC_HOST) {
            id
        } else {
            view_name.unwrap_or(id)
        }
    } else {
        view_name.unwrap_or(id)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repository_location_to_thumbnail() {
        let s = "<repository-location derived-from='http://localhost:9100/t/xethubintegjoe/workbooks/Superstore?rev=1.2' id='Superstore' path='/t/xethubintegjoe/workbooks' revision='1.5' site='xethubintegjoe' />";
        let document = roxmltree::Document::parse(s).unwrap();
        let root = document.root().get_tagged_descendant("repository-location").unwrap();
        let s = repository_location_to_thumbnail_name(root);
        assert_eq!("Superstore", &s);
    }

    #[test]
    fn test_repository_location_to_thumbnail_different_id() {
        let s = "<repository-location derived-from='http://localhost:9100/t/xethubintegjoe/workbooks/BookSales/Sheet1?rev=' id='1352269' path='/t/xethubintegjoe/workbooks/BookSales' revision='' site='xethubintegjoe' />";
        let document = roxmltree::Document::parse(s).unwrap();
        let root = document.root().get_tagged_descendant("repository-location").unwrap();
        let s = repository_location_to_thumbnail_name(root);
        assert_eq!("Sheet1", &s);
    }
}
