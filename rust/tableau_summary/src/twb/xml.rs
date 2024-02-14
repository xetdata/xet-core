use roxmltree::Node;

pub(crate) fn get_attr(node: Node, s: &str) -> String {
    get_maybe_attr(node, s).unwrap_or_default()
}

pub(crate) fn get_maybe_attr(node: Node, s: &str) -> Option<String> {
    node.attribute(s).map(str::to_owned)
}

pub(crate) fn get_nodes_with_tags<'a, 'b>(node: Node<'a, 'b>, tag_name: &str) -> Vec<Node<'a, 'b>> {
    let mut v = vec![];
    if node.tag_name().name() == tag_name {
        v.push(node);
    }
    for ch in node.children() {
        v.extend_from_slice(&get_nodes_with_tags(ch, tag_name))
    }
    v
}

pub(crate) fn find_single_tagged_node<'a, 'b>(node: Node<'a, 'b>, tag_name: &str) -> Option<Node<'a, 'b>> {
    let nodes = get_nodes_with_tags(node, tag_name);
    if !nodes.is_empty() {
        Some(nodes[0])
    } else {
        None
    }
}
