use roxmltree::{Children, Node};

pub trait XmlExt: Clone {
    type ChildIter: Iterator<Item=Self>;

    fn get_maybe_attr(&self, attr: &str) -> Option<String>;

    fn get_tag(&self) -> &str;

    fn get_children(&self) -> Self::ChildIter;

    fn get_attr(&self, attr: &str) -> String {
        self.get_maybe_attr(attr).unwrap_or_default()
    }
    
    fn get_text(&self) -> String;

    /// Depth is how many generations we should dive down to:
    /// 0 == check this node, 1 == children, 2 == grand-children, ...
    fn find_tagged_descendants_to_depth(&self, tag_name: &str, depth: i8) -> Vec<Self> {
        if depth < 0 {
            return Vec::default();
        }
        let mut v = (self.get_tag() == tag_name)
            .then(|| vec![self.clone()])
            .unwrap_or_default();
        if depth == 0 {
            return v;
        }

        v.extend(self.get_children()
            .flat_map(|ch| ch.find_tagged_descendants_to_depth(tag_name, depth - 1)));
        v
    }

    /// Gets all descendant nodes with the specified tag
    fn find_all_tagged_descendants(&self, tag_name: &str) -> Vec<Self> {
        self.get_children()
            .flat_map(|ch| ch.find_tagged_descendants_to_depth(tag_name, i8::MAX))
            .collect()
    }

    /// Gets the descendant with the indicated tag. Returns None if no descendants
    /// were found with that tag. Returns the first (depth-first-search) descendant
    /// if there are multiple.
    fn get_tagged_descendant(&self, tag_name: &str) -> Option<Self> {
        self.find_all_tagged_descendants(tag_name)
            .into_iter().next()
    }

    /// Finds all direct children with the specified tag
    fn find_tagged_children(&self, tag_name: &str) -> Vec<Self> {
        self.get_children()
            .flat_map(|ch| ch.find_tagged_descendants_to_depth(tag_name, 0))
            .collect()
    }

    /// Gets the child with the indicated tag. Returns None if no children
    /// were found with that tag. Returns the first child if there are
    /// multiple.
    fn get_tagged_child(&self, tag_name: &str) -> Option<Self> {
        self.find_tagged_children(tag_name)
            .into_iter().next()
    }
}

impl<'a, 'b> XmlExt for Node<'a, 'b> {
    type ChildIter = Children<'a, 'b>;

    fn get_maybe_attr(&self, attr: &str) -> Option<String> {
        self.attribute(attr).map(str::to_owned)
    }

    fn get_tag(&self) -> &str {
        self.tag_name().name()
    }

    fn get_children(&self) -> Self::ChildIter {
        self.children()
    }

    fn get_text(&self) -> String {
        self.text()
            .map(str::to_owned)
            .unwrap_or_default()
    }
}


#[cfg(test)]
mod tests {
    use roxmltree::Document;
    use crate::xml::XmlExt;

    const TEST_XML: &str = r#"
    <a foo="bar">
        <b id='1'/>
        <b id='2'/>
        <c val='7'>
            <b id='4'/>
            <d str='abc' />
        </c>
        <b id='3'/>
        <e/>
        <f id='1'>
            <f id='2'/>
        </f>
    </a>"#;


    #[test]
    fn test_tagged_child() {
        let doc = Document::parse(TEST_XML).unwrap();
        let root = doc.root();
        let a = root.get_tagged_child("a").unwrap();
        a.get_tagged_child("e").unwrap();
        assert!(a.get_tagged_child("d").is_none());
        assert!(a.get_tagged_child("unknown").is_none());

        // if there are multiple children, we should get the first one.
        let b_first = a.get_tagged_child("b").unwrap();
        assert_eq!(1, b_first.get_attr("id").parse::<usize>().unwrap());
    }

    #[test]
    fn test_attr() {
        let doc = Document::parse(TEST_XML).unwrap();
        let root = doc.root();
        let a = root.get_tagged_child("a").unwrap();
        assert_eq!("bar", a.get_attr("foo"));
        assert!(a.get_maybe_attr("other").is_none());
        assert_eq!("", a.get_attr("other"));
    }

    #[test]
    fn test_find_children() {
        let doc = Document::parse(TEST_XML).unwrap();
        let root = doc.root();
        let a = root.get_tagged_child("a").unwrap();
        let b = a.find_tagged_children("b");
        assert_eq!(3, b.len());
        assert!(b.iter()
            .enumerate()
            .all(|(i, ch)|
                ch.get_attr("id").parse::<usize>().unwrap() == i + 1
            ));

        assert!(a.find_tagged_children("d").is_empty());
        let f_nodes = a.find_tagged_children("f");
        assert_eq!(1, f_nodes.len());
        let f_child = f_nodes[0].find_tagged_children("f");
        assert_eq!(1, f_child.len());
        assert_eq!(2, f_child[0].get_attr("id").parse::<i32>().unwrap())

    }

    #[test]
    fn test_find_descendants() {
        let doc = Document::parse(TEST_XML).unwrap();
        let root = doc.root();
        let a = root.get_tagged_child("a").unwrap();
        let b = a.find_all_tagged_descendants("b");
        assert_eq!(4, b.len());
        let ids = b.iter().map(|n| n.get_attr("id").parse::<i32>().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(vec![1,2,4,3], ids);

        // should find grandchild, even with no child match.
        assert_eq!(1, a.find_all_tagged_descendants("d").len());

        // should collect both child and grandchild with same tag
        assert_eq!(2, a.find_all_tagged_descendants("f").len());
    }
}
