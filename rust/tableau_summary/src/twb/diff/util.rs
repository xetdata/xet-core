use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct DiffItem<T>
    where
        T: Serialize + Default + PartialEq + Clone + Debug
{
    pub base: T, //TODO: change to before/after
    pub changed: Option<T>,
    pub status: ChangeState,
}

impl<T> DiffItem<T>
    where
        T: Serialize + Default + PartialEq + Clone + Debug
{
    pub fn added(base: &T) -> Self {
        Self {
            base: base.clone(),
            changed: None,
            status: ChangeState::Add,
        }
    }
    pub fn changed(base: &T, changed: &T) -> Self {
        Self {
            base: base.clone(),
            changed: Some(changed.clone()),
            status: ChangeState::Change,
        }
    }
    pub fn deleted(base: &T) -> Self {
        Self {
            base: base.clone(),
            changed: None,
            status: ChangeState::Delete,
        }
    }
    pub fn no_change(base: &T) -> Self {
        Self {
            base: base.clone(),
            changed: None,
            status: ChangeState::None,
        }
    }

    pub fn compared(base: &T, changed: &T) -> Self {
        if base == changed {
            Self::no_change(base)
        } else {
            Self::changed(base, changed)
        }
    }

    pub fn added_list(items: &[T]) -> Vec<Self> {
        items.iter().map(Self::added).collect()
    }

    pub fn deleted_list(items: &[T]) -> Vec<Self> {
        items.iter().map(Self::deleted).collect()
    }

    // TODO: ordering might matter and maybe duplicates in lists
    pub fn compare_unique_lists<H, F>(before: &[T], after: &[T], hash_fn: F) -> Vec<Self>
    where
        H: Hash + Eq,
        F: Fn(&T) -> H
    {
        let mut map = after.iter()
            .map(|t| (hash_fn(t), t))
            .collect::<HashMap<_, _>>();
        let mut vals = Vec::new();
        for item in before {
            let h = hash_fn(item);
            let some_val = map.remove(&h);
            match some_val {
                Some(after_item) => {
                    vals.push(DiffItem::compared(item, after_item))
                },
                None => {
                    vals.push(DiffItem::deleted(item))
                }
            }
        }
        map.values()
            .map(|v| DiffItem::added(*v))
            .for_each(|i| vals.push(i));
        vals
    }

    pub fn is_unchanged(&self) -> bool {
        self.status == ChangeState::None
    }

    pub fn is_list_unchanged(v: &[Self]) -> bool {
        v.iter()
            .all(Self::is_unchanged)
    }
}


#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub enum ChangeState {
    Add,
    Change,
    Delete,
    #[default]
    None,
}

pub trait DiffProducer<T> {

    fn new_addition(summary: &T) -> Self;

    fn new_deletion(summary: &T) -> Self;

    fn new_diff(before: &T, after: &T) -> Self;
}

pub fn diff_list<T, D, F, H>(before: &[T], after: &[T], hash_fn: F) -> Vec<D>
where
    D: DiffProducer<T>,
    H: Hash + Eq,
    F: Fn(&T) -> H,
{
    let mut map = after.iter()
        .map(|t| (hash_fn(t), t))
        .collect::<HashMap<_, _>>();
    let mut vals = Vec::new();
    for item in before {
        let h = hash_fn(item);
        let some_val = map.remove(&h);
        match some_val {
            Some(after_item) => {
                vals.push(D::new_diff(item, after_item))
            },
            None => {
                vals.push(D::new_deletion(item))
            }
        }
    }
    map.values()
        .map(|v| D::new_addition(*v))
        .for_each(|i| vals.push(i));
    vals
}



#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use super::*;

    #[test]
    fn test_added() {
        let s = "foo".to_string();
        let diff_item = DiffItem::added(&s);
        assert_eq!(s, diff_item.base);
        assert!(diff_item.changed.is_none());
        assert_eq!(ChangeState::Add, diff_item.status);
    }

    #[test]
    fn test_deleted() {
        let s = "foo".to_string();
        let diff_item = DiffItem::deleted(&s);
        assert_eq!(s, diff_item.base);
        assert!(diff_item.changed.is_none());
        assert_eq!(ChangeState::Delete, diff_item.status);
    }

    #[test]
    fn test_changed() {
        let s = "foo".to_string();
        let s2 = "bar".to_string();
        let diff_item = DiffItem::changed(&s, &s2);
        assert_eq!(s, diff_item.base);
        assert_eq!(s2, diff_item.changed.unwrap());
        assert_eq!(ChangeState::Change, diff_item.status);
    }

    #[test]
    fn test_none() {
        let s = "foo".to_string();
        let diff_item = DiffItem::no_change(&s);
        assert_eq!(s, diff_item.base);
        assert!(diff_item.changed.is_none());
        assert_eq!(ChangeState::None, diff_item.status);
    }

    #[test]
    fn test_compare() {
        let s = "foo".to_string();
        let s2 = "bar".to_string();
        let diff_item = DiffItem::compared(&s, &s2);
        assert_eq!(s, diff_item.base);
        assert_eq!(s2, diff_item.changed.unwrap());
        assert_eq!(ChangeState::Change, diff_item.status);

        let diff_item = DiffItem::compared(&s, &s);
        assert_eq!(s, diff_item.base);
        assert!(diff_item.changed.is_none());
        assert_eq!(ChangeState::None, diff_item.status);
    }

    #[test]
    fn test_compare_lists() {
        let list1 = ["foo", "bar"].into_iter().map(str::to_string).collect_vec();
        let list2 = ["foo2", "bar"].into_iter().map(str::to_string).collect_vec();

        let diff_items = DiffItem::compare_unique_lists(&list1, &list2, String::clone);
        assert_eq!(3, diff_items.len());
        let states = diff_items.iter().map(|i| i.status.clone()).collect_vec();
        assert_eq!(vec![ChangeState::Delete, ChangeState::None, ChangeState::Add], states);
        let bases = diff_items.iter().map(|i| i.base.clone()).collect_vec();
        assert_eq!(vec!["foo", "bar", "foo2"].into_iter().map(str::to_string).collect_vec(), bases);
    }

    #[test]
    fn test_compare_lists_structs() {
        #[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
        struct Pair {
            name: String,
            val: u32,
        }

        impl From<(&str, u32)> for Pair {
            fn from((name, val): (&str, u32)) -> Self {
                Self { name: name.to_string(), val }
            }
        }

        let list1 = [("foo", 3), ("bar", 7), ("f2", 12)].into_iter().map(Pair::from).collect_vec();
        let list2 = [("foo", 30), ("bar", 7), ("x2", 12)].into_iter().map(Pair::from).collect_vec();

        let diff_items = DiffItem::compare_unique_lists(&list1, &list2, |p|p.name.clone());
        assert_eq!(4, diff_items.len());
        let states = diff_items.iter().map(|i| i.status.clone()).collect_vec();
        assert_eq!(vec![ChangeState::Change, ChangeState::None, ChangeState::Delete, ChangeState::Add], states);
        let bases = diff_items.iter().map(|i| i.base.clone()).collect_vec();
        assert_eq!(vec![("foo", 3), ("bar", 7), ("f2", 12), ("x2", 12)].into_iter().map(Pair::from).collect_vec(), bases);
        let changed = diff_items.iter().map(|i| i.changed.clone()).collect_vec();
        assert_eq!(vec![Some(("foo", 30)), None, None, None].into_iter().map(|x|x.map(Pair::from)).collect_vec(), changed);
    }

    #[test]
    fn test_compare_lists_duplicates() {
        let list1 = ["foo", "bar", "foo"].into_iter().map(str::to_string).collect_vec();
        let list2 = ["foo", "bar", "foo"].into_iter().map(str::to_string).collect_vec();

        let diff_items = DiffItem::compare_unique_lists(&list1, &list2, String::clone);
        assert_eq!(3, diff_items.len());
        let states = diff_items.iter().map(|i| i.status.clone()).collect_vec();
        assert_eq!(vec![ChangeState::None, ChangeState::None, ChangeState::None], states);
        let bases = diff_items.iter().map(|i| i.base.clone()).collect_vec();
        assert_eq!(vec!["foo", "bar", "foo"].into_iter().map(str::to_string).collect_vec(), bases);
    }

}
