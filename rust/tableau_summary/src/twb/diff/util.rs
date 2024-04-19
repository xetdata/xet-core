use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use serde::{Deserialize, Serialize};
use crate::twb::summary::worksheet::Table;

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct DiffItem<T>
    where
        T: Serialize + Default + PartialEq + Clone + Debug
{
    pub before: Option<T>,
    pub after: Option<T>,
    pub status: ChangeState,
}

impl<T> DiffItem<T>
    where
        T: Serialize + Default + PartialEq + Clone + Debug
{
    pub fn added(item: &T) -> Self {
        Self {
            before: None,
            after: Some(item.clone()),
            status: ChangeState::Add,
        }
    }
    pub fn changed(before: &T, after: &T) -> Self {
        Self {
            before: Some(before.clone()),
            after: Some(after.clone()),
            status: ChangeState::Change,
        }
    }
    pub fn deleted(item: &T) -> Self {
        Self {
            before: Some(item.clone()),
            after: None,
            status: ChangeState::Delete,
        }
    }
    pub fn no_change(item: &T) -> Self {
        Self {
            before: Some(item.clone()),
            after: None,
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

    pub fn compare_lists(before: &[T], after: &[T]) -> Vec<Self> {
        diff_list(before, after)
    }

    pub fn has_diff(&self) -> bool {
        !self.is_unchanged()
    }

    pub fn is_unchanged(&self) -> bool {
        self.status == ChangeState::None
    }

    pub fn num_list_diffs(v: &[Self]) -> usize {
        v.iter()
            .filter(|i|i.has_diff())
            .count()
    }

    pub fn is_list_unchanged(v: &[Self]) -> bool {
        Self::num_list_diffs(v) == 0
    }
}

impl<T> DiffItem<Option<T>>
    where
        T: Serialize + Default + PartialEq + Clone + Debug
{
    pub fn has_option_diff(&self) -> bool {
        if self.is_unchanged() {
            return false;
        }
        self.before.as_ref().is_some_and(Option::is_some)
            || self.after.as_ref().is_some_and(Option::is_some)
    }
}

impl<T> DiffProducer<T> for DiffItem<T>
where
    T: Serialize + Default + PartialEq + Clone + Debug
{
    fn new_addition(summary: &T) -> Self {
        DiffItem::added(summary)
    }

    fn new_deletion(summary: &T) -> Self {
        DiffItem::deleted(summary)
    }

    fn new_diff(before: &T, after: &T) -> Self {
        DiffItem::compared(before, after)
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

/// simple diff of a list that might have duplicates and where order matters.
pub fn diff_list<T, D>(before: &[T], after: &[T]) -> Vec<D>
    where
        D: DiffProducer<T>
{
    let mut i = 0;
    let mut vals = Vec::new();
    while i < before.len() && i < after.len() {
        vals.push(D::new_diff(&before[i], &after[i]));
        i += 1;
    }
    while i < before.len() { // i >= after.len()
        vals.push(D::new_deletion(&before[i]));
        i += 1;
    }
    while i < after.len() { // i >= before.len()
        vals.push(D::new_addition(&after[i]));
        i += 1;
    }
    vals
}


/// diff lists that are expected to be unique and where order doesn't matter.
pub fn diff_unique_list<T, D, F, H>(before: &[T], after: &[T], hash_fn: F) -> Vec<D>
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
        assert!(diff_item.before.is_none());
        assert_eq!(s, diff_item.after.unwrap());
        assert_eq!(ChangeState::Add, diff_item.status);
    }

    #[test]
    fn test_deleted() {
        let s = "foo".to_string();
        let diff_item = DiffItem::deleted(&s);
        assert_eq!(s, diff_item.before.unwrap());
        assert!(diff_item.after.is_none());
        assert_eq!(ChangeState::Delete, diff_item.status);
    }

    #[test]
    fn test_changed() {
        let s = "foo".to_string();
        let s2 = "bar".to_string();
        let diff_item = DiffItem::changed(&s, &s2);
        assert_eq!(s, diff_item.before.unwrap());
        assert_eq!(s2, diff_item.after.unwrap());
        assert_eq!(ChangeState::Change, diff_item.status);
    }

    #[test]
    fn test_none() {
        let s = "foo".to_string();
        let diff_item = DiffItem::no_change(&s);
        assert_eq!(s, diff_item.before.unwrap());
        assert!(diff_item.after.is_none());
        assert_eq!(ChangeState::None, diff_item.status);
    }

    #[test]
    fn test_compare() {
        let s = "foo".to_string();
        let s2 = "bar".to_string();
        let diff_item = DiffItem::compared(&s, &s2);
        assert_eq!(s, diff_item.before.unwrap());
        assert_eq!(s2, diff_item.after.unwrap());
        assert_eq!(ChangeState::Change, diff_item.status);

        let diff_item = DiffItem::compared(&s, &s);
        assert_eq!(s, diff_item.before.unwrap());
        assert!(diff_item.after.is_none());
        assert_eq!(ChangeState::None, diff_item.status);
    }

    #[test]
    fn test_compare_lists() {
        let list1 = ["foo", "bar"].into_iter().map(str::to_string).collect_vec();
        let list2 = ["foo2", "bar"].into_iter().map(str::to_string).collect_vec();

        let diff_items = DiffItem::compare_lists(&list1, &list2, String::clone);
        assert_eq!(3, diff_items.len());
        let states = diff_items.iter().map(|i| i.status.clone()).collect_vec();
        assert_eq!(vec![ChangeState::Delete, ChangeState::None, ChangeState::Add], states);
        let bases = diff_items.iter().map(|i| i.before.clone()).collect_vec();
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

        let diff_items = DiffItem::compare_lists(&list1, &list2, |p|p.name.clone());
        assert_eq!(4, diff_items.len());
        let states = diff_items.iter().map(|i| i.status.clone()).collect_vec();
        assert_eq!(vec![ChangeState::Change, ChangeState::None, ChangeState::Delete, ChangeState::Add], states);
        let bases = diff_items.iter().map(|i| i.before.clone()).collect_vec();
        assert_eq!(vec![("foo", 3), ("bar", 7), ("f2", 12), ("x2", 12)].into_iter().map(Pair::from).collect_vec(), bases);
        let changed = diff_items.iter().map(|i| i.after.clone()).collect_vec();
        assert_eq!(vec![Some(("foo", 30)), None, None, None].into_iter().map(|x|x.map(Pair::from)).collect_vec(), changed);
    }

    #[test]
    fn test_compare_lists_duplicates() {
        let list1 = ["foo", "bar", "foo"].into_iter().map(str::to_string).collect_vec();
        let list2 = ["foo", "bar", "foo"].into_iter().map(str::to_string).collect_vec();

        let diff_items = DiffItem::compare_lists(&list1, &list2, String::clone);
        assert_eq!(3, diff_items.len());
        let states = diff_items.iter().map(|i| i.status.clone()).collect_vec();
        assert_eq!(vec![ChangeState::None, ChangeState::None, ChangeState::None], states);
        let bases = diff_items.iter().map(|i| i.before.clone()).collect_vec();
        assert_eq!(vec!["foo", "bar", "foo"].into_iter().map(str::to_string).collect_vec(), bases);
    }

    #[test]
    fn t() {
        let l1 = vec!["foo", "bar", "baz"];
        let l2 = vec!["bar", "foo", "baz"];
        let diff = vec![
            DiffItem::deleted(&"foo".to_string()),
            DiffItem::no_change(&"bar".to_string()),
            DiffItem::added(&"foo".to_string()),
            DiffItem::no_change(&"baz".to_string()),
        ];
    }

}
