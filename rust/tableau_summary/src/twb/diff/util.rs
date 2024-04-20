use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use serde::{Deserialize, Serialize};

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
    fn new_addition(item: &T) -> Self {
        Self {
            before: None,
            after: Some(item.clone()),
            status: ChangeState::Add,
        }
    }

    fn new_deletion(item: &T) -> Self {
        Self {
            before: Some(item.clone()),
            after: None,
            status: ChangeState::Delete,
        }
    }

    fn new_diff(before: &T, after: &T) -> Self {
        if before == after {
            Self {
                before: Some(before.clone()),
                after: None, // No need to duplicate the item.
                status: ChangeState::None,
            }
        } else {
            Self {
                before: Some(before.clone()),
                after: Some(after.clone()),
                status: ChangeState::Change,
            }
        }
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

/// DiffProducer is a factory trait allowing diffs to be created under various conditions
/// from some summary type `T`. For example creating a new diff where an item was added,
/// or deleted, or how to compare two items together. This also extends to creating lists
/// of diffs from lists of `T`s.
///
/// TODO: this could possibly be a good candidate for a derive macro as it seems that
///       implementations just contain instances of [DiffProducer] or [DiffItem].
pub trait DiffProducer<T>: Sized {
    /// Creates a new diff from the added item.
    fn new_addition(item: &T) -> Self;

    /// Creates a new diff from the deleted item.
    fn new_deletion(item: &T) -> Self;

    /// Creates a new diff from a before+after item.
    fn new_diff(before: &T, after: &T) -> Self;

    /// Creates a new Vec of diffs from the provided list of added items.
    fn new_addition_list(items: &[T]) -> Vec<Self> {
        items.iter().map(Self::new_addition).collect()
    }

    /// Creates a new Vec of diffs from the provided list of deleted items.
    fn new_deletion_list(items: &[T]) -> Vec<Self> {
        items.iter().map(Self::new_deletion).collect()
    }

    /// Simple diff of a list that might have duplicates and where order matters.
    /// Currently, just compares indices to see if the content changed.
    /// TODO: possibly change to a minimal diff, looking into what elements moved
    ///       around the list.
    fn new_diff_list(before: &[T], after: &[T]) -> Vec<Self> {
        let mut i = 0;
        let mut vals = Vec::new();
        while i < before.len() && i < after.len() {
            vals.push(Self::new_diff(&before[i], &after[i]));
            i += 1;
        }
        while i < before.len() { // i >= after.len()
            vals.push(Self::new_deletion(&before[i]));
            i += 1;
        }
        while i < after.len() { // i >= before.len()
            vals.push(Self::new_addition(&after[i]));
            i += 1;
        }
        vals
    }

    /// diff lists that are expected to be unique and where order doesn't matter.
    fn new_unique_diff_list<F, H>(before: &[T], after: &[T], hash_fn: F) -> Vec<Self>
        where
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
                    vals.push(Self::new_diff(item, after_item))
                },
                None => {
                    vals.push(Self::new_deletion(item))
                }
            }
        }
        map.values()
            .map(|v| Self::new_addition(*v))
            .for_each(|i| vals.push(i));
        vals
    }

}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use super::*;

    #[test]
    fn test_added() {
        let s = "foo".to_string();
        let diff_item = DiffItem::new_addition(&s);
        assert!(diff_item.before.is_none());
        assert_eq!(s, diff_item.after.unwrap());
        assert_eq!(ChangeState::Add, diff_item.status);
    }

    #[test]
    fn test_deleted() {
        let s = "foo".to_string();
        let diff_item = DiffItem::new_deletion(&s);
        assert_eq!(s, diff_item.before.unwrap());
        assert!(diff_item.after.is_none());
        assert_eq!(ChangeState::Delete, diff_item.status);
    }

    #[test]
    fn test_diff() {
        let s = "foo".to_string();
        let s2 = "bar".to_string();
        let s3 = "foo".to_string();
        let diff_item = DiffItem::new_diff(&s, &s2);
        assert_eq!(s, diff_item.before.unwrap());
        assert_eq!(s2, diff_item.after.unwrap());
        assert_eq!(ChangeState::Change, diff_item.status);

        let diff_item = DiffItem::new_diff(&s, &s3);
        assert_eq!(s, diff_item.before.unwrap());
        assert!(diff_item.after.is_none());
        assert_eq!(ChangeState::None, diff_item.status);
    }

    fn from_vec<T, U: From<T>>(v: Vec<T>) -> Vec<U> {
        v.into_iter().map(U::from).collect_vec()
    }

    impl<T, U> From<(ChangeState, Option<T>, Option<T>)> for DiffItem<U>
        where
            U: Serialize + Default + PartialEq + Clone + Debug + From<T>,
    {
        fn from((status, before, after): (ChangeState, Option<T>, Option<T>)) -> Self {
            Self {
                before: before.map(U::from),
                after: after.map(U::from),
                status,
            }
        }
    }

    #[test]
    fn test_compare_lists() {
        let test = |v1: Vec<&str>, v2: Vec<&str>, expected: Vec<(ChangeState, Option<&str>, Option<&str>)>| {
            let l1 = from_vec(v1);
            let l2 = from_vec(v2);
            let diff_items = DiffItem::new_diff_list(&l1, &l2);
            let expected: Vec<DiffItem<String>> = from_vec(expected);
            assert_eq!(expected, diff_items);
        };

        // single change
        test(
            vec!["foo", "bar"],
            vec!["foo2", "bar"],
            vec![
                (ChangeState::Change, Some("foo"), Some("foo2")),
                (ChangeState::None, Some("bar"), None),
            ]
        );
        // no change
        test(
            vec!["foo", "bar"],
            vec!["foo", "bar"],
            vec![
                (ChangeState::None, Some("foo"), None),
                (ChangeState::None, Some("bar"), None),
            ]
        );
        // swap
        test(
            vec!["bar", "foo"],
            vec!["foo", "bar"],
            vec![
                (ChangeState::Change, Some("bar"), Some("foo")),
                (ChangeState::Change, Some("foo"), Some("bar")),
            ]
        );
        // push
        test(
            vec!["foo", "bar"],
            vec!["foo", "bar", "baz"],
            vec![
                (ChangeState::None, Some("foo"), None),
                (ChangeState::None, Some("bar"), None),
                (ChangeState::Add, None, Some("baz")),
            ]
        );
        // pop
        test(
            vec!["foo", "bar", "baz"],
            vec!["foo", "bar"],
            vec![
                (ChangeState::None, Some("foo"), None),
                (ChangeState::None, Some("bar"), None),
                (ChangeState::Delete, Some("baz"), None),
            ]
        );

        // prepend
        test(
            vec!["bar", "baz"],
            vec!["foo", "bar", "baz"],
            vec![
                (ChangeState::Change, Some("bar"), Some("foo")),
                (ChangeState::Change, Some("baz"), Some("bar")),
                (ChangeState::Add, None, Some("baz")),
            ]
        );
        // remove front
        test(
            vec!["foo", "bar", "baz"],
            vec!["bar", "baz"],
            vec![
                (ChangeState::Change, Some("foo"), Some("bar")),
                (ChangeState::Change, Some("bar"), Some("baz")),
                (ChangeState::Delete, Some("baz"), None),
            ]
        );
    }

}
