use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::ops::Range;

use imara_diff::Algorithm::Myers;
use imara_diff::intern::{InternedInput, Token, TokenSource};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

/// A generic struct to represent the difference between 2 summary items.
/// Provides helper methods to aid in calculating diffs between 2 summaries.
/// Implements the [DiffProducer] trait to allow easier construction of diffs.
#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Hash, Clone, Debug)]
pub struct DiffItem<T>
    where
        T: Serialize + Default + PartialEq + Eq + Hash + Clone + Debug
{
    pub before: Option<T>,
    pub after: Option<T>,
    pub status: ChangeState,
}

impl<T> DiffProducer<T> for DiffItem<T>
    where
        T: Serialize + Default + PartialEq + Eq + Hash + Clone + Debug
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

/// Different ways a summary can be changed (or lack of a change).
#[derive(Serialize, Deserialize, Default, Eq, Hash, PartialEq, Ord, PartialOrd, Clone, Debug, Copy)]
pub enum ChangeState {
    Add,
    Change,
    Delete,
    #[default]
    None,
}


#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Clone, Debug)]
#[serde(transparent)]
pub struct ChangeMap(HashMap<ChangeState, usize>);

impl Hash for ChangeMap {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.iter()
            .sorted_by_key(|x| x.0)
            .for_each(|(&k, &v)| {
                k.hash(state);
                v.hash(state);
            })
    }
}

impl ChangeMap {
    /// Directly increment the value for the given state by 1.
    pub fn increment_change(&mut self, state: ChangeState) {
        if state != ChangeState::None {
            *self.0.entry(state).or_insert(0) += 1;
        }
    }

    /// Update the map with the change encapsulated by the DiffItem.
    pub fn update<T>(&mut self, item: &DiffItem<T>)
        where
            T: Serialize + Default + PartialEq + Eq + Hash + Clone + Debug
    {
        self.increment_change(item.status)
    }

    pub fn update_first(&mut self, items: &[ChangeState]) {
        if let Some(&s) = items.iter().find(|&&i| (i != ChangeState::None)) {
            self.increment_change(s)
        }
    }

    /// Update the map with the change encapsulated by the DiffItem of an Option.
    /// This differs from `update()` since a DiffItem<Option<T>> contains
    /// Option<Option<T>> for the before/after, so we want to consider the presence
    /// of a Some(None) as a no-op and not update the map.
    pub fn update_option<T>(&mut self, item: &DiffItem<Option<T>>)
        where
            T: Serialize + Default + PartialEq + Eq + Hash + Clone + Debug
    {
        // if either before or after are Some(Some(t)), then we can try incrementing
        // the change.
        if item.before.as_ref().is_some_and(Option::is_some)
            || item.after.as_ref().is_some_and(Option::is_some) {
            self.increment_change(item.status)
        }
    }

    /// Update the map with a list of DiffItem changes
    pub fn update_list<T>(&mut self, items: &[DiffItem<T>])
        where
            T: Serialize + Default + PartialEq + Eq + Hash + Clone + Debug
    {
        items.iter()
            .for_each(|i| self.update(i))
    }

    /// Merge another ChangeMap's values with this ChangeMap
    pub fn merge(&mut self, other: &ChangeMap) {
        other.0.iter()
            .for_each(|(&k, &v)| {
                *self.0.entry(k).or_insert(0) += v
            });
    }

    /// Returns true if there are no changes stored in this ChangeMap.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Gets the number of changes for a particular ChangeState.
    pub fn get(&self, state: ChangeState) -> usize {
        self.0.get(&state).copied().unwrap_or_default()
    }
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
    fn new_diff_list(before: &[T], after: &[T]) -> Vec<Self>
        where
            T: Eq + Hash
    {
        let btok = DiffTokenSource {
            list: before,
            cur_idx: 0,
        };
        let atok = DiffTokenSource {
            list: after,
            cur_idx: 0,
        };
        let input = InternedInput::new(btok, atok);
        let mut vals = Vec::new();
        let (mut before_idx, mut after_idx) = (0, 0);
        let sink = |before_range: Range<u32>, after_range: Range<u32>| {
            before_idx = before_range.end;
            after_idx = after_range.end;
            if before.len() > before_range.start as usize && after.len() > after_range.start as usize {
                vals.push(Self::new_diff(&before[before_range.start as usize], &after[after_range.start as usize]));
            }
        };
        imara_diff::diff(Myers, &input, sink);
        //
        // let mut i = 0;
        // let mut vals = Vec::new();
        // while i < before.len() && i < after.len() {
        //     vals.push(Self::new_diff(&before[i], &after[i]));
        //     i += 1;
        // }
        // while i < before.len() { // i >= after.len()
        //     vals.push(Self::new_deletion(&before[i]));
        //     i += 1;
        // }
        // while i < after.len() { // i >= before.len()
        //     vals.push(Self::new_addition(&after[i]));
        //     i += 1;
        // }
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
                }
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

struct DiffTokenSource<'a, T>
    where
        T: Eq + Hash,
{
    list: &'a [T],
    cur_idx: usize,
}

impl<'a, T> TokenSource for DiffTokenSource<'a, T>
    where
        T: Eq + Hash,
{
    type Token = &'a T;
    type Tokenizer = Self;

    fn tokenize(&self) -> Self::Tokenizer {
        Self {
            list: self.list,
            cur_idx: 0,
        }
    }

    fn estimate_tokens(&self) -> u32 {
        self.list.len() as u32
    }
}

impl<'a, T: Eq + Hash> Iterator for DiffTokenSource<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_idx >= self.list.len() {
            None
        } else {
            let t = &self.list[self.cur_idx];
            self.cur_idx += 1;
            Some(t)
        }
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

    pub fn from_vec<T, U: From<T>>(v: Vec<T>) -> Vec<U> {
        v.into_iter().map(U::from).collect_vec()
    }

    impl<T, U> From<(ChangeState, Option<T>, Option<T>)> for DiffItem<U>
        where
            U: Serialize + Default + PartialEq + Eq + Hash + Clone + Debug + From<T>,
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
            ],
        );
        // no change
        test(
            vec!["foo", "bar"],
            vec!["foo", "bar"],
            vec![
                (ChangeState::None, Some("foo"), None),
                (ChangeState::None, Some("bar"), None),
            ],
        );
        // swap
        test(
            vec!["bar", "foo"],
            vec!["foo", "bar"],
            vec![
                (ChangeState::Change, Some("bar"), Some("foo")),
                (ChangeState::Change, Some("foo"), Some("bar")),
            ],
        );
        // push
        test(
            vec!["foo", "bar"],
            vec!["foo", "bar", "baz"],
            vec![
                (ChangeState::None, Some("foo"), None),
                (ChangeState::None, Some("bar"), None),
                (ChangeState::Add, None, Some("baz")),
            ],
        );
        // pop
        test(
            vec!["foo", "bar", "baz"],
            vec!["foo", "bar"],
            vec![
                (ChangeState::None, Some("foo"), None),
                (ChangeState::None, Some("bar"), None),
                (ChangeState::Delete, Some("baz"), None),
            ],
        );

        // prepend
        test(
            vec!["bar", "baz"],
            vec!["foo", "bar", "baz"],
            vec![
                (ChangeState::Change, Some("bar"), Some("foo")),
                (ChangeState::Change, Some("baz"), Some("bar")),
                (ChangeState::Add, None, Some("baz")),
            ],
        );
        // remove front
        test(
            vec!["foo", "bar", "baz"],
            vec!["bar", "baz"],
            vec![
                (ChangeState::Change, Some("foo"), Some("bar")),
                (ChangeState::Change, Some("bar"), Some("baz")),
                (ChangeState::Delete, Some("baz"), None),
            ],
        );
    }
}

#[cfg(test)]
mod test_change_map {
    use ChangeState::{Add, Change, Delete};

    use super::*;

    #[test]
    fn test_update() {
        // Addition
        let mut m = ChangeMap::default();
        assert!(m.is_empty());
        m.update(&DiffItem::new_addition(&"foo".to_string()));
        assert!(!m.is_empty());
        assert_eq!(1, m.get(Add));

        // Deletion
        let mut m = ChangeMap::default();
        m.update(&DiffItem::new_deletion(&"foo".to_string()));
        assert_eq!(1, m.get(Delete));

        // Change
        let mut m = ChangeMap::default();
        m.update(&DiffItem::new_diff(&"foo".to_string(), &"bar".to_string()));
        assert_eq!(1, m.get(Change));

        // No-op
        let mut m = ChangeMap::default();
        m.update(&DiffItem::new_diff(&"foo".to_string(), &"foo".to_string()));
        assert!(m.is_empty());

        // Multiple
        let mut m = ChangeMap::default();
        m.update(&DiffItem::new_addition(&"foo".to_string()));
        m.update(&DiffItem::new_deletion(&"bar".to_string()));
        m.update(&DiffItem::new_diff(&"f1".to_string(), &"f2".to_string()));
        assert_eq!(1, m.get(Add));
        assert_eq!(1, m.get(Delete));
        assert_eq!(1, m.get(Change));
    }

    #[test]
    fn test_update_option() {
        let mut m = ChangeMap::default();
        m.update_option(&DiffItem::new_addition(&Some("foo".to_string())));
        assert_eq!(1, m.get(Add));

        let mut m = ChangeMap::default();
        m.update_option::<String>(&DiffItem::new_addition(&None));
        assert!(m.is_empty());
    }

    fn diff_list(v: Vec<(&str, ChangeState)>) -> Vec<DiffItem<String>> {
        v.into_iter()
            .map(|(s, st)| match st {
                Add => DiffItem::new_addition(&s.to_string()),
                Change => DiffItem::new_diff(&s.to_string(), &format!("{s}2")),
                Delete => DiffItem::new_deletion(&s.to_string()),
                ChangeState::None => DiffItem::new_diff(&s.to_string(), &s.to_string())
            })
            .collect()
    }

    #[test]
    fn test_update_list() {
        // Addition
        let mut m = ChangeMap::default();
        let v = diff_list(vec![("foo", Add), ("bar", Add)]);
        m.update_list(&v);
        assert_eq!(2, m.get(Add));

        // Deletion
        let mut m = ChangeMap::default();
        let v = diff_list(vec![("foo", Delete), ("bar", Delete)]);
        m.update_list(&v);
        assert_eq!(2, m.get(Delete));

        // Change
        let mut m = ChangeMap::default();
        let v = diff_list(vec![("foo", Change), ("bar", Change)]);
        m.update_list(&v);
        assert_eq!(2, m.get(Change));

        // No-op
        let mut m = ChangeMap::default();
        let v = diff_list(vec![("foo", ChangeState::None), ("bar", ChangeState::None)]);
        m.update_list(&v);
        assert!(m.is_empty());

        // Multiple
        let mut m = ChangeMap::default();
        let v = diff_list(vec![
            ("foo", Add),
            ("bar", Add),
            ("n", ChangeState::None),
            ("c", Change),
            ("d", Delete),
        ]);
        m.update_list(&v);
        assert_eq!(2, m.get(Add));
        assert_eq!(1, m.get(Delete));
        assert_eq!(1, m.get(Change));
    }

    #[test]
    fn test_merge() {
        let mut m = ChangeMap::default();
        let diff = diff_list(vec![
            ("a", Add),
            ("c", Change),
            ("d", Add),
            ("e", ChangeState::None),
        ]);
        m.update_list(&diff);

        let mut m2 = ChangeMap::default();
        let diff2 = diff_list(vec![
            ("a", Add),
            ("b", Delete),
            ("d", Delete),
            ("e", ChangeState::None),
            ("f", ChangeState::None),
        ]);
        m2.update_list(&diff2);

        m.merge(&m2);
        assert_eq!(3, m.get(Add));
        assert_eq!(2, m.get(Delete));
        assert_eq!(1, m.get(Change));
    }
}
