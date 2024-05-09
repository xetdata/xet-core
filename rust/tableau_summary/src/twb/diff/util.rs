use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::ops::Range;

use imara_diff::Algorithm::MyersMinimal;
use imara_diff::intern::{InternedInput, Token, TokenSource};
use imara_diff::Sink;
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

    pub fn get_most_changes(&self) -> ChangeState {
        self.0.iter()
            .max_by(|(_, a), (_,b)| a.cmp(b))
            .map(|(k, _)| *k)
            .unwrap_or_default()
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

    /// Produce a minimal list of changes for before -> after, including
    /// any elements that didn't change.
    fn new_diff_list(before: &[T], after: &[T]) -> Vec<Self>
        where
            T: Eq + Hash
    {
        let input = InternedInput::new(DiffTokenSource(before), DiffTokenSource(after));
        imara_diff::diff(MyersMinimal, &input, DiffSink::new(&input))
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

/// Sink used by the imara_diff algorithm to process the change-list
/// into a list of `DiffProducer` structs for the input lists.
struct DiffSink<'a, T, P>
    where
        T: Eq + Hash,
        P: DiffProducer<T>
{
    input: &'a InternedInput<&'a T>,
    vals: Vec<P>,
    before_idx: u32,
    after_idx: u32,
}

impl<'a, T, P> DiffSink<'a, T, P>
    where
        T: Eq + Hash,
        P: DiffProducer<T>
{
    fn new(input: &'a InternedInput<&'a T>) -> Self {
        Self {
            input,
            vals: vec![],
            before_idx: 0,
            after_idx: 0,
        }
    }

    /// Uses the indicated input_vec of Tokens and range to return the vec of values
    /// we can compare against.
    fn hunk(&self, input_vec: &[Token], r: Range<u32>) -> Vec<&T> {
        input_vec[r.start as usize..r.end as usize]
            .iter()
            .map(|&token| self.input.interner[token])
            .collect_vec()
    }

    /// Processes a before/after set of ranges to produce the additions/deletions/changes
    /// and add them to the result Vec.
    fn process_ranges(&mut self, before: Range<u32>, after: Range<u32>) {
        // get before/after hunks from the ranges
        let hunk_before = self.hunk(&self.input.before, before);
        let hunk_after = self.hunk(&self.input.after, after);
        // Use a temporary vec to store Ps since we already immutably borrowed self for the
        // hunks
        let mut vals = vec![];
        if hunk_after.is_empty() {
            // before -> empty ==> hunk was deleted
            vals.extend(hunk_before.into_iter().map(|t| P::new_deletion(t)));
        } else if hunk_before.is_empty() {
            // empty -> after ==> hunk was added
            vals.extend(hunk_after.into_iter().map(|t| P::new_addition(t)));
        } else {
            // before -> after ==> we changed the hunk, iterate on the common length
            let (min_len, excess, is_delete) = if hunk_before.len() > hunk_after.len() {
                (hunk_after.len(), &hunk_before, true)
            } else {
                (hunk_before.len(), &hunk_after, false)
            };
            for i in 0..min_len {
                vals.push(P::new_diff(hunk_before[i], hunk_after[i]));
            }
            // We may have some excess if before.len() != after.len(), so we just go through the
            // excess items and either delete/add.
            for i in excess[min_len..].iter() {
                let d = if is_delete {
                    P::new_deletion(i)
                } else {
                    P::new_addition(i)
                };
                vals.push(d)
            }
        }
        // now that we're done with the hunks, we can mutate self.vals
        self.vals.append(&mut vals);
    }
}

impl<'a, T, P> Sink for DiffSink<'a, T, P>
    where
        T: Eq + Hash,
        P: DiffProducer<T>
{
    type Out = Vec<P>;

    fn process_change(&mut self, before: Range<u32>, after: Range<u32>) {
        // process all indices between the last call and this call (should be unchanged)
        self.process_ranges(self.before_idx..before.start, self.after_idx..after.start);
        // process all indices for this change
        self.process_ranges(before.start..before.end, after.start..after.end);
        // update before/after idx
        self.before_idx = before.end;
        self.after_idx = after.end;
    }

    fn finish(mut self) -> Self::Out {
        // process any remaining items (should be unchanged)
        self.process_ranges(self.before_idx..self.input.before.len() as u32,
                            self.after_idx..self.input.after.len() as u32);
        self.vals
    }
}

/// TokenSource for usage with imara_diff that wraps an input list,
/// providing each element of the list as a token.
struct DiffTokenSource<'a, T: Eq + Hash>(&'a [T]);

impl<'a, T: Eq + Hash> TokenSource for DiffTokenSource<'a, T> {
    type Token = &'a T;
    type Tokenizer = Self;

    fn tokenize(&self) -> Self::Tokenizer {
        Self(self.0)
    }

    fn estimate_tokens(&self) -> u32 {
        self.0.len() as u32
    }
}

impl<'a, T: Eq + Hash> Iterator for DiffTokenSource<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
             return None
        }
        let (item, rem) = self.0.split_at(1);
        self.0 = rem;
        Some(&item[0])
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
                (ChangeState::Delete, Some("bar"), None),
                (ChangeState::None, Some("foo"), None),
                (ChangeState::Add, None, Some("bar")),
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
                (ChangeState::Add, None, Some("foo")),
                (ChangeState::None, Some("bar"), None),
                (ChangeState::None, Some("baz"), None),
            ],
        );
        // remove front
        test(
            vec!["foo", "bar", "baz"],
            vec!["bar", "baz"],
            vec![
                (ChangeState::Delete, Some("foo"), None),
                (ChangeState::None, Some("bar"), None),
                (ChangeState::None, Some("baz"), None),
            ],
        );
        // change middle
        test(
            vec!["foo", "bar", "baz"],
            vec!["foo", "bang", "baz"],
            vec![
                (ChangeState::None, Some("foo"), None),
                (ChangeState::Change, Some("bar"), Some("bang")),
                (ChangeState::None, Some("baz"), None),
            ],
        );
        // change end
        test(
            vec!["foo", "bar", "baz"],
            vec!["foo", "bar", "bar"],
            vec![
                (ChangeState::None, Some("foo"), None),
                (ChangeState::None, Some("bar"), None),
                (ChangeState::Change, Some("baz"), Some("bar")),
            ],
        );
        // all add
        test(
            vec![],
            vec!["foo", "bar", "baz"],
            vec![
                (ChangeState::Add, None, Some("foo")),
                (ChangeState::Add, None, Some("bar")),
                (ChangeState::Add, None, Some("baz")),
            ],
        );
        // all remove
        test(
            vec!["foo", "bar", "baz"],
            vec![],
            vec![
                (ChangeState::Delete, Some("foo"), None),
                (ChangeState::Delete, Some("bar"), None),
                (ChangeState::Delete, Some("baz"), None),
            ],
        );
        // empty
        test(
            vec![],
            vec![],
            vec![],
        );
        // single-add
        test(
            vec![],
            vec!["foo"],
            vec![
                (ChangeState::Add, None, Some("foo")),
            ],
        );
        // single-delete
        test(
            vec!["foo"],
            vec![],
            vec![
                (ChangeState::Delete, Some("foo"), None),
            ],
        );
        // single-change
        test(
            vec!["foo"],
            vec!["bar"],
            vec![
                (ChangeState::Change, Some("foo"), Some("bar")),
            ],
        );

        // mixed cases
        test(
            vec!["a", "b", "c", "d"],
            vec!["e", "a", "c", "d", "d", "b"],
            vec![
                (ChangeState::Add, None, Some("e")),
                (ChangeState::None, Some("a"), None),
                (ChangeState::Delete, Some("b"), None),
                (ChangeState::None, Some("c"), None),
                (ChangeState::None, Some("d"), None),
                (ChangeState::Add, None, Some("d")),
                (ChangeState::Add, None, Some("b")),
            ],
        );
        test(
            vec!["a", "b", "c", "a", "b", "c"],
            vec!["c", "a", "b", "c", "b", "a"],
            vec![
                (ChangeState::Delete, Some("a"), None),
                (ChangeState::Delete, Some("b"), None),
                (ChangeState::None, Some("c"), None),
                (ChangeState::None, Some("a"), None),
                (ChangeState::None, Some("b"), None),
                (ChangeState::None, Some("c"), None),
                (ChangeState::Add, None, Some("b")),
                (ChangeState::Add, None, Some("a")),
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
