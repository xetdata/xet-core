use crate::{
    analyzer_trait::Analyzer,
    sketches_bridge::{self, ffi::frequent_item_with_range},
};
use serde::{Deserialize, Serialize};
use sketches_bridge::ffi::frequent_item;
use std::{mem::take, pin::Pin};
use truncrate::TruncateToBoundary;

const FREQUENT_ITEM_SUMMARY_SIZE: usize = 50;
const ITEM_SUMMARY_MAX_STRING_LEN: usize = 50;

// The gymnastics we have to go through to get the items_with_count_ranges to serialize and deserialize properly
#[allow(clippy::type_complexity)]
#[derive(Serialize, Deserialize, PartialEq, Eq, Default, Debug, Clone)]
pub struct SpaceSavingSketchSummaryInner {
    pub version: u16,
    pub items_with_count_ranges: Vec<(String, (u64, u64))>,
}

/** The summary information on what will be reported
 *  from the space saving sketch.
 */
#[derive(Serialize, Deserialize, PartialEq, Eq, Default, Debug, Clone)]
pub struct SpaceSavingSketchSummary {
    pub count: usize,
    pub guaranteed_frequent_items: Vec<(String, usize)>,
    pub frequent_items: Vec<(String, usize)>,

    // The Option<()> is to allow further extensibility.
    pub data: Option<SpaceSavingSketchSummaryInner>,
}

/**
 * This class implements the Space Saving Sketch as described in
 * Ahmed Metwally † Divyakant Agrawal Amr El Abbadi. Efficient Computation of
 * Frequent and Top-k Elements in Data Streams.
 *
 * It provides an efficient one pass scan of all the data and provides an
 * estimate all the frequently occuring elements, with guarantees that all
 * elements with occurances >= \epsilon N will be reported.
 *
 * \code
 *   let ss = SpaceSavingSketch::default();
 *   // repeatedly call
 *   ss.add(s); // s is &str  
 *   // will return an array containing all the elements tracked
 *   // not all elements may be truly frequent items
 *   let items : Vec<(&str, usize)> = ss.frequent_items()
 *   // will return a vector containing all the elements tracked which are
 *   // guaranteed to have occurances >= \epsilon N
 * \endcode
 *
 */
pub struct SpaceSavingSketch {
    sss: cxx::SharedPtr<sketches_bridge::ffi::space_saving_sketch_t>,
}

// Declare these explicitly
unsafe impl Send for SpaceSavingSketch {}
unsafe impl Sync for SpaceSavingSketch {}

const SPACE_SAVING_SKETCH_EPS_THRESHHOLD: f64 = 0.0001;

impl Default for SpaceSavingSketch {
    fn default() -> Self {
        Self::new(SPACE_SAVING_SKETCH_EPS_THRESHHOLD)
    }
}

impl Analyzer<&str> for SpaceSavingSketch {
    /// Adds an item to the sketch.
    fn add(&mut self, value: &str) {
        sketches_bridge::ffi::sss_add_element(self.sss.clone(), value.to_string());
    }

    /// Returns the number of items inserted into the sketch.
    fn len(&self) -> usize {
        sketches_bridge::ffi::sss_size(self.sss.clone())
    }

    /// Resets the sketch.
    fn clear(&mut self) {
        sketches_bridge::ffi::sss_clear(self.sss.clone());
    }
}

impl SpaceSavingSketch {
    /**
     * Initalizes a save saving sketch using 1 / epsilon buckets.  The
     * resultant hyperloglog datastructure will use O(1 / epsilon)
     * memory, and guarantees that all elements with occurances >=
     * \epsilon N will be reported.
     */
    pub fn new(epsilon: f64) -> Self {
        Self {
            sss: sketches_bridge::ffi::new_space_saving_sketch(epsilon),
        }
    }

    fn convert_items(
        &self,
        mut v: ::cxx::UniquePtr<::cxx::CxxVector<sketches_bridge::ffi::frequent_item>>,
    ) -> Vec<(String, usize)> {
        // This all took longer than it should have to figure out.
        v.as_mut()
            .unwrap()
            .iter_mut()
            .map(|x| {
                let mut x = unsafe { take(Pin::<&mut frequent_item>::into_inner_unchecked(x)) };
                (take(&mut x.value), x.count)
            })
            .collect()
    }

    /**
     * Merges a second space saving sketch into the current sketch.
     */
    pub fn combine(&mut self, other: &Self) {
        sketches_bridge::ffi::sss_combine(self.sss.clone(), other.sss.clone());
    }

    /**
     * Returns all the elements tracked by the sketch as well as an
     * estimated count. All elements returned are guaranteed to have
     * occurance >= epsilon * m_size
     */
    pub fn guaranteed_frequent_items(&self) -> Vec<(String, usize)> {
        self.convert_items(sketches_bridge::ffi::sss_guaranteed_frequent_items(
            self.sss.clone(),
        ))
    }

    /**
     * Returns all the elements tracked by the sketch as well as count ranges.  Returns
     * a vector of (element, (min_count, max_count) ) ranges.
     *
     */
    pub fn frequent_items_with_count_ranges(&self) -> Vec<(String, (u64, u64))> {
        let mut v = sketches_bridge::ffi::sss_frequent_items_with_range(self.sss.clone());

        // This all took longer than it should have to figure out.
        v.as_mut()
            .unwrap()
            .iter_mut()
            .map(|x| {
                let mut x = unsafe {
                    take(Pin::<&mut frequent_item_with_range>::into_inner_unchecked(
                        x,
                    ))
                };
                (take(&mut x.value), (x.count_min as u64, x.count_max as u64))
            })
            .collect()
    }

    /**
     * Returns all the elements tracked by the sketch as well as an
     * estimated count. The estimated can be a large overestimate.
     */
    pub fn frequent_items(&self) -> Vec<(String, usize)> {
        self.convert_items(sketches_bridge::ffi::sss_frequent_items(self.sss.clone()))
    }

    /** Returns the total number of items seen so far.
     *
     */
    pub fn count(&self) -> usize {
        sketches_bridge::ffi::sss_size(self.sss.clone())
    }

    /**  Summarize the contents of the space saving sketch into a digest of the items.  
     *
     *   Currently, the primary content given and recorded is simply the most common
     *   items with the count ranges.
     *
     */
    pub fn summary(&self) -> Option<SpaceSavingSketchSummary> {
        let mut v = self.frequent_items_with_count_ranges();

        // Sort by reducing count so we keep the most frequent items.
        v.sort_by(|a, b| b.1.cmp(&a.1));
        v.truncate(FREQUENT_ITEM_SUMMARY_SIZE);
        for (s, _) in v.iter_mut() {
            if s.len() > ITEM_SUMMARY_MAX_STRING_LEN {
                let mut s1 = s
                    .truncate_to_byte_offset(ITEM_SUMMARY_MAX_STRING_LEN)
                    .to_string();
                s1.push_str("...");
                *s = s1;
            }
        }

        Some(SpaceSavingSketchSummary {
            count: self.count(),
            frequent_items: Vec::new(),
            guaranteed_frequent_items: Vec::new(),
            data: Some(SpaceSavingSketchSummaryInner {
                version: 0,
                items_with_count_ranges: v,
            }),
        })
    }
}

#[cfg(test)]
mod sketch_tests {
    use super::*;
    use crate::analyzer_trait::Analyzer;
    use anyhow::Result;
    use rand::{distributions::Alphanumeric, Rng};
    use std::collections::HashMap;
    use truncrate::TruncateToBoundary;

    #[test]
    fn test_truncrate() {
        let s = "🤚🏾a🤚 ";
        assert_eq!(s.truncate_to_byte_offset(0), "");
        assert_eq!(s.truncate_to_byte_offset(7), "");
        assert_eq!(s.truncate_to_byte_offset(8), "🤚🏾");
        assert_eq!(s.truncate_to_byte_offset(9), "🤚🏾a");
        assert_eq!(s.truncate_to_byte_offset(10), "🤚🏾a");
        assert_eq!(s.truncate_to_byte_offset(18), s);

        let s = "abcdef";
        let mut s1 = s.truncate_to_byte_offset(2).to_string();
        s1.push_str("...");
        assert_eq!(5, s1.len());
    }
    #[test]
    fn test_basic() {
        let mut sketch = SpaceSavingSketch::default();
        let string_values: Vec<String> = (0..100).map(|i| format!("{:?}", i)).collect();

        let mut true_values: Vec<usize> = vec![0; 100];

        for i in 0..100 {
            for j in i..100 {
                sketch.add(string_values[j].as_str());
                true_values[j] += 1;
            }
        }

        let results = sketch.guaranteed_frequent_items();

        let value_reference: HashMap<String, usize> = string_values
            .iter()
            .cloned()
            .zip(true_values.iter().copied())
            .collect();

        assert_eq!(results.len(), 100);
        for (v, count) in results.iter() {
            assert_eq!(*count, value_reference[v]);
        }

        // Now, do the above but to a second one, and merge that one in.
        let mut sketch2 = SpaceSavingSketch::default();

        for i in 0..100 {
            for j in 0..i {
                sketch2.add(string_values[j].as_str());
                true_values[j] += 1;
            }
        }

        sketch.combine(&sketch2);
        let results = sketch.guaranteed_frequent_items();

        let value_reference: HashMap<String, usize> =
            string_values.iter().cloned().zip(true_values).collect();

        assert_eq!(results.len(), 100);
        for (v, count) in results.iter() {
            assert_eq!(*count, value_reference[v]);
        }
    }
    #[test]
    fn test_summary_stuff() -> Result<()> {
        // Test that we only choose the top 50 values,
        // and that all the strings get properly truncated.

        let mut sketch = SpaceSavingSketch::default();
        let n = 2 * FREQUENT_ITEM_SUMMARY_SIZE;
        let string_values: Vec<String> = (0..n)
            .map(|_| {
                rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(100)
                    .map(char::from)
                    .collect()
            })
            .collect();

        let mut true_values: Vec<u64> = vec![0; n];
        let mut count = 0;

        for i in 0..n {
            // Adding it like this will preserve the correct order.
            for j in 0..i {
                sketch.add(string_values[j].as_str());
                true_values[j] += 1;
                count += 1;
            }
        }

        let summary = sketch.summary().unwrap();
        let items = &summary.data.unwrap().items_with_count_ranges;

        assert_eq!(items.len(), FREQUENT_ITEM_SUMMARY_SIZE);
        assert_eq!(summary.count, count);

        for i in 0..FREQUENT_ITEM_SUMMARY_SIZE {
            let fi_s = &items[i];
            assert_eq!(
                &fi_s.0[..ITEM_SUMMARY_MAX_STRING_LEN],
                &string_values[i][..ITEM_SUMMARY_MAX_STRING_LEN]
            );
            assert_eq!(fi_s.0.len(), ITEM_SUMMARY_MAX_STRING_LEN + 3);

            assert_eq!(fi_s.1 .0, true_values[i]);
            assert_eq!(fi_s.1 .1, true_values[i]);
        }

        Ok(())
    }
}
