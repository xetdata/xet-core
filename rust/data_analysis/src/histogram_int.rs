use std::cmp::min;

use crate::analyzer_trait::Analyzer;
use serde::{Deserialize, Serialize};

// Streaming histogram expects NUM_BINS cleanly divisible by 4
const NUM_BINS: usize = 20;

// A private enum to handle the current initialization state when we use
// the default initializer
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum IntHistogramInitializationState {
    NoValue,
    ValueInMin,
    Initialized,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct IntHistogramSummary {
    bin_counts: Vec<usize>,
    data_min: i64,
    data_max: i64,
    data_avg: f64,
    data_var: f64,
    data_std: f64,
    scale_min: i64,
    scale_max: i64,
    initial_scale_min: i64,
    initial_scale_max: i64,
    _buffer: Option<()>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IntHistogram {
    bin_counts: [usize; NUM_BINS],
    data_min: i64,
    data_max: i64,
    data_avg: f64,
    data_var: f64,
    scale_min: i64,
    scale_max: i64,
    initial_scale_min: i64,
    initial_scale_max: i64,
    initialization_state: IntHistogramInitializationState,
}

fn get_bin_index(value: i64, min: i64, max: i64) -> usize {
    // Can't assert these, since the scale range might actually not be able to accommodate data around the edges.
    // Resizing it by 2x would put it outside of the possible int64 range, so we can't do that.
    // So instead, let's keep track of it by keeping data_max and data_min correct for the data,
    // and all data at the beginning/end will fall into the first/last bucket.
    // This is a literal edge case.
    /*
    debug_assert!($value >= $min);
    debug_assert!($value <= $max);
    */
    let offset = (value as f64) - (min as f64);
    let range = (max as f64) - (min as f64);
    let count = NUM_BINS as f64;
    let bin = (offset / range) * count;
    if bin < 0.0 {
        return 0;
    }
    let mut index = bin as usize;
    if index >= NUM_BINS {
        index = NUM_BINS - 1;
    }
    index
}

impl Analyzer<i64> for IntHistogram {
    fn add(&mut self, value: i64) {
        match self.initialization_state {
            IntHistogramInitializationState::NoValue => {
                self.scale_min = value;
                self.initialization_state = IntHistogramInitializationState::ValueInMin;
                return;
            }
            IntHistogramInitializationState::ValueInMin => {
                let v2 = self.scale_min;
                *self = Self::new(i64::min(value, v2), i64::max(value, v2));
                self.add(value);
                self.add(v2);
                return;
            }
            IntHistogramInitializationState::Initialized => {}
        }

        // keep track of min/max
        if value < self.data_min {
            self.data_min = value;
        }
        if value > self.data_max {
            self.data_max = value;
        }

        // rescale if needed
        self.rescale(self.data_min, self.data_max);

        /*
        // Can't assert these, since the scale range might actually not be able to accommodate data around the edges.
        // Resizing it by 2x would put it outside of the possible int64 range, so we can't do that.
        // So instead, let's keep track of it by keeping data_max and data_min correct for the data,
        // and all data at the beginning/end will fall into the first/last bucket.
        // This is a literal edge case.
        debug_assert!(self.scale_max >= self.data_max);
        debug_assert!(self.scale_min <= self.data_min);
        */

        // increment count for bin
        let bin = self.get_bin_index(value);
        self.bin_counts[bin] += 1;

        let n = self.len() as f64;
        self.data_var += ((value as f64) - self.data_avg).powi(2) * (n - 1.0) / n;
        self.data_avg += ((value as f64) - self.data_avg) / n;
    }

    fn len(&self) -> usize {
        let count_sum = self.bin_counts.iter().sum();
        count_sum
    }

    fn clear(&mut self) {
        self.bin_counts = Default::default();
        self.data_min = i64::MAX;
        self.data_max = i64::MIN;
        self.data_avg = 0.0;
        self.data_var = 0.0;
        self.scale_min = self.initial_scale_min;
        self.scale_max = self.initial_scale_max;
    }
}

impl Default for IntHistogram {
    fn default() -> Self {
        Self {
            bin_counts: Default::default(),
            data_min: i64::MAX,
            data_max: i64::MIN,
            data_avg: 0.0,
            data_var: 0.0,
            scale_min: 0,
            scale_max: 0,
            initial_scale_min: 0,
            initial_scale_max: 0,
            initialization_state: IntHistogramInitializationState::NoValue,
        }
    }
}

impl IntHistogram {
    pub fn new(initial_scale_min: i64, initial_scale_max: i64) -> Self {
        let mut scale_min = initial_scale_min;
        let mut scale_max = initial_scale_max;

        // check preconditions
        let range = initial_scale_max - initial_scale_min;
        assert!(range >= 2); // initial scale range for int histogram must be at least 2
        assert_eq!(range % 2, 0); // initial scale range for int histogram must be divisible by 2

        // We also need to make sure we can evenly divide the scale range into REAL_BINS,
        // so that we don't end up with non-integer bin boundaries
        let bins = NUM_BINS as i64;
        let mut pad = range % bins;
        if pad > 0 {
            pad = bins - pad;
            let pad_left = pad / 2;
            let pad_right = (pad / 2) + (pad % 2);
            scale_min -= pad_left;
            scale_max += pad_right;
        }

        Self {
            bin_counts: Default::default(),
            data_min: i64::MAX,
            data_max: i64::MIN,
            data_avg: 0.0,
            data_var: 0.0,
            scale_min,
            scale_max,
            initial_scale_min: scale_min,
            initial_scale_max: scale_max,
            initialization_state: IntHistogramInitializationState::Initialized,
        }
    }

    pub fn rescale(&mut self, new_min: i64, new_max: i64) {
        debug_assert!(new_max >= new_min);

        // collapse bins towards the center to expand range by 2x
        while new_min < self.scale_min || new_max > self.scale_max {
            // bump up scale by 2x
            if let Some(range) = self.scale_max.checked_sub(self.scale_min) {
                debug_assert!(range > 0);
                debug_assert_eq!(range % 2, 0);

                let new_scale_max: i64 =
                    if let Some(new_max) = self.scale_max.checked_add(range / 2) {
                        new_max
                    } else {
                        // ignore resizes that put us outside the range of possible integers.
                        // resulting histogram will be wrong at the edges.
                        // this is an edge case.
                        break;
                    };

                let new_scale_min: i64 =
                    if let Some(new_min) = self.scale_min.checked_sub(range / 2) {
                        new_min
                    } else {
                        // ignore resizes that put us outside the range of possible integers.
                        // resulting histogram will be wrong at the edges.
                        // this is an edge case.
                        break;
                    };
                self.scale_max = new_scale_max;
                self.scale_min = new_scale_min;
            } else {
                // ignore resizes that put us outside the range of possible integers.
                // resulting histogram will be wrong at the edges.
                // this is an edge case.
                break;
            }

            // first, combine bins next to each other (every other bin)
            let mut i = (NUM_BINS / 2) - 1;
            while i > 0 {
                self.bin_counts[i] += self.bin_counts[i - 1];
                i -= min(i, 2);
            }
            i = NUM_BINS / 2;
            while i < NUM_BINS {
                self.bin_counts[i] += self.bin_counts[i + 1];
                i += 2;
            }

            // then, collapse them inward towards the center
            i = 0;
            while i < NUM_BINS / 4 {
                self.bin_counts[(NUM_BINS / 2) + i] = self.bin_counts[(NUM_BINS / 2) + (2 * i)];
                self.bin_counts[(NUM_BINS / 2) - (i + 1)] =
                    self.bin_counts[(NUM_BINS / 2) - ((2 * i) + 1)];
                i += 1;
            }

            // finally, zero out the newly-unused bins
            i = (NUM_BINS * 3) / 4;
            while i < NUM_BINS {
                self.bin_counts[i] = 0;
                i += 1;
            }
            i = 0;
            while i < (NUM_BINS / 4) {
                self.bin_counts[i] = 0;
                i += 1;
            }
        }
    }

    pub fn combine(&mut self, other: &Self) {
        // precondition for combining histograms:
        // they must have been initialized originally with the same scale min/max
        // (though they could have been resized since)
        assert_eq!(self.initial_scale_min, other.initial_scale_min);
        assert_eq!(self.initial_scale_max, other.initial_scale_max);

        let combined_min = self.scale_min.min(other.scale_min);
        let combined_max = self.scale_max.max(other.scale_max);
        self.rescale(combined_min, combined_max);
        let mut other_copy = other.clone();
        other_copy.rescale(combined_min, combined_max);

        // we now expect the two histograms to have the same scale (the bins line up)
        debug_assert_eq!(self.scale_min, other_copy.scale_min);
        debug_assert_eq!(self.scale_max, other_copy.scale_max);

        let an = self.len_finite() as f64;
        let bn = self.len_finite() as f64;
        let cn = an + bn;

        self.data_var +=
            other_copy.data_var + (other_copy.data_avg - self.data_avg).powi(2) * an * bn / cn;
        self.data_avg = (an * self.data_avg + bn * other_copy.data_avg) / cn;

        for idx in 0..NUM_BINS {
            self.bin_counts[idx] += other_copy.bin_counts[idx];
        }
        self.data_max = self.data_max.max(other_copy.data_max);
        self.data_min = self.data_min.min(other_copy.data_min);
    }

    fn get_bin_index(&self, value: i64) -> usize {
        get_bin_index(value, self.scale_min, self.scale_max)
    }

    pub fn summary(&self) -> IntHistogramSummary {
        let var = if self.len_finite() > 1 {
            self.data_var / ((self.len_finite() - 1) as f64)
        } else {
            0.0
        };
        IntHistogramSummary {
            bin_counts: Vec::from(&self.bin_counts[..]),
            data_min: self.data_min,
            data_max: self.data_max,
            data_avg: self.data_avg,
            data_var: var,
            data_std: var.sqrt(),
            scale_min: self.scale_min,
            scale_max: self.scale_max,
            initial_scale_min: self.initial_scale_min,
            initial_scale_max: self.initial_scale_max,
            _buffer: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    // This implementation is simpler, but needs to know min/max up front since it can't rescale.
    // Thus it can't be done in a single pass over the input.
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct NonStreamingHistogramForTesting {
        bin_counts: [usize; NUM_BINS],
        min: i64,
        max: i64,
        avg: f64,
        var: f64,
        total_count: usize,
    }

    impl NonStreamingHistogramForTesting {
        fn new(min: i64, max: i64) -> Self {
            Self {
                bin_counts: Default::default(),
                min,
                max,
                avg: 0.0,
                var: 0.0,
                total_count: 0,
            }
        }
    }

    impl Analyzer<i64> for NonStreamingHistogramForTesting {
        fn add(&mut self, value: i64) {
            self.total_count += 1;
            // increment count for bin
            let bin = get_bin_index(value, self.min, self.max);
            self.bin_counts[bin] += 1;
            let n = self.len() as f64;
            self.var += ((value as f64) - self.avg).powi(2) * (n - 1.0) / n;
            self.avg += ((value as f64) - self.avg) / n;
        }

        fn len(&self) -> usize {
            self.total_count
        }

        fn clear(&mut self) {
            self.bin_counts = Default::default();
            self.min = i64::MAX;
            self.max = i64::MIN;
            self.avg = 0.0;
            self.var = 0.0;
        }
    }

    fn compare_histogram(hist1: IntHistogram, hist2: IntHistogram) {
        let eps = 10_f64.powi(-10); // Make eps smaller later
        assert_eq!(hist1.bin_counts, hist2.bin_counts);
        assert_eq!(hist1.data_min, hist2.data_min);
        assert_eq!(hist1.data_max, hist2.data_max);
        assert_relative_eq!(hist1.data_avg, hist2.data_avg, epsilon = eps);
        assert_relative_eq!(hist1.data_var, hist2.data_var, epsilon = eps);
        assert_eq!(hist1.scale_min, hist2.scale_min);
        assert_eq!(hist1.scale_max, hist2.scale_max);
        assert_eq!(hist1.initial_scale_min, hist2.initial_scale_min);
        assert_eq!(hist1.initialization_state, hist2.initialization_state);
        assert_eq!(hist1.initial_scale_max, hist2.initial_scale_max);
        assert_eq!(hist1.len(), hist2.len());
    }

    fn check_histogram(input: &Vec<i64>, use_default: bool) -> IntHistogram {
        assert!(!input.is_empty());
        let mut streaming = if use_default {
            IntHistogram::default()
        } else {
            IntHistogram::new(input[0] - 1, input[0] + 1)
        };
        for value in input {
            streaming.add(*value);
        }
        let mut simple =
            NonStreamingHistogramForTesting::new(streaming.scale_min, streaming.scale_max);
        for value in input {
            simple.add(*value);
        }
        assert_eq!(streaming.bin_counts, simple.bin_counts);
        assert_eq!(streaming.len(), simple.len());
        streaming
    }

    #[test]
    fn test_simple_histogram() {
        let input: Vec<i64> = vec![9, 1, 2, 3, 4, 5, 6, 7, 8, 8, 8, 8, 8, 5];
        check_histogram(&input, false);
        check_histogram(&input, true);
    }

    #[test]
    fn test_random_data() {
        use rand::prelude::*;
        let mut input: Vec<i64> = vec![];
        let mut rng = StdRng::seed_from_u64(0);
        let num_elements = 100000;
        for _ in 0..num_elements {
            let element: i64 = rng.gen();
            input.push(element / 100);
        }
        check_histogram(&input, false);
    }

    #[test]
    fn test_combine() {
        let input1 = vec![-25, 100, -350, 234];
        let input2 = vec![98, 87, -87, -240];

        // split it into two histograms and combine
        let mut streaming1 = IntHistogram::new(input1[0] - 1, input1[0] + 1);
        for value in &input1 {
            streaming1.add(*value);
        }
        let mut streaming2 = IntHistogram::new(input1[0] - 1, input1[0] + 1);
        for value in &input2 {
            streaming2.add(*value);
        }
        streaming1.combine(&streaming2);

        // do it all in one, for comparison
        let mut streaming3 = IntHistogram::new(input1[0] - 1, input1[0] + 1);
        for value in &input1 {
            streaming3.add(*value);
        }
        for value in &input2 {
            streaming3.add(*value);
        }

        compare_histogram(streaming1.clone(), streaming3.clone());

        // compare against a simple histogram
        let mut simple =
            NonStreamingHistogramForTesting::new(streaming1.scale_min, streaming1.scale_max);
        for value in &input1 {
            simple.add(*value);
        }
        for value in &input2 {
            simple.add(*value);
        }
        assert_eq!(streaming1.bin_counts, simple.bin_counts);
        assert_eq!(streaming1.len(), simple.len());
    }
}
