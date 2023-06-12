use float_cmp::Ulps;
use serde::{Deserialize, Serialize};
use std::cmp::min;

use crate::analyzer_trait::Analyzer;

// Streaming histogram expects NUM_BINS cleanly divisible by 4
const NUM_BINS: usize = 20;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct FloatHistogramSummary {
    pub bin_counts: Vec<usize>,
    pub neg_inf_count: usize,
    pub pos_inf_count: usize,
    pub nan_count: usize,
    pub data_min: f64,
    pub data_max: f64,
    pub data_avg: f64,
    pub data_std: f64,
    pub data_var: f64,
    pub scale_min: f64,
    pub scale_max: f64,
    pub initial_scale_min: f64,
    pub initial_scale_max: f64,
    _buffer: Option<()>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FloatHistogram {
    bin_counts: [usize; NUM_BINS],
    neg_inf_count: usize,
    pos_inf_count: usize,
    nan_count: usize,
    data_min: f64,
    data_max: f64,
    data_avg: f64,
    data_var: f64,
    scale_min: f64,
    scale_max: f64,
    initial_scale_min: f64,
    initial_scale_max: f64,
}

fn get_bin_index(value: f64, min: f64, max: f64) -> usize {
    let range = max - min;
    let count = NUM_BINS as f64;
    debug_assert!(value >= min);
    debug_assert!(value <= max);
    let bin = ((value - min) / range) * count;
    let mut index = bin.floor() as usize;
    debug_assert!(index <= NUM_BINS);
    if index == NUM_BINS {
        index -= 1;
    }
    index
}

impl Analyzer<f64> for FloatHistogram {
    fn add(&mut self, value: f64) {
        // deal with inf/nan first -- just need to count them

        if value.is_nan() {
            self.nan_count += 1;
            return;
        }
        if value.is_infinite() {
            if value.is_sign_negative() {
                self.neg_inf_count += 1;
            } else {
                self.pos_inf_count += 1;
            }
            return;
        }

        if self.initial_scale_min.is_nan() {
            debug_assert!(f64::is_nan(self.initial_scale_max));
            self.initial_scale_min = value.prev();
            self.scale_min = self.initial_scale_min;
            self.initial_scale_max = value.next();
            self.scale_max = self.initial_scale_max;
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
        debug_assert!(self.scale_max >= self.data_max);
        debug_assert!(self.scale_min <= self.data_min);

        // increment count for bin
        let bin = self.get_bin_index(value);
        self.bin_counts[bin] += 1;

        let n = self.len_finite() as f64;
        self.data_var += (value - self.data_avg).powi(2) * (n - 1.0) / n;
        self.data_avg += (value - self.data_avg) / n;
    }

    fn len_finite(&self) -> usize {
        self.bin_counts.iter().sum()
    }

    fn len(&self) -> usize {
        self.len_finite() + self.neg_inf_count + self.pos_inf_count + self.nan_count
    }

    fn clear(&mut self) {
        self.bin_counts = Default::default();
        self.neg_inf_count = 0;
        self.pos_inf_count = 0;
        self.nan_count = 0;
        self.data_avg = 0.0;
        self.data_var = 0.0;
        self.data_min = f64::INFINITY;
        self.data_max = f64::NEG_INFINITY;
        self.scale_min = self.initial_scale_min;
        self.scale_max = self.initial_scale_max;
    }
}

impl Default for FloatHistogram {
    fn default() -> Self {
        Self {
            bin_counts: Default::default(),
            neg_inf_count: 0,
            pos_inf_count: 0,
            nan_count: 0,
            data_min: f64::INFINITY,
            data_max: f64::NEG_INFINITY,
            data_avg: 0.0,
            data_var: 0.0,
            scale_min: f64::NAN,
            scale_max: f64::NAN,
            initial_scale_min: f64::NAN,
            initial_scale_max: f64::NAN,
        }
    }
}

impl FloatHistogram {
    pub fn new(initial_scale_min: f64, initial_scale_max: f64) -> Self {
        assert!(!f64::is_nan(initial_scale_min));
        assert!(!f64::is_nan(initial_scale_max));

        Self {
            scale_min: initial_scale_min,
            scale_max: initial_scale_max,
            initial_scale_min,
            initial_scale_max,
            ..Default::default()
        }
    }

    pub fn rescale(&mut self, new_min: f64, new_max: f64) {
        debug_assert!(new_min.is_finite());
        debug_assert!(new_max.is_finite());
        debug_assert!(new_max >= new_min);

        // collapse bins towards the center to expand range by 2x
        while new_min < self.scale_min || new_max > self.scale_max {
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

            // bump up scale by 2x
            let range = self.scale_max - self.scale_min;
            debug_assert!(range > 0.0);
            self.scale_max += (range / 2.0).max(f64::EPSILON);
            self.scale_min -= (range / 2.0).max(f64::EPSILON);
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

        // Before len gets updated
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
        self.nan_count += other_copy.nan_count;
        self.pos_inf_count += other_copy.pos_inf_count;
        self.neg_inf_count += other_copy.neg_inf_count;
    }

    fn get_bin_index(&self, value: f64) -> usize {
        get_bin_index(value, self.scale_min, self.scale_max)
    }

    pub fn summary(&self) -> Option<FloatHistogramSummary> {
        if !self.initial_scale_min.is_finite() {
            // not initialized
            return None;
        }
        let var = if self.len_finite() > 1 {
            self.data_var / ((self.len_finite() - 1) as f64)
        } else {
            0.0
        };
        Some(FloatHistogramSummary {
            bin_counts: Vec::from(self.bin_counts),
            neg_inf_count: self.neg_inf_count,
            pos_inf_count: self.pos_inf_count,
            nan_count: self.nan_count,
            data_min: self.data_min,
            data_max: self.data_max,
            data_avg: self.data_avg,
            data_std: var.sqrt(),
            data_var: var,
            scale_min: self.scale_min,
            scale_max: self.scale_max,
            initial_scale_min: self.initial_scale_min,
            initial_scale_max: self.initial_scale_max,
            _buffer: None,
        })
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
        neg_inf_count: usize,
        pos_inf_count: usize,
        nan_count: usize,
        min: f64,
        max: f64,
        avg: f64,
        var: f64,
        total_count: usize,
    }

    impl NonStreamingHistogramForTesting {
        fn new(min: f64, max: f64) -> Self {
            Self {
                bin_counts: Default::default(),
                neg_inf_count: 0,
                pos_inf_count: 0,
                nan_count: 0,
                min,
                max,
                avg: 0.0,
                var: 0.0,
                total_count: 0,
            }
        }
    }

    impl Analyzer<f64> for NonStreamingHistogramForTesting {
        fn add(&mut self, value: f64) {
            self.total_count += 1;
            if value.is_nan() {
                self.nan_count += 1;
                return;
            }
            if value.is_infinite() {
                if value.is_sign_negative() {
                    self.neg_inf_count += 1;
                } else {
                    self.pos_inf_count += 1;
                }
                return;
            }

            let bin = get_bin_index(value, self.min, self.max);
            self.bin_counts[bin] += 1;

            let n = self.len_finite() as f64;
            self.var += (value - self.avg).powi(2) * (n - 1.0) / n;
            self.avg += (value - self.avg) / n;
        }

        fn len(&self) -> usize {
            self.total_count
        }

        fn len_finite(&self) -> usize {
            self.total_count - self.nan_count - self.neg_inf_count - self.pos_inf_count
        }

        fn clear(&mut self) {
            self.bin_counts = Default::default();
            self.neg_inf_count = 0;
            self.pos_inf_count = 0;
            self.nan_count = 0;
            self.avg = 0.0;
            self.var = 0.0;
            self.min = f64::INFINITY;
            self.max = f64::NEG_INFINITY;
        }
    }

    fn check_histogram(input: &Vec<f64>, default_init: bool) -> FloatHistogram {
        assert!(!input.is_empty());
        assert!(input[0].is_finite());
        let mut streaming = if default_init {
            FloatHistogram::default()
        } else {
            FloatHistogram::new(
                input[0] - (10.0 * f64::EPSILON),
                input[0] + (10.0 * f64::EPSILON),
            )
        };
        for value in input {
            streaming.add(*value);
        }
        let mut simple =
            NonStreamingHistogramForTesting::new(streaming.scale_min, streaming.scale_max);
        for value in input {
            simple.add(*value);
        }
        assert_eq!(streaming.data_avg, simple.avg);
        assert_eq!(streaming.data_var, simple.var);
        assert_eq!(streaming.bin_counts, simple.bin_counts);
        assert_eq!(streaming.nan_count, simple.nan_count);
        assert_eq!(streaming.neg_inf_count, simple.neg_inf_count);
        assert_eq!(streaming.pos_inf_count, simple.pos_inf_count);
        assert_eq!(streaming.len(), simple.len());
        streaming
    }

    fn compare_histogram(hist1: FloatHistogram, hist2: FloatHistogram) {
        let eps = 10_f64.powi(-10); // Make eps smaller later
        assert_eq!(hist1.bin_counts, hist2.bin_counts);
        assert_eq!(hist1.neg_inf_count, hist2.neg_inf_count);
        assert_eq!(hist1.pos_inf_count, hist2.pos_inf_count);
        assert_eq!(hist1.data_min, hist2.data_min);
        assert_eq!(hist1.data_max, hist2.data_max);
        assert_relative_eq!(hist1.data_avg, hist2.data_avg, epsilon = eps);
        assert_relative_eq!(hist1.data_var, hist2.data_var, epsilon = eps);
        assert_eq!(hist1.scale_min, hist2.scale_min);
        assert_eq!(hist1.scale_max, hist2.scale_max);
        assert_eq!(hist1.initial_scale_min, hist2.initial_scale_min);
        assert_eq!(hist1.bin_counts, hist2.bin_counts);
        assert_eq!(hist1.initial_scale_max, hist2.initial_scale_max);
        assert_eq!(hist1.len(), hist2.len());
    }

    #[test]
    fn test_simple_histogram() {
        let input: Vec<f64> = vec![
            9.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 5.0, 8.0, 8.0, 8.0, 8.0,
        ];
        check_histogram(&input, false);
        check_histogram(&input, true);
    }

    #[test]
    fn test_random_data() {
        use rand::prelude::*;
        let mut input: Vec<f64> = vec![];
        let mut rng = StdRng::seed_from_u64(0);
        let num_elements = 100000;
        for _ in 0..num_elements {
            let element = rng.gen();
            input.push(element);
        }
        check_histogram(&input, false);
    }

    #[test]
    fn test_infs_and_nans() {
        let input = vec![
            -25.0,
            100.5,
            -350.2,
            f64::NAN,
            234.1,
            f64::INFINITY,
            98.6,
            f64::NAN,
            87.1,
            -87.2,
            f64::NEG_INFINITY,
            -240.3,
            f64::INFINITY,
            f64::NAN,
        ];
        let histogram = check_histogram(&input, false);
        assert_eq!(histogram.nan_count, 3);
        assert_eq!(histogram.neg_inf_count, 1);
        assert_eq!(histogram.pos_inf_count, 2);

        let histogram = check_histogram(&input, true);
        assert_eq!(histogram.nan_count, 3);
        assert_eq!(histogram.neg_inf_count, 1);
        assert_eq!(histogram.pos_inf_count, 2);
    }

    #[test]
    fn test_combine() {
        let input1 = vec![-25.0, 100.5, -350.2, f64::NAN, 234.1];
        let input2 = vec![
            f64::INFINITY,
            98.6,
            f64::NAN,
            87.1,
            -87.2,
            f64::NEG_INFINITY,
            -240.3,
            f64::INFINITY,
            f64::NAN,
        ];

        // split it into two histograms and combine
        let mut streaming1 = FloatHistogram::new(
            input1[0] - (10.0 * f64::EPSILON),
            input1[0] + (10.0 * f64::EPSILON),
        );
        for value in &input1 {
            streaming1.add(*value);
        }
        let mut streaming2 = FloatHistogram::new(
            input1[0] - (10.0 * f64::EPSILON),
            input1[0] + (10.0 * f64::EPSILON),
        );
        for value in &input2 {
            streaming2.add(*value);
        }
        streaming1.combine(&streaming2);

        // do it all in one, for comparison
        let mut streaming3 = FloatHistogram::new(
            input1[0] - (10.0 * f64::EPSILON),
            input1[0] + (10.0 * f64::EPSILON),
        );
        for value in &input2 {
            streaming3.add(*value);
        }
        for value in &input1 {
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
        assert_eq!(streaming1.nan_count, simple.nan_count);
        assert_eq!(streaming1.neg_inf_count, simple.neg_inf_count);
        assert_eq!(streaming1.pos_inf_count, simple.pos_inf_count);
        assert_eq!(streaming1.len(), simple.len());
    }
}
