pub trait Analyzer<T> {
    /// Adds an item to the analyzer.
    fn add(&mut self, value: T);

    /// Returns the number of items inserted into the analyzer.
    fn len(&self) -> usize;

    /// Returns the number of non NaN/Inf items inserted into the analyzer.
    fn len_finite(&self) -> usize {
        self.len()
    }

    /// Returns true if the analyzer is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Resets the analyzer.
    fn clear(&mut self);
}
