/// Smart pointer to hold either a reference to a type, the owned type,
/// or nothing. This is different than std::Cow<'a, B>, since it allows
/// for None to be stored and this enum doesn't support mutation.
///
/// This is needed for situations where a function can return
/// either a reference to a type or the actual type, avoiding
/// cloning the reference, which could be quite expensive.
///
/// If mutation of the element is needed, then we should rethink/reinvestigate
/// whether we can leverage Cow (and some combination with Option) to
/// satisfy our use-case.
pub enum RefOrT<'a, T> {
    Borrowed(&'a T),
    Owned(T),
    None,
}

impl<T> From<T> for RefOrT<'_, T> {
    fn from(val: T) -> Self {
        Self::Owned(val)
    }
}

impl<'a, T> From<&'a T> for RefOrT<'a, T> {
    fn from(val: &'a T) -> Self {
        Self::Borrowed(val)
    }
}

impl<'a, T> RefOrT<'a, T> {
    /// Gets a reference to the contents contained within (if any).
    pub fn get(&'a self) -> Option<&'a T> {
        match self {
            RefOrT::Borrowed(opt) => Some(*opt),
            RefOrT::Owned(opt) => Some(opt),
            RefOrT::None => None,
        }
    }
}

impl<T> Default for RefOrT<'_, T> {
    fn default() -> Self {
        Self::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn test_borrow() {
        let expected = "foo".to_string();
        let r: RefOrT<String> = RefOrT::from(&expected);
        let actual = r.get().unwrap();
        assert_eq!(&expected, actual);
    }

    #[test]
    fn test_owned() {
        let expected = "foo".to_string();
        let r: RefOrT<String> = RefOrT::from(expected.clone());
        let actual = r.get().unwrap();
        assert_eq!(&expected, actual);
    }

    #[test]
    fn test_none() {
        let r: RefOrT<String> = RefOrT::None;
        assert!(r.get().is_none());
    }

    #[test]
    fn test_uncloned() {
        let expected = RefCell::new(12345);
        let r: RefOrT<RefCell<i32>> = RefOrT::from(&expected);
        let actual = r.get().unwrap();
        actual.replace(45);
        assert_eq!(45, *expected.borrow());
        assert_eq!(&expected, actual);
    }
}
