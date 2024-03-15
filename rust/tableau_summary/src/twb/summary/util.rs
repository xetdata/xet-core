pub fn strip_brackets<T: AsRef<str>>(s: T) -> String {
    s.as_ref().trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned()
}

pub fn strip_quotes<T: AsRef<str>>(s: T) -> String {
    s.as_ref().trim_start_matches('"')
        .trim_end_matches('"')
        .to_owned()
}
