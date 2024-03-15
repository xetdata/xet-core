use std::borrow::Cow;
use std::mem;
use tracing::info;

pub trait ColumnFinder {
    /// Given the name of a column, find the column's display name.
    /// If there is no such column, returns None.
    fn find_column(&self, name: &str) -> Option<Cow<str>>;
    /// Given the logical source name and the column name, get the display name for the
    /// column.
    fn find_column_for_source(&self, source: &str, name: &str) -> Option<Cow<str>>;
}

struct Substituter<'a, T: ColumnFinder> {
    finder: &'a T,
    result: String,
    token: String,
    col_token: String,
    in_token: bool,
    token_pend: bool,
    had_dot: bool,
    dependencies: Vec<(String, String)>,
}

/// Given the ColumnFinder and parameterized string, replace any referenced columns with their
/// Caption'ed representation and return the referenced columns (as tuples of (datasource, column)).
/// e.g. if the column: `[Calc_12345]` has the caption: `'Orders made'`, then given the string:
/// `"CEIL([Calc_12345])"` we will process it into: `"CEIL([Orders made])"` and return `[("", "[Calc_12345]")]`.
pub fn substitute_columns<T: ColumnFinder>(finder: &T, s: &str) -> (Option<String>, Vec<(String, String)>) {
    Substituter::new(finder).substitute(s)
}


impl<'a, T: ColumnFinder> Substituter<'a, T> {

    fn new(finder: &'a T) -> Self {
        Self {
            finder,
            result: String::new(),
            token: String::new(),
            col_token: String::new(),
            in_token: false,
            token_pend: false,
            had_dot: false,
            dependencies: vec![],
        }
    }

    fn substitute(mut self, s: &str) -> (Option<String>, Vec<(String, String)>) {
        for ch in s.chars() {
            let res = match ch {
                '[' => self.process_open(ch),
                ']' => self.process_close(ch),
                '.' => self.process_separator(ch),
                _ => self.process_char(ch),
            };
            if let Err(err) = res {
                info!("Found invalid string: {s}: {err}");
                return (None, vec![])
            }
        }
        if self.token_pend && self.token.ends_with(']') {
            // we ended with a token, flush it
            self.flush_token();
        }
        (Some(self.result), self.dependencies)
    }

    /// Process a new `[` character, which opens a token (or col_token).
    fn process_open(&mut self, ch: char) -> Result<(), &'static str> {
        if self.in_token {
            return Err("`[` found inside another `[`");
        }

        if self.token_pend {
            if !self.had_dot {
                // case: `<token>[`, flush token and start a new one
                self.flush_token();
            }
            self.had_dot = false;
        }
        self.push_to_cur_token(ch);
        self.in_token = true;
        Ok(())
    }

    /// Process a new `]` character, which ends of a token (or col_token).
    fn process_close(&mut self, ch: char) -> Result<(), &'static str> {
        if !self.in_token {
            return Err("un-escaped `]` found");
        }
        self.push_to_cur_token(ch);
        if self.token_pend {
            // we already have a datasource (stored in token), so resolve (token, col_token)
            self.flush_token_and_col(); // note: this also resets token_pend.
        } else {
            // token might contain either a datasource (if followed by `.[...]`) or
            // a column, so indicate that there is a pending token.
            self.token_pend = true;
        }
        self.in_token = false;
        Ok(())
    }

    /// Process the `.` character, indicating the separation of a datasource and column.
    fn process_separator(&mut self, ch: char) -> Result<(), &'static str> {
        if self.in_token {
            self.push_to_cur_token(ch);
            return Ok(());
        }
        if self.token_pend && self.had_dot {
            // case: `<token>..` flush the token and both dots.
            self.flush_token();
            self.result.push_str("..");
            self.had_dot = false;
        } else if self.token_pend { // && !had_dot
            // case: `<token>.` we are now expecting the col_token to fill up.
            self.had_dot = true;
        } else { // !token_pend
            self.result.push(ch);
        }
        Ok(())
    }

    /// Process any non-special character, adding to either the current token or
    /// result string.
    fn process_char(&mut self, ch: char) -> Result<(), &'static str> {
        if self.in_token {
            self.push_to_cur_token(ch);
            return Ok(());
        }
        if self.token_pend {
            // we have: <token><ch>, so flush token
            self.flush_token();
            if self.had_dot {
                // case: <token>.<ch>, need to make sure the `.` is added
                self.result.push('.');
                self.had_dot = false;
            }
        }
        self.result.push(ch);
        Ok(())
    }

    /// Adds the character to either the col_token (if we already have a pending token)
    /// or token.
    fn push_to_cur_token(&mut self, ch: char) {
        if self.token_pend {
            self.col_token.push(ch);
        } else {
            self.token.push(ch);
        }
    }

    /// Uses finder to resolve a caption from (token, col_token) as a (datasource, column) pair.
    /// Flushes the resolved caption to result or just adds {token}.{col_token} if a caption
    /// wasn't found.
    ///
    /// The (token, col_token) pair is flushed to dependencies.
    ///
    /// Lastly, the current token and col_token fields are reset to empty strings.
    fn flush_token_and_col(&mut self) {
        let token = self.reset_token();
        let col_token = self.reset_col_token();
        if let Some(var) = self.finder.find_column_for_source(&token, &col_token) {
            self.result.push_str(var.as_ref());
        } else {
            self.result.push_str(&format!("{token}.{col_token}"));
        }
        self.dependencies.push((token, col_token));
    }

    /// Uses finder to resolve a caption from token as a column. Flushes the resolved caption
    /// to result or just adds token if a caption wasn't found.
    ///
    /// The token is also flushed to dependencies with a blank datasource.
    ///
    /// Lastly, the current token is reset to an empty string.
    fn flush_token(&mut self) {
        let token = self.reset_token();
        let col= self.finder.find_column(&token)
            .unwrap_or(Cow::from(&token));
        self.result.push_str(col.as_ref());
        self.dependencies.push(("".to_string(), token));
    }

    /// Resets the token and token_pend fields, returning the old value for token.
    fn reset_token(&mut self) -> String {
        self.token_pend = false;
        mem::take(&mut self.token)
    }

    /// Resets the col_token field with an empty string, returning the old value
    fn reset_col_token(&mut self) -> String {
        mem::take(&mut self.col_token)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use super::*;


    impl ColumnFinder for HashMap<(&str, &str), &str> {
        fn find_column(&self, name: &str) -> Option<Cow<str>> {
            self.iter()
                .filter_map(|((_, col), sub)| (*col == name).then_some(*sub))
                .map(Cow::from)
                .next()
        }

        fn find_column_for_source(&self, table: &str, name: &str) -> Option<Cow<str>> {
            self.get(&(table, name)).copied().map(Cow::from)
        }
    }

    #[test]
    fn test_substituter() {
        let m = HashMap::from([
            (("[t1]", "[col1]"), "[val1]"),
            (("[t1]", "[col2]"), "[val2]"),
            (("[t2]", "[col3]"), "[val3]"),
            (("[t3]", "[col3]"), "[val4]"),
            (("[t4.csv]", "[col.1]"), "[val.3]"),
        ]);

        // let sub = Substituter {
        //     finder: &m,
        // };

        let cases = [
            ("OP([col1])", "OP([val1])", vec![("", "[col1]")]),
            ("Call([t1].[col2]) + 4", "Call([val2]) + 4", vec![("[t1]", "[col2]")]),
            ("Call([t1].[col2]) + [t3].[col3][t1].[col1]", "Call([val2]) + [val4][val1]", vec![("[t1]", "[col2]"), ("[t3]", "[col3]"), ("[t1]", "[col1]")]),
            ("SUB([col2],[col1]) + 3.55 - [t4.csv].[col.1]", "SUB([val2],[val1]) + 3.55 - [val.3]", vec![("", "[col2]"), ("", "[col1]"), ("[t4.csv]", "[col.1]")]),
            ("[col1]", "[val1]", vec![("", "[col1]")]),
            ("[t1].[col2]", "[val2]", vec![("[t1]", "[col2]")]),
            ("[col1].non-var", "[val1].non-var", vec![("", "[col1]")]),
            ("[col1]..[col2]", "[val1]..[val2]", vec![("", "[col1]"), ("", "[col2]")])
        ];

        for (pre, post, expected_deps) in cases {
            // let (res, deps) = sub.substitute_columns(pre);
            let (res, deps) = substitute_columns(&m, pre);
            let res = res.unwrap();
            assert_eq!(post, res.as_str());
            assert_eq!(expected_deps.len(), deps.len());
            for (i, (ds, col)) in deps.iter().enumerate() {
                assert_eq!(expected_deps[i].0, ds);
                assert_eq!(expected_deps[i].1, col);
            }
        }

    }
}
