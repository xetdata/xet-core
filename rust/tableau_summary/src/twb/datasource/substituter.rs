use std::borrow::Cow;
use tracing::info;

pub struct Substituter<'a, T: ColumnFinder> {
    pub finder: &'a T,
}

pub trait ColumnFinder {
    /// Given the name of a column, find the column's display name.
    /// If there is no such column, returns None.
    fn find_column(&self, name: &str) -> Option<Cow<str>>;
    /// Given the logical source name and the column name, get the display name for the
    /// column.
    fn find_column_for_source(&self, source: &str, name: &str) -> Option<Cow<str>>;
}

impl<'a, T: ColumnFinder> Substituter<'a, T> {
    /// Given the parameterized string, replace any referenced columns with their Caption'ed representation
    /// e.g. if the column: `[Calc_12345]` has the caption: `'Orders made'`, then given the string:
    /// `"CEIL([Calc_12345])"` we will output: `"CEIL([Orders made])"`.
    /// This will also return a list of dependencies found in the string: at tuple of (datasource, column)
    pub fn substitute_columns(&self, s: &str) -> (Option<String>, Vec<(String, String)>) {
        let mut result = String::new();
        let mut token = String::new();
        let mut col_token = String::new();
        let mut is_temp = false;
        let mut token_pend = false;
        let mut had_dot = false;
        let mut dependencies: Vec<(String, String)> = vec![];

        for ch in s.chars() {
            match ch {
                '[' => {
                    if is_temp {
                        info!("found string: {s} with `[` inside of another `[`");
                        return (None, vec![]);
                    }
                    if token_pend && !had_dot {
                        // we have a `<token>[...`, flush the token and start a new one
                        let col= self.finder.find_column(&token)
                            .unwrap_or(Cow::from(&token));
                        result.push_str(col.as_ref());
                        dependencies.push(("".to_string(), token));
                        token = String::new();
                        token_pend = false;
                        token.push(ch);
                    } else if token_pend { // && had_dot
                        // we are now parsing a column for the parsed table token
                        had_dot = false;
                        col_token.push(ch);
                    } else {
                        // new token
                        token.push(ch);
                    }
                    is_temp = true;
                },
                ']' => {
                    if !is_temp {
                        info!("found string: {s} with unescaped `]`");
                        return (None, vec![]);
                    }
                    if token_pend {
                        // we already have a datasource, so we are now closing the column
                        col_token.push(ch);
                        if let Some(var) = self.finder.find_column_for_source(&token, &col_token) {
                            result.push_str(var.as_ref());
                        } else {
                            result.push_str(&format!("{token}.{col_token}"));
                        }
                        dependencies.push((token, col_token));
                        token = String::new();
                        col_token = String::new();
                        token_pend = false;
                    } else {
                        token.push(ch);
                        token_pend = true;

                    }
                    is_temp = false;
                },
                '.' => {
                    if is_temp {
                        // inside of either token or col_token
                        if token_pend {
                            col_token.push(ch);
                        } else {
                            token.push(ch);
                        }
                    } else if token_pend && had_dot {
                        // we have `<token>..` flush the token and both dots.
                        let col= self.finder.find_column(&token)
                            .unwrap_or(Cow::from(&token));
                        result.push_str(col.as_ref());
                        dependencies.push(("".to_string(), token));
                        token = String::new();
                        token_pend = false;
                        result.push_str("..");
                    } else if token_pend { // && !had_dot
                        // we have `<token>.` we are now expecting the col_token to fill up.
                        had_dot = true;
                    } else { // !token_pend
                        result.push(ch);
                    }
                },
                _ => {
                    if is_temp {
                        // inside of either token or col_token
                        if token_pend {
                            col_token.push(ch);
                        } else {
                            token.push(ch);
                        }
                    } else if token_pend {
                        // we have: <token><ch>, so flush token
                        let col= self.finder.find_column(&token)
                            .unwrap_or(Cow::from(&token));
                        result.push_str(col.as_ref());
                        dependencies.push(("".to_string(), token));
                        token = String::new();
                        token_pend = false;
                        result.push(ch);
                    } else {
                        result.push(ch);
                    }
                }
            }
        }
        if token_pend && token.ends_with(']') {
            // we ended with a token, flush it and start a new one
            let col = self.finder.find_column(&token)
                .unwrap_or(Cow::from(&token));
            result.push_str(col.as_ref());
            dependencies.push(("".to_string(), token));
        }
        (Some(result), dependencies)
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

        let sub = Substituter {
            finder: &m,
        };

        let cases = [
            ("OP([col1])", "OP([val1])", vec![("", "[col1]")]),
            ("Call([t1].[col2]) + 4", "Call([val2]) + 4", vec![("[t1]", "[col2]")]),
            ("Call([t1].[col2]) + [t3].[col3][t1].[col1]", "Call([val2]) + [val4][val1]", vec![("[t1]", "[col2]"), ("[t3]", "[col3]"), ("[t1]", "[col1]")]),
            ("SUB([col2],[col1]) + 3.55 - [t4.csv].[col.1]", "SUB([val2],[val1]) + 3.55 - [val.3]", vec![("", "[col2]"), ("", "[col1]"), ("[t4.csv]", "[col.1]")]),
            ("[col1]", "[val1]", vec![("", "[col1]")]),
        ];

        for (pre, post, expected_deps) in cases {
            let (res, deps) = sub.substitute_columns(pre);
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
