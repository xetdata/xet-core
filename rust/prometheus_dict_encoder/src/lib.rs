use prometheus::proto;
use prometheus::proto::{MetricFamily, MetricType};
use prometheus::Result;
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{self, Write};
use tracing::info;

#[derive(Default, Debug)]
pub struct DictEncoder;

// source adapted by inspection of
// https://github.com/tikv/rust-prometheus/blob/0a63d514608030e7fba7d122042366a5d62dd676/src/encoder/text.rs

/// Implementation verbatim
trait WriteUtf8 {
    fn write_all(&mut self, text: &str) -> io::Result<()>;
}

/// Implementation verbatim
impl<W: Write> WriteUtf8 for W {
    fn write_all(&mut self, text: &str) -> io::Result<()> {
        Write::write_all(self, text.as_bytes())
    }
}

/// Coherence forbids to impl `WriteUtf8` directly on `String`, need this
/// wrapper as a work-around.
struct StringBuf<'a>(&'a mut String);

/// Implementation verbatim
impl WriteUtf8 for StringBuf<'_> {
    fn write_all(&mut self, text: &str) -> io::Result<()> {
        self.0.push_str(text);
        Ok(())
    }
}

/// Implementation from
/// https://github.com/tikv/rust-prometheus/blob/0a63d514608030e7fba7d122042366a5d62dd676/src/encoder/text.rs
fn find_first_occurence(v: &str, include_double_quote: bool) -> Option<usize> {
    if include_double_quote {
        memchr::memchr3(b'\\', b'\n', b'\"', v.as_bytes())
    } else {
        memchr::memchr2(b'\\', b'\n', v.as_bytes())
    }
}

/// `escape_string` replaces `\` by `\\`, new line character by `\n`, and `"` by `\"` if
/// `include_double_quote` is true.
///
/// Implementation from
/// https://github.com/tikv/rust-prometheus/blob/0a63d514608030e7fba7d122042366a5d62dd676/src/encoder/text.rs
///
/// Which is adapted from
/// https://lise-henry.github.io/articles/optimising_strings.html
fn escape_string(v: &str, include_double_quote: bool) -> Cow<'_, str> {
    let first_occurence = find_first_occurence(v, include_double_quote);

    if let Some(first) = first_occurence {
        let mut escaped = String::with_capacity(v.len() * 2);
        escaped.push_str(&v[0..first]);
        let remainder = v[first..].chars();

        for c in remainder {
            match c {
                '\\' | '\n' => {
                    escaped.extend(c.escape_default());
                }
                '"' if include_double_quote => {
                    escaped.extend(c.escape_default());
                }
                _ => {
                    escaped.push(c);
                }
            }
        }

        escaped.shrink_to_fit();
        escaped.into()
    } else {
        // The input string does not contain any characters that would need to
        // be escaped. Return it as it is.
        v.into()
    }
}
/// `label_pairs_to_text` converts a slice of `LabelPair` proto messages plus
/// the explicitly given additional label pair into text formatted as required
/// by the text format and writes it to `writer`. An empty slice in combination
/// with an empty string `additional_label_name` results in nothing being
/// written. Otherwise, the label pairs are written, escaped as required by the
/// text format, The function returns the number of
/// bytes written and any error encountered.
///
/// Implementation verbatim from
/// https://github.com/tikv/rust-prometheus/blob/0a63d514608030e7fba7d122042366a5d62dd676/src/encoder/text.rs
///
/// This produces produces the '{...}' for instance in the text metric:
/// ```ignore
/// xetea_api_request_count{api="/",status="200",type="web"} 65
/// ```
///
fn label_pairs_to_text(
    pairs: &[proto::LabelPair],
    additional_label: Option<(&str, &str)>,
    writer: &mut dyn WriteUtf8,
) -> Result<()> {
    if pairs.is_empty() && additional_label.is_none() {
        return Ok(());
    }

    let mut separator = "{";
    for lp in pairs {
        writer.write_all(separator)?;
        writer.write_all(lp.get_name())?;
        writer.write_all("=\"")?;
        writer.write_all(&escape_string(lp.get_value(), true))?;
        writer.write_all("\"")?;

        separator = ",";
    }

    if let Some((name, value)) = additional_label {
        writer.write_all(separator)?;
        writer.write_all(name)?;
        writer.write_all("=\"")?;
        writer.write_all(&escape_string(value, true))?;
        writer.write_all("\"")?;
    }

    writer.write_all("}")?;

    Ok(())
}

fn make_key_name(
    name: &str,
    name_postfix: Option<&str>,
    mc: &proto::Metric,
    additional_label: Option<(&str, &str)>,
) -> Result<String> {
    let mut ret = String::new();
    let mut writer = StringBuf(&mut ret);
    writer.write_all(name)?;
    if let Some(postfix) = name_postfix {
        writer.write_all(postfix)?;
    }

    label_pairs_to_text(mc.get_label(), additional_label, &mut writer)?;

    Ok(ret)
}

impl DictEncoder {
    /// Create a new text encoder.
    pub fn new() -> DictEncoder {
        DictEncoder
    }
    pub fn encode(&self, metric_families: &[MetricFamily]) -> Result<HashMap<String, f64>> {
        let mut ret: HashMap<String, f64> = HashMap::new();
        for mf in metric_families {
            if mf.get_metric().is_empty() || mf.get_name().is_empty() {
                continue;
            }
            let name = mf.get_name();
            let metric_type = mf.get_field_type();
            for m in mf.get_metric() {
                match metric_type {
                    MetricType::COUNTER => {
                        let keyname = make_key_name(name, None, m, None)?;
                        ret.insert(keyname, m.get_counter().get_value());
                    }
                    MetricType::GAUGE => {
                        let keyname = make_key_name(name, None, m, None)?;
                        ret.insert(keyname, m.get_gauge().get_value());
                    }
                    MetricType::HISTOGRAM => {
                        let h = m.get_histogram();

                        for b in h.get_bucket() {
                            let upper_bound = b.get_upper_bound();
                            let keyname = make_key_name(
                                name,
                                Some("_bucket"),
                                m,
                                Some(("le", &upper_bound.to_string())),
                            )?;
                            ret.insert(keyname, b.get_cumulative_count() as f64);
                        }
                    }
                    MetricType::SUMMARY => {
                        info!("Unsupported summary type metric");
                        continue;
                    }
                    MetricType::UNTYPED => {
                        unimplemented!();
                    }
                }
            }
        }
        Ok(ret)
    }
}
