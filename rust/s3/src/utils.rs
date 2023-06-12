use crate::errors::{Result, XetS3Error};
use std::ops::Range;
use tracing::debug;
use url::Url;

/// Creates a header clause for the byte range as specified in:
/// https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
///
/// Note that we accept the range with the end exclusive, but the range header
/// requires it to be inclusive.
pub fn to_range_header(range: Range<u64>) -> String {
    format!("bytes={}-{}", range.start, range.end - 1)
}

/// Parses an s3 URL into the bucket and component parts.
///
pub fn parse_s3_url(url: &str) -> Result<(String, String)> {
    let s3_url = Url::parse(url).map_err(|e| {
        XetS3Error::Error(format!(
            "Error parsing URL {url:?} : {e:?}. \nS3 Url must be of the form s3://<bucket>/<prefix>."
        ))
    })?;

    if s3_url.scheme() != "s3" {
        return Err(XetS3Error::Error(format!(
            "URL {url:?} does not have s3 prefix.  S3 Url must be of the form s3://<bucket>/<prefix>."
        )));
    }

    if s3_url.host_str().is_none() {
        return Err(XetS3Error::Error(format!(
            "Error extracting bucket from s3 URL {url:?}.  S3 Url must be of the form s3://<bucket>/<prefix>."
        )));
    }

    let bucket = s3_url.host_str().unwrap();

    let mut prefix = s3_url.path();
    if let Some(stripped_prefix) = prefix.strip_prefix('/') {
        prefix = stripped_prefix;
    }

    debug!(
        "Parsed s3 url {} to bucket = {}, prefix = {}",
        url, bucket, prefix
    );

    Ok((bucket.to_owned(), prefix.to_owned()))
}
