use tracing::warn;

fn retry_http_status_code(stat: &reqwest::StatusCode) -> bool {
    stat.is_server_error() || *stat == reqwest::StatusCode::TOO_MANY_REQUESTS
}

pub fn is_status_retriable_and_print(err: &reqwest::Error) -> bool {
    let ret = if let Some(code) = err.status() {
        retry_http_status_code(&code)
    } else {
        true
    };
    if ret {
        warn!("{}. Retrying...", err);
    }
    ret
}
