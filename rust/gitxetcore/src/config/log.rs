use crate::config;
use crate::config::util::get_sanitized_invocation_command;
use crate::config::ConfigError;
use crate::config::ConfigError::{LogPathNotFile, LogPathReadOnly};
use atty::Stream;
use chrono::Utc;
use std::path::{Path, PathBuf};
use tracing::{warn, Level};
use xet_config::Log;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogFormat {
    Compact,
    Json,
}

impl From<Option<&str>> for LogFormat {
    fn from(fmt: Option<&str>) -> Self {
        fmt.map(|s| match s.to_lowercase().as_str() {
            "json" => LogFormat::Json,
            "compact" | "" => LogFormat::Compact,
            _ => {
                warn!(
                    "log.format: {} is not supported, defaulting to `compact`",
                    s
                );
                LogFormat::Compact
            }
        })
        .unwrap_or(if atty::is(Stream::Stderr) {
            LogFormat::Compact
        } else {
            LogFormat::Json
        })
    }
}

#[derive(Debug, Clone)]
pub struct LogSettings {
    pub level: Level,
    pub path: Option<PathBuf>,
    pub format: LogFormat,
    pub with_tracer: bool,
    pub silent_summary: bool,
    pub exceptions: bool,
}

impl Default for LogSettings {
    fn default() -> Self {
        Self {
            level: Level::WARN,
            path: None,
            format: LogFormat::Compact,
            with_tracer: false,
            silent_summary: false,
            exceptions: false,
        }
    }
}

impl TryFrom<Option<&Log>> for LogSettings {
    type Error = ConfigError;

    fn try_from(log: Option<&Log>) -> Result<Self, Self::Error> {
        fn validate_path(path: &Path) -> Result<(), ConfigError> {
            if path.exists() {
                if !path.is_file() {
                    return Err(LogPathNotFile(path.to_path_buf()));
                } else if !config::util::can_write(path) {
                    return Err(LogPathReadOnly(path.to_path_buf()));
                }
            } else if let Some(p) = path.parent() {
                if !p.exists() {
                    std::fs::create_dir_all(p)
                        .map_err(|_| ConfigError::LogPathReadOnly(path.to_path_buf()))?;
                }
            }
            Ok(())
        }

        Ok(match log {
            Some(log) => {
                let level = match log.level.as_ref() {
                    Some(level) => parse_level(level),
                    None => Level::WARN,
                };
                let path = match log.path.as_ref() {
                    Some(path) if !config::util::is_empty(path) => {
                        let mut path_s = path.to_str().unwrap_or_default().to_owned();

                        if path_s.contains("{timestamp}") {
                            path_s = path_s
                                .replace("{timestamp}", &Utc::now().to_rfc3339().replace(':', "-"));
                        }

                        if path_s.contains("{pid}") {
                            path_s = path_s.replace("{pid}", &format!("{}", std::process::id()));
                        }

                        let path = PathBuf::from(path_s);

                        // Attempt to convert it to the absolute path in order to ensure the writing location is correct.
                        let path = path.canonicalize().unwrap_or(path);

                        validate_path(&path)?;
                        if std::env::var("XET_PRINT_LOG_FILE_PATH").unwrap_or("0".to_owned()) != "0"
                        {
                            let prog = get_sanitized_invocation_command(true);
                            eprintln!("Xet: ({prog}) Writing logs to file {path:?}",);
                        }
                        Some(path)
                    }
                    _ => None,
                };
                let format = log.format.as_deref().into();
                let with_tracer = log.tracing.unwrap_or(false);
                let silent_summary = log.silentsummary.unwrap_or(false);
                let exceptions = log.exceptions.unwrap_or(false);
                LogSettings {
                    level,
                    path,
                    format,
                    with_tracer,
                    silent_summary,
                    exceptions,
                }
            }
            None => LogSettings::default(),
        })
    }
}

fn parse_level(level: &str) -> Level {
    match level.to_lowercase().as_str() {
        "error" => Level::ERROR,
        "warn" => Level::WARN,
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => Level::WARN,
    }
}

#[cfg(test)]
mod tests {
    use atty::Stream::Stderr;
    use super::*;
    use tempfile::{NamedTempFile, TempDir};
    use tokio_test::assert_err;

    #[test]
    fn test_log_format_parse() {
        let mut x: LogFormat = Some("JSON").into();
        assert_eq!(x, LogFormat::Json);
        x = Some("comPact").into();
        assert_eq!(x, LogFormat::Compact);
        x = Some("").into();
        assert_eq!(x, LogFormat::Compact);
        x = Some("other").into();
        assert_eq!(x, LogFormat::Compact);
        x = None.into();
        if atty::is(Stderr) {
            assert_eq!(x, LogFormat::Compact);
        } else {
            assert_eq!(x, LogFormat::Json);
        }
    }

    #[test]
    fn test_log_level_parse() {
        fn check(s: &str, expected_level: Level) {
            assert_eq!(expected_level, parse_level(s));
        }

        check("trace", Level::TRACE);
        check("TRACE", Level::TRACE);
        check("tRaCe", Level::TRACE);
        check("debug", Level::DEBUG);
        check("DEBUG", Level::DEBUG);
        check("dEbUg", Level::DEBUG);
        check("info", Level::INFO);
        check("INFO", Level::INFO);
        check("iNfO", Level::INFO);
        check("warn", Level::WARN);
        check("WARN", Level::WARN);
        check("wArN", Level::WARN);
        check("error", Level::ERROR);
        check("ERROR", Level::ERROR);
        check("eRRoR", Level::ERROR);

        check("default", Level::WARN);
        check("something", Level::WARN);
        check("", Level::WARN);
    }

    #[test]
    fn test_parse() {
        let tmpfile = NamedTempFile::new().unwrap();
        let log_file_path = tmpfile.path().to_path_buf();
        let log_cfg = Log {
            path: Some(log_file_path.clone()),
            level: Some("info".to_string()),
            format: Some("json".to_string()),
            tracing: None,
            silentsummary: None,
            exceptions: None,
        };

        let log_settings = LogSettings::try_from(Some(&log_cfg)).unwrap();
        assert_eq!(
            log_file_path.canonicalize().unwrap(),
            log_settings.path.unwrap()
        );
        assert_eq!(Level::INFO, log_settings.level);
        assert_eq!(LogFormat::Json, log_settings.format);
        assert!(!log_settings.with_tracer);
    }

    #[test]
    fn test_parse_no_path() {
        let log_cfg = Log {
            path: None,
            format: Some("json".to_string()),
            ..Default::default()
        };

        let log_settings = LogSettings::try_from(Some(&log_cfg)).unwrap();
        assert!(log_settings.path.is_none());
        assert_eq!(Level::WARN, log_settings.level);
        assert_eq!(LogFormat::Json, log_settings.format);
        assert!(!log_settings.with_tracer);
    }

    #[test]
    fn test_parse_none() {
        let log_settings = LogSettings::try_from(None).unwrap();
        assert!(log_settings.path.is_none());
        assert_eq!(Level::WARN, log_settings.level);
        assert_eq!(LogFormat::Compact, log_settings.format);
        assert!(!log_settings.with_tracer);
    }

    #[test]
    fn test_invalid_path() {
        let tmpdir = TempDir::new().unwrap();
        let log_file_path = tmpdir.path().to_path_buf();
        let log_cfg = Log {
            path: Some(log_file_path),
            level: Some("info".to_string()),
            format: Some("json".to_string()),
            tracing: None,
            silentsummary: None,
            exceptions: None,
        };

        assert_err!(LogSettings::try_from(Some(&log_cfg)));
    }
}
