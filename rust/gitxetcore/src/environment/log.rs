use std::fmt::Debug;
use std::fs::OpenOptions;
use std::{env, io, process};

use crate::command::Command;
use crate::config::LogFormat;
use crate::config::XetConfig;
use crate::errors::GitXetRepoError::InvalidLogPath;
use cas::constants::TRACE_ID_HEADER;
use cas_client::set_trace_forwarding;
use opentelemetry::propagation::text_map_propagator::FieldIter;
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::sdk::trace;
use opentelemetry::sdk::trace::XrayIdGenerator;
use opentelemetry::Context;
use tracing::{debug, info_span, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

fn log_exception_info(source: &str) {
    tracing::info!(
        "Error reported: {source} error; context={:?}",
        std::backtrace::Backtrace::force_capture()
    );
}

fn log_exception_error(source: &str) {
    tracing::error!(
        "Error reported: {source} error; context={:?}",
        std::backtrace::Backtrace::force_capture()
    );
}

pub fn initialize_tracing_subscriber(config: &XetConfig) -> Result<(), anyhow::Error> {
    // Logging format
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_file(true)
        .with_target(false);

    // Level filter
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(config.log.level.as_str()))?;

    let mut otel_layer = None;
    if config.log.with_tracer {
        // Tracing exporter + format
        let jaeger_tracer = opentelemetry_jaeger::new_pipeline()
            .with_max_packet_size(9216)
            .with_service_name("git-xet")
            .with_trace_config(
                trace::config()
                    .with_max_events_per_span(1)
                    .with_id_generator(XrayIdGenerator::default()),
            )
            .with_auto_split_batch(true)
            .install_batch(opentelemetry::runtime::Tokio)?;
        otel_layer = Some(tracing_opentelemetry::layer().with_tracer(jaeger_tracer));
        set_trace_forwarding(true);
    }

    // Set the global tracing subscriber.
    let trace_builder = tracing_subscriber::registry()
        .with(filter_layer)
        .with(otel_layer);

    // Due to the type system of Rust, building the fmt_layer based off our config is ugly.
    // We cannot process this into branching stages like:  "if Json, then layer.json(), else layer.compact()"
    // because those return different types. The same is true when using `layer.with_writer(T)`
    // as the returned object is dependent upon type T and thus, cannot be stored in the
    // same variable... Thus we have to do this combinatorial explosion of choices to build the
    // logger. Luckily it is small (2x2), but if we have more fields to configure or more choices,
    // we will need to revisit and think of a better way to solve this.
    match config.log.path.as_ref() {
        Some(path) => {
            let file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(path)
                .map_err(|e| InvalidLogPath(path.clone(), e))?;
            match config.log.format {
                LogFormat::Compact => trace_builder
                    .with(fmt_layer.compact().with_writer(file))
                    .init(),
                LogFormat::Json => trace_builder
                    .with(fmt_layer.json().with_writer(file))
                    .init(),
            }
        }
        None => match config.log.format {
            LogFormat::Compact => trace_builder
                .with(fmt_layer.compact().with_writer(io::stderr))
                .init(),
            LogFormat::Json => trace_builder
                .with(fmt_layer.json().with_writer(io::stderr))
                .init(),
        },
    }

    // Logging the exceptions is really messy, but really useful.   We log it as an error in telemetry applications,
    // so it gets recorded, but at the info level everywhere else to enable useful debugging but also
    if config.log.with_tracer {
        xet_error::enable_exception_logging(log_exception_error);
    } else {
        xet_error::enable_exception_logging(log_exception_info);
    }

    Ok(())
}

pub fn get_trace_span(command: &Command) -> Span {
    let command_name = command.name();
    let span = info_span!("gitxet", "command" = command_name);
    let propagator = EnvTracePropagator::default();
    propagator.add_env_context_to_span(&span);
    span
}

/// Allows propagating trace_ids across process boundaries via environment variables.
///
/// The caller can call [export_context_to_subcommand] to add the trace_id to the indicated
/// child process. The child process then calls [add_env_context_to_span] to extract the
/// trace_id context from the process env (if it exists).
///
/// Note: They were being used in [checkout] to propagate the trace across the sub-call to
/// git. However, that was removed since it caused issues if the checkout was too large
/// (too many segments in the trace).
/// We keep this in case we find other subprocess calls to propagate traces.
#[derive(Debug, Default)]
pub struct EnvTracePropagator {
    jaeger_prop: opentelemetry_jaeger::Propagator,
}

impl EnvTracePropagator {
    pub fn export_context_to_subcommand(&self, cmd: process::Command) -> process::Command {
        let cur_span = Span::current();
        let ctx = cur_span.context();
        let mut injector = CommandInjector::new(cmd);
        self.inject_context(&ctx, &mut injector);
        injector.take_cmd()
    }

    pub fn add_env_context_to_span(&self, span: &Span) {
        let extractor = EnvTraceExtractor::new();
        if extractor.get(JAEGER_HEADER).is_some() {
            debug!("Found trace_id in env");
            let context = self.extract(&extractor);
            span.set_parent(context);
        } else {
            let envs = env::vars()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<String>>();
            debug!("No trace_id found in ENV: {:?}", envs);
        }
    }
}

impl TextMapPropagator for EnvTracePropagator {
    fn inject_context(&self, cx: &Context, injector: &mut dyn Injector) {
        self.jaeger_prop.inject_context(cx, injector)
    }

    fn extract_with_context(&self, cx: &Context, extractor: &dyn Extractor) -> Context {
        self.jaeger_prop.extract_with_context(cx, extractor)
    }

    fn fields(&self) -> FieldIter<'_> {
        self.jaeger_prop.fields()
    }
}

const JAEGER_HEADER: &str = TRACE_ID_HEADER;
const TRACE_ENV: &str = "XET_TRACE_ID";

/// Injects a trace-id into a [process::Command]'s env variables.
struct CommandInjector {
    cmd: process::Command,
}
impl CommandInjector {
    fn new(cmd: process::Command) -> Self {
        Self { cmd }
    }

    fn take_cmd(self) -> process::Command {
        self.cmd
    }
}

impl Injector for CommandInjector {
    fn set(&mut self, key: &str, value: String) {
        if key == JAEGER_HEADER {
            self.cmd.env(TRACE_ENV, value);
        }
    }
}

/// Extracts an env-propagated trace field.
///
/// Note: since the otel Extractor trait works on &str, we can't have this be
/// generic as env::* will clone the values, thus, causing borrow checker problems.
struct EnvTraceExtractor {
    trace_header: Option<String>,
}

impl EnvTraceExtractor {
    fn new() -> Self {
        Self {
            trace_header: env::var(TRACE_ENV).ok(),
        }
    }
}

impl Extractor for EnvTraceExtractor {
    fn get(&self, key: &str) -> Option<&str> {
        if key == JAEGER_HEADER {
            self.trace_header.as_deref()
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        self.trace_header.iter().map(String::as_str).collect()
    }
}
