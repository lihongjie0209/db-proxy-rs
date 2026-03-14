//! mysql-proxy-rs — transparent MySQL proxy that logs SQL traffic.
//!
//! # Usage
//!
//! ```text
//! # Console only (default)
//! mysql-proxy-rs
//!
//! # JSONL file only
//! mysql-proxy-rs --output jsonl=/tmp/queries.jsonl
//!
//! # Both console and file
//! mysql-proxy-rs --output console --output jsonl=/tmp/queries.jsonl
//! ```
//!
//! Set `RUST_LOG=debug` for verbose output (auth exchange, ping, etc.).

mod analyzer;
mod event;
mod packet;
mod proxy;
mod sink;

use std::{path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{bail, Result};
use clap::Parser;
use tracing_subscriber::{fmt, EnvFilter};

use event::EventSink;
use sink::{JsonlConsumer, MultiSink, TracingConsumer};

// ─────────────────────────────────────────────────────────────────────────────
// Output specification

/// A single output sink, parsed from a CLI string.
///
/// Supported formats:
/// - `console`           → structured tracing log to stderr
/// - `jsonl=<path>`      → JSON Lines appended to `<path>`
#[derive(Debug, Clone)]
enum OutputSpec {
    Console,
    Jsonl(PathBuf),
}

impl FromStr for OutputSpec {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "console" {
            return Ok(Self::Console);
        }
        if let Some(path) = s.strip_prefix("jsonl=") {
            return Ok(Self::Jsonl(PathBuf::from(path)));
        }
        Err(format!(
            "unknown output format '{s}'. \
             Valid values: `console`, `jsonl=<path>`"
        ))
    }
}

impl OutputSpec {
    fn into_sink(self) -> Result<Arc<dyn EventSink>> {
        match self {
            Self::Console => Ok(Arc::new(TracingConsumer)),
            Self::Jsonl(path) => {
                let consumer = JsonlConsumer::open(&path)
                    .map_err(|e| anyhow::anyhow!("cannot open '{}': {e}", path.display()))?;
                Ok(Arc::new(consumer))
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CLI

#[derive(Parser)]
#[command(
    name    = "mysql-proxy-rs",
    version,
    about   = "Transparent MySQL proxy — tee(1) for MySQL traffic",
    after_help = "\
OUTPUT FORMATS
  console          Structured log to stderr (respects RUST_LOG)
  jsonl=<path>     JSON Lines appended to <path>, one event per line

EXAMPLES
  mysql-proxy-rs
  mysql-proxy-rs --output jsonl=/var/log/mysql-proxy.jsonl
  mysql-proxy-rs --output console --output jsonl=/tmp/queries.jsonl"
)]
struct Args {
    /// Proxy listen address.
    #[arg(short, long, default_value = "0.0.0.0:3307", env = "PROXY_LISTEN")]
    listen: String,

    /// Upstream MySQL server.
    #[arg(short, long, default_value = "127.0.0.1:3306", env = "PROXY_UPSTREAM")]
    upstream: String,

    /// Output sink(s). May be repeated. Defaults to `console` when omitted.
    /// Formats: `console`, `jsonl=<path>`.
    #[arg(short, long = "output", value_name = "SINK")]
    outputs: Vec<OutputSpec>,
}

// ─────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // Default to console when no --output flags are provided.
    let specs: Vec<OutputSpec> = if args.outputs.is_empty() {
        vec![OutputSpec::Console]
    } else {
        args.outputs
    };

    if specs.is_empty() {
        bail!("at least one --output must be specified");
    }

    // Build individual sinks, then combine.
    let mut sinks: Vec<Arc<dyn EventSink>> = Vec::with_capacity(specs.len());
    for spec in specs {
        sinks.push(spec.into_sink()?);
    }

    let sink: Arc<dyn EventSink> = if sinks.len() == 1 {
        sinks.remove(0)
    } else {
        Arc::new(MultiSink::new(sinks))
    };

    proxy::run(&args.listen, &args.upstream, sink).await
}


