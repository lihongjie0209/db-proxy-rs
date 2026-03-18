//! db-proxy-rs — transparent SQL proxy that logs wire-protocol traffic.
//!
//! # Usage
//!
//! ```text
//! # Console only (default, MySQL mode)
//! db-proxy-rs
//!
//! # JSONL file
//! db-proxy-rs --output jsonl=/tmp/queries.jsonl
//!
//! # Redis list
//! db-proxy-rs --output redis=127.0.0.1:6379
//! db-proxy-rs --output redis=127.0.0.1:6379/my-list
//!
//! # Kafka topic
//! db-proxy-rs --output kafka=127.0.0.1:9092/mysql-events
//!
//! # Multiple outputs
//! db-proxy-rs --output console --output jsonl=/tmp/queries.jsonl
//! db-proxy-rs --output redis=127.0.0.1:6379 --output kafka=127.0.0.1:9092
//!
//! # PostgreSQL mode
//! db-proxy-rs --protocol postgres --listen 0.0.0.0:5433 --upstream 127.0.0.1:5432
//! ```
//!
//! Set `RUST_LOG=debug` for verbose output (auth exchange, ping, etc.).

mod analyzer;
mod event;
mod mysql_analyzer;
mod packet;
mod postgres_analyzer;
mod protocol;
mod proxy;
mod sink;

use std::{path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{Result, bail};
use clap::Parser;
use tracing_subscriber::{EnvFilter, fmt};

use event::EventSink;
use protocol::Protocol;
use sink::{JsonlConsumer, KafkaSink, MultiSink, RedisSink, TracingConsumer};

// ─────────────────────────────────────────────────────────────────────────────
// Output specification

/// A single output sink, parsed from a CLI string.
///
/// Supported formats:
/// - `console`                    → structured tracing log to stderr
/// - `jsonl=<path>`               → JSON Lines appended to `<path>`
/// - `redis=<[user:pass@]host:port>[/<key>]` → RPUSH to a Redis list (default key: `mysql-proxy-events`)
/// - `kafka=<host:port>[/<topic>]`→ produce to a Kafka topic (default topic: `mysql-proxy-events`)
#[derive(Debug, Clone)]
enum OutputSpec {
    Console,
    Jsonl(PathBuf),
    Redis { addr: String, key: String },
    Kafka { broker: String, topic: String },
}

const DEFAULT_REDIS_KEY: &str = "mysql-proxy-events";
const DEFAULT_KAFKA_TOPIC: &str = "mysql-proxy-events";

impl FromStr for OutputSpec {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "console" {
            return Ok(Self::Console);
        }
        if let Some(path) = s.strip_prefix("jsonl=") {
            return Ok(Self::Jsonl(PathBuf::from(path)));
        }
        if let Some(rest) = s.strip_prefix("redis=") {
            let (addr, key) = split_addr_key(rest, DEFAULT_REDIS_KEY);
            return Ok(Self::Redis { addr, key });
        }
        if let Some(rest) = s.strip_prefix("kafka=") {
            let (broker, topic) = split_addr_key(rest, DEFAULT_KAFKA_TOPIC);
            return Ok(Self::Kafka { broker, topic });
        }
        Err(format!(
            "unknown output format '{s}'. \
             Valid: `console`, `jsonl=<path>`, `redis=<host:port>[/<key>]`, \
             `kafka=<host:port>[/<topic>]`"
        ))
    }
}

/// Split `host:port/name` into `("host:port", "name")`, falling back to
/// `default_name` when no `/` is present.
fn split_addr_key(s: &str, default_name: &str) -> (String, String) {
    // Find the LAST '/' to avoid splitting IPv6 brackets or protocol slashes.
    match s.rfind('/') {
        Some(pos) => (s[..pos].to_string(), s[pos + 1..].to_string()),
        None => (s.to_string(), default_name.to_string()),
    }
}

impl OutputSpec {
    async fn into_sink(self) -> Result<Arc<dyn EventSink>> {
        match self {
            Self::Console => Ok(Arc::new(TracingConsumer)),
            Self::Jsonl(path) => {
                let consumer = JsonlConsumer::open(&path)
                    .map_err(|e| anyhow::anyhow!("cannot open '{}': {e}", path.display()))?;
                Ok(Arc::new(consumer))
            }
            Self::Redis { addr, key } => {
                let sink = RedisSink::new(&addr, &key).await?;
                Ok(Arc::new(sink))
            }
            Self::Kafka { broker, topic } => {
                let sink = KafkaSink::new(&broker, &topic).await?;
                Ok(Arc::new(sink))
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CLI

#[derive(Parser)]
#[command(
    name = "db-proxy-rs",
    version,
    about = "Transparent SQL proxy — tee(1) for MySQL and PostgreSQL traffic",
    after_help = "\
OUTPUT FORMATS
  console                   Structured log to stderr (respects RUST_LOG)
  jsonl=<path>              JSON Lines appended to <path>, one event per line
  redis=<[user:pass@]host:port>[/<key>] RPUSH JSON to a Redis list  (default key: mysql-proxy-events)
  kafka=<host:port>[/<topic>] Produce JSON to a Kafka topic (default: mysql-proxy-events)

EXAMPLES
  db-proxy-rs
  db-proxy-rs --output jsonl=/var/log/mysql-proxy.jsonl
  db-proxy-rs --output console --output jsonl=/tmp/queries.jsonl
  db-proxy-rs --protocol postgres --listen 0.0.0.0:5433 --upstream 127.0.0.1:5432
  db-proxy-rs --output redis=127.0.0.1:6379
  db-proxy-rs --output redis=:mypassword@127.0.0.1:6379/my-list --output console
  db-proxy-rs --output kafka=127.0.0.1:9092/mysql-events"
)]
struct Args {
    /// Protocol spoken by the client and upstream server.
    #[arg(long, value_enum, default_value_t = Protocol::Mysql, env = "PROXY_PROTOCOL")]
    protocol: Protocol,

    /// Proxy listen address.
    #[arg(short, long, default_value = "0.0.0.0:3307", env = "PROXY_LISTEN")]
    listen: String,

    /// Upstream MySQL/PostgreSQL server.
    #[arg(short, long, default_value = "127.0.0.1:3306", env = "PROXY_UPSTREAM")]
    upstream: String,

    /// Output sink(s). May be repeated. Defaults to `console` when omitted.
    /// Formats: `console`, `jsonl=<path>`, `redis=<host:port>[/<key>]`, `kafka=<host:port>[/<topic>]`.
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
        sinks.push(spec.into_sink().await?);
    }

    let sink: Arc<dyn EventSink> = if sinks.len() == 1 {
        sinks.remove(0)
    } else {
        Arc::new(MultiSink::new(sinks))
    };

    proxy::run(&args.listen, &args.upstream, args.protocol, sink).await
}
