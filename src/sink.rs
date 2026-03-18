//! Built-in [`EventSink`] implementations.
//!
//! | Type                | Description                                        |
//! |---------------------|----------------------------------------------------|
//! | [`TracingConsumer`] | Emits structured `tracing` log records             |
//! | [`JsonlConsumer`]   | Appends JSON Lines to a file with timestamp        |
//! | [`RedisSink`]       | RPUSHes JSON lines to a Redis list                 |
//! | [`KafkaSink`]       | Produces JSON records to a Kafka topic             |
//! | [`MultiSink`]       | Fans out one event to multiple sinks               |

use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::mpsc::UnboundedSender;

use crate::event::{ConnInfo, Event, EventSink};
use serde::Serialize;

// ─────────────────────────────────────────────────────────────────────────────
// Shared JSON helper

/// Wire format: timestamp + flattened ConnInfo + flattened Event.
#[derive(Serialize)]
struct LogRecord<'a> {
    ts_ms: u128,
    #[serde(flatten)]
    conn: &'a ConnInfo,
    #[serde(flatten)]
    event: &'a Event,
}

fn ts_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn to_json_line(conn: &ConnInfo, event: &Event) -> Option<String> {
    let record = LogRecord {
        ts_ms: ts_ms(),
        conn,
        event,
    };
    match serde_json::to_string(&record) {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::warn!(error = %e, "failed to serialize event to JSON");
            None
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TracingConsumer

/// Default sink: emits each [`Event`] as a structured `tracing` log record.
///
/// * Handshake / commands → `INFO`
/// * Query results        → `DEBUG`
/// * Errors / auth fails  → `WARN`
pub struct TracingConsumer;

impl EventSink for TracingConsumer {
    fn on_event(&self, conn: &ConnInfo, event: Event) {
        let cid = conn.conn_id;
        let client = &conn.client;
        let user = &conn.user;
        let upstream = &conn.upstream;
        let db = conn.database.as_deref().unwrap_or("-");
        let protocol = conn.protocol.as_str();

        match event {
            Event::ServerHello {
                server_version,
                mysql_conn_id,
                auth_plugin,
            } => {
                tracing::info!(
                    conn_id = cid, protocol, %client, %upstream,
                    server_version, mysql_conn_id, auth_plugin,
                    "server hello"
                );
            }
            Event::ClientHandshake => {
                tracing::info!(
                    conn_id = cid, protocol, %client, %user, database = db,
                    "client handshake"
                );
            }
            Event::Authenticated => {
                tracing::info!(conn_id = cid, protocol, %client, %user, database = db, "authenticated");
            }
            Event::AuthFailed { code, message } => {
                tracing::warn!(conn_id = cid, protocol, %client, %user, code, message, "auth failed");
            }
            Event::Query { sql } => {
                tracing::info!(conn_id = cid, protocol, %client, %user, database = db, sql, "COM_QUERY");
            }
            Event::StmtPrepare {
                stmt_id,
                num_params,
                sql,
            } => {
                tracing::info!(
                    conn_id = cid, protocol, %client, %user, database = db,
                    stmt_id, num_params, sql,
                    "COM_STMT_PREPARE"
                );
            }
            Event::StmtExecute {
                stmt_id,
                sql,
                params,
            } => {
                let params_sql: Vec<String> = params.iter().map(|v| v.as_sql(false)).collect();
                match sql {
                    Some(tpl) => tracing::info!(
                        conn_id = cid, protocol, %client, %user, database = db,
                        stmt_id, sql = tpl, params = ?params_sql,
                        "COM_STMT_EXECUTE"
                    ),
                    None => tracing::info!(
                        conn_id = cid, protocol, %client, %user, database = db,
                        stmt_id, params = ?params_sql,
                        "COM_STMT_EXECUTE"
                    ),
                }
            }
            Event::InitDb { database } => {
                tracing::info!(conn_id = cid, protocol, %client, %user, database, "COM_INIT_DB");
            }
            Event::Quit => {
                tracing::info!(conn_id = cid, protocol, %client, %user, "COM_QUIT");
            }
            Event::QueryOk {
                affected_rows,
                last_insert_id,
                warnings,
            } => {
                tracing::debug!(
                    conn_id = cid, protocol, %client, %user, database = db,
                    affected_rows, last_insert_id = ?last_insert_id, warnings,
                    "query ok"
                );
            }
            Event::QueryError {
                code,
                sql_state,
                message,
            } => {
                tracing::warn!(
                    conn_id = cid, protocol, %client, %user, database = db,
                    code, sql_state = ?sql_state, message,
                    "query error"
                );
            }
            Event::PgStartup {
                protocol_version,
                params,
            } => {
                tracing::info!(
                    conn_id = cid, protocol, %client, %user, database = db,
                    protocol_version, params = ?params,
                    "pg startup"
                );
            }
            Event::PgSslRequest => {
                tracing::info!(conn_id = cid, protocol, %client, "pg ssl request");
            }
            Event::PgCancelRequest {
                process_id,
                secret_key,
            } => {
                tracing::info!(
                    conn_id = cid, protocol, %client, process_id, secret_key,
                    "pg cancel request"
                );
            }
            Event::PgAuthRequest { method } => {
                tracing::info!(conn_id = cid, protocol, %client, %user, method, "pg auth request");
            }
            Event::PgAuthenticated => {
                tracing::info!(conn_id = cid, protocol, %client, %user, database = db, "pg authenticated");
            }
            Event::PgAuthFailed {
                severity,
                code,
                message,
            } => {
                tracing::warn!(
                    conn_id = cid, protocol, %client, %user,
                    severity = ?severity, code = ?code, message,
                    "pg auth failed"
                );
            }
            Event::PgSimpleQuery { sql } => {
                tracing::info!(conn_id = cid, protocol, %client, %user, database = db, sql, "pg query");
            }
            Event::PgParse {
                statement,
                sql,
                param_types,
            } => {
                tracing::info!(
                    conn_id = cid, protocol, %client, %user, database = db,
                    statement, sql, param_types = ?param_types,
                    "pg parse"
                );
            }
            Event::PgBind {
                portal,
                statement,
                param_formats,
                params,
                result_formats,
            } => {
                tracing::info!(
                    conn_id = cid, protocol, %client, %user, database = db,
                    portal, statement, param_formats = ?param_formats, params = ?params,
                    result_formats = ?result_formats,
                    "pg bind"
                );
            }
            Event::PgDescribe { target, name } => {
                tracing::info!(
                    conn_id = cid, protocol, %client, %user, target, name,
                    "pg describe"
                );
            }
            Event::PgExecute {
                portal,
                statement,
                sql,
                params,
                max_rows,
            } => {
                tracing::info!(
                    conn_id = cid, protocol, %client, %user, database = db,
                    portal, statement = ?statement, sql = ?sql, params = ?params, max_rows,
                    "pg execute"
                );
            }
            Event::PgClose { target, name } => {
                tracing::info!(conn_id = cid, protocol, %client, %user, target, name, "pg close");
            }
            Event::PgFlush => {
                tracing::debug!(conn_id = cid, protocol, %client, %user, "pg flush");
            }
            Event::PgSync => {
                tracing::debug!(conn_id = cid, protocol, %client, %user, "pg sync");
            }
            Event::PgTerminate => {
                tracing::info!(conn_id = cid, protocol, %client, %user, "pg terminate");
            }
            Event::PgParseComplete => {
                tracing::debug!(conn_id = cid, protocol, %client, %user, "pg parse complete");
            }
            Event::PgBindComplete => {
                tracing::debug!(conn_id = cid, protocol, %client, %user, "pg bind complete");
            }
            Event::PgCloseComplete => {
                tracing::debug!(conn_id = cid, protocol, %client, %user, "pg close complete");
            }
            Event::PgCommandComplete { tag } => {
                tracing::debug!(conn_id = cid, protocol, %client, %user, database = db, tag, "pg command complete");
            }
            Event::PgReadyForQuery { status } => {
                tracing::debug!(conn_id = cid, protocol, %client, %user, database = db, status, "pg ready");
            }
            Event::PgError {
                severity,
                code,
                message,
            } => {
                tracing::warn!(
                    conn_id = cid, protocol, %client, %user, database = db,
                    severity = ?severity, code = ?code, message,
                    "pg error"
                );
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// JsonlConsumer

/// Appends one JSON object per line to a file (JSON Lines / NDJSON format).
///
/// Each record merges connection metadata with the event payload:
/// ```json
/// {"ts_ms":1710000000000,"conn_id":3,"proxy":"0.0.0.0:3307","upstream":"127.0.0.1:3306",
///  "client":"127.0.0.1:54321","user":"root","database":"testdb",
///  "type":"query","sql":"SELECT 1"}
/// ```
pub struct JsonlConsumer {
    writer: Mutex<BufWriter<File>>,
}

impl JsonlConsumer {
    /// Open `path` for appending (creates the file if it doesn't exist).
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
        })
    }
}

impl EventSink for JsonlConsumer {
    fn on_event(&self, conn: &ConnInfo, event: Event) {
        if let Some(line) = to_json_line(conn, &event) {
            if let Ok(mut w) = self.writer.lock() {
                let _ = writeln!(w, "{line}");
                let _ = w.flush();
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RedisSink

/// Pushes one JSON line per event to a Redis list using `RPUSH`.
///
/// # CLI format
/// ```text
/// redis=127.0.0.1:6379
/// redis=127.0.0.1:6379/my-list-key
/// redis=:password@127.0.0.1:6379/my-list-key
/// ```
///
/// The `addr` argument may be `[user:password@]host:port` — it is prefixed
/// with `redis://` to form the full connection URL.
///
/// Events are serialised in the same [`LogRecord`] format as [`JsonlConsumer`].
/// Network I/O happens on a background task; the proxy forwarding path is never
/// blocked.
pub struct RedisSink {
    tx: UnboundedSender<String>,
}

impl RedisSink {
    /// Connect to Redis and spawn a background publisher task.
    ///
    /// `addr` is `[user:password@]host:port`; `list_key` is the Redis list name.
    pub async fn new(addr: &str, list_key: &str) -> anyhow::Result<Self> {
        // Build a standard redis:// URL so the crate handles auth automatically.
        let url = if addr.starts_with("redis://") || addr.starts_with("rediss://") {
            addr.to_string()
        } else {
            format!("redis://{addr}")
        };
        let client = redis::Client::open(url.as_str())
            .map_err(|e| anyhow::anyhow!("redis connect to {url}: {e}"))?;
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| anyhow::anyhow!("redis handshake {url}: {e}"))?;

        let key = list_key.to_string();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        tokio::spawn(async move {
            while let Some(line) = rx.recv().await {
                if let Err(e) = redis::cmd("RPUSH")
                    .arg(&key)
                    .arg(&line)
                    .query_async::<()>(&mut conn)
                    .await
                {
                    tracing::warn!(error = %e, "redis RPUSH failed");
                }
            }
        });

        Ok(Self { tx })
    }
}

impl EventSink for RedisSink {
    fn on_event(&self, conn: &ConnInfo, event: Event) {
        if let Some(line) = to_json_line(conn, &event) {
            let _ = self.tx.send(line);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// KafkaSink

/// Produces one JSON record per event to a Kafka topic (partition determined by
/// `conn_id` key).
///
/// # CLI format
/// ```text
/// kafka=127.0.0.1:9092
/// kafka=127.0.0.1:9092/my-topic
/// ```
///
/// Each Kafka record uses `"{upstream}|{conn_id}|{proxy}"` as the **message
/// key** (UTF-8 bytes).  Kafka's default murmur2 hash partitioner guarantees
/// that all events sharing the same key are routed to the same partition,
/// preserving per-connection ordering for consumers.  The key is also
/// human-readable, making it easy to filter or trace events in Kafka tooling.
///
/// Uses [`rskafka`] (pure-Rust, no C dependency). Network I/O happens on a
/// background task; the proxy forwarding path is never blocked.
pub struct KafkaSink {
    tx: UnboundedSender<(String, String)>,
}

impl KafkaSink {
    /// Connect to Kafka and spawn a background producer task.
    pub async fn new(broker: &str, topic: &str) -> anyhow::Result<Self> {
        use rskafka::client::{
            ClientBuilder,
            partition::{Compression, UnknownTopicHandling},
        };
        use rskafka::record::Record;

        let client = ClientBuilder::new(vec![broker.to_string()])
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("kafka connect to {broker}: {e}"))?;

        let partition = client
            .partition_client(topic, 0, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| anyhow::anyhow!("kafka partition_client topic={topic}: {e}"))?;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(String, String)>();

        tokio::spawn(async move {
            while let Some((key, line)) = rx.recv().await {
                let record = Record {
                    key: Some(key.into_bytes()),
                    value: Some(line.into_bytes()),
                    headers: BTreeMap::new(),
                    timestamp: chrono::Utc::now(),
                };
                if let Err(e) = partition
                    .produce(vec![record], Compression::NoCompression)
                    .await
                {
                    tracing::warn!(error = %e, "kafka produce failed");
                }
            }
        });

        Ok(Self { tx })
    }
}

impl EventSink for KafkaSink {
    fn on_event(&self, conn: &ConnInfo, event: Event) {
        if let Some(line) = to_json_line(conn, &event) {
            // key format: "{upstream}|{conn_id}|{proxy}"
            let key = format!("{}|{}|{}", conn.upstream, conn.conn_id, conn.proxy);
            let _ = self.tx.send((key, line));
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MultiSink

/// Fans out every [`Event`] to a list of inner sinks.
pub struct MultiSink {
    sinks: Vec<Arc<dyn EventSink>>,
}

impl MultiSink {
    pub fn new(sinks: Vec<Arc<dyn EventSink>>) -> Self {
        Self { sinks }
    }
}

impl EventSink for MultiSink {
    fn on_event(&self, conn: &ConnInfo, event: Event) {
        let mut iter = self.sinks.iter().peekable();
        while let Some(sink) = iter.next() {
            if iter.peek().is_some() {
                sink.on_event(conn, event.clone());
            } else {
                sink.on_event(conn, event);
                break;
            }
        }
    }
}
