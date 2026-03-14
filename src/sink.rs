//! Built-in [`EventSink`] implementations.
//!
//! | Type              | Description                                   |
//! |-------------------|-----------------------------------------------|
//! | [`TracingConsumer`] | Emits structured `tracing` log records      |
//! | [`JsonlConsumer`]   | Appends JSON Lines to a file with timestamp |
//! | [`MultiSink`]       | Fans out one event to multiple sinks        |

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

use crate::event::{ConnInfo, Event, EventSink};

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
        let cid      = conn.conn_id;
        let client   = &conn.client;
        let user     = &conn.user;
        let upstream = &conn.upstream;
        let db       = conn.database.as_deref().unwrap_or("-");

        match event {
            Event::ServerHello { server_version, mysql_conn_id, auth_plugin } => {
                tracing::info!(
                    conn_id = cid, %client, %upstream,
                    server_version, mysql_conn_id, auth_plugin,
                    "server hello"
                );
            }
            Event::ClientHandshake => {
                tracing::info!(
                    conn_id = cid, %client, %user, database = db,
                    "client handshake"
                );
            }
            Event::Authenticated => {
                tracing::info!(conn_id = cid, %client, %user, database = db, "authenticated");
            }
            Event::AuthFailed { code, message } => {
                tracing::warn!(conn_id = cid, %client, %user, code, message, "auth failed");
            }
            Event::Query { sql } => {
                tracing::info!(conn_id = cid, %client, %user, database = db, sql, "COM_QUERY");
            }
            Event::StmtPrepare { stmt_id, num_params, sql } => {
                tracing::info!(
                    conn_id = cid, %client, %user, database = db,
                    stmt_id, num_params, sql,
                    "COM_STMT_PREPARE"
                );
            }
            Event::StmtExecute { stmt_id, sql, params } => {
                let params_sql: Vec<String> = params.iter().map(|v| v.as_sql(false)).collect();
                match sql {
                    Some(tpl) => tracing::info!(
                        conn_id = cid, %client, %user, database = db,
                        stmt_id, sql = tpl, params = ?params_sql,
                        "COM_STMT_EXECUTE"
                    ),
                    None => tracing::info!(
                        conn_id = cid, %client, %user, database = db,
                        stmt_id, params = ?params_sql,
                        "COM_STMT_EXECUTE"
                    ),
                }
            }
            Event::InitDb { database } => {
                tracing::info!(conn_id = cid, %client, %user, database, "COM_INIT_DB");
            }
            Event::Quit => {
                tracing::info!(conn_id = cid, %client, %user, "COM_QUIT");
            }
            Event::QueryOk { affected_rows, last_insert_id, warnings } => {
                tracing::debug!(
                    conn_id = cid, %client, %user, database = db,
                    affected_rows, last_insert_id = ?last_insert_id, warnings,
                    "query ok"
                );
            }
            Event::QueryError { code, sql_state, message } => {
                tracing::warn!(
                    conn_id = cid, %client, %user, database = db,
                    code, sql_state = ?sql_state, message,
                    "query error"
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
        Ok(Self { writer: Mutex::new(BufWriter::new(file)) })
    }
}

/// Wire format: timestamp + flattened ConnInfo + flattened Event.
#[derive(Serialize)]
struct LogRecord<'a> {
    ts_ms: u128,
    #[serde(flatten)]
    conn:  &'a ConnInfo,
    #[serde(flatten)]
    event: &'a Event,
}

impl EventSink for JsonlConsumer {
    fn on_event(&self, conn: &ConnInfo, event: Event) {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        let record = LogRecord { ts_ms, conn, event: &event };

        match serde_json::to_string(&record) {
            Ok(line) => {
                if let Ok(mut w) = self.writer.lock() {
                    let _ = writeln!(w, "{line}");
                    let _ = w.flush();
                }
            }
            Err(e) => tracing::warn!(error = %e, "failed to serialize event to JSON"),
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
