//! Structured events emitted by the MySQL protocol analyzer.
//!
//! The [`Event`] enum captures every observable moment in a MySQL connection
//! lifecycle. Downstream [`EventSink`] implementations decide what to do with
//! them (log, persist, collect metrics, etc.).
//!
//! Each call to [`EventSink::on_event`] also carries a [`ConnInfo`] that
//! describes the connection context at the time the event was emitted.

use serde::Serialize;

// ─────────────────────────────────────────────────────────────────────────────
// Connection metadata

/// Immutable snapshot of connection-level metadata passed alongside every event.
///
/// Fields:
/// - `conn_id`  — proxy-internal sequential connection counter (starts at 1)
/// - `proxy`    — address the proxy is listening on (`listen` CLI flag)
/// - `upstream` — address of the upstream MySQL server
/// - `client`   — peer address of the connected client
/// - `user`     — MySQL user authenticated on this connection (empty before auth)
/// - `database` — current default database (updated on `USE` / `COM_INIT_DB`)
#[derive(Debug, Clone, Serialize)]
pub struct ConnInfo {
    pub conn_id:  u64,
    pub proxy:    String,
    pub upstream: String,
    pub client:   String,
    pub user:     String,
    pub database: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Event

/// A single observable moment in a MySQL connection.
///
/// Connection metadata (`who`, `where`) is carried by [`ConnInfo`] passed
/// alongside the event — individual variants only carry data specific to
/// that moment.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    // ── Handshake phase ──────────────────────────────────────────────────────

    /// Server sent its initial greeting packet.
    ServerHello {
        server_version: String,
        /// MySQL server-assigned connection id (not the proxy's `conn_id`).
        mysql_conn_id:  u32,
        auth_plugin:    String,
    },

    /// Client sent its authentication response.
    /// `user` and `database` in the accompanying [`ConnInfo`] are already
    /// updated to the values from this packet.
    ClientHandshake,

    /// Authentication succeeded — connection is now in command phase.
    Authenticated,

    /// Authentication failed.
    AuthFailed {
        code:    u16,
        message: String,
    },

    // ── Command phase (client → server) ──────────────────────────────────────

    /// A plain-text SQL query (`COM_QUERY`).
    Query { sql: String },

    /// A prepared-statement SQL string (`COM_STMT_PREPARE`).
    /// Emitted when the **server** responds with the assigned `stmt_id`,
    /// so consumers can correlate this with subsequent [`Event::StmtExecute`]
    /// events using the same `stmt_id`.
    StmtPrepare {
        /// Statement id assigned by MySQL server — use this to correlate
        /// with `StmtExecute` events carrying the same `stmt_id`.
        stmt_id:    u32,
        /// Number of `?` parameters in the prepared statement.
        num_params: u16,
        /// The original SQL template sent by the client.
        sql:        String,
    },

    /// Execution of a previously prepared statement (`COM_STMT_EXECUTE`).
    StmtExecute {
        /// The statement id assigned by the server in `COM_STMT_PREPARE` response.
        stmt_id: u32,
        /// Original SQL template (e.g. `SELECT * FROM t WHERE id = ?`).
        /// Present when the matching `COM_STMT_PREPARE` was observed.
        sql:     Option<String>,
        /// Decoded binary-protocol parameter values, in positional order.
        #[serde(serialize_with = "serialize_mysql_params")]
        params:  Vec<mysql_common::value::Value>,
    },

    /// `USE <database>` (`COM_INIT_DB`).
    /// `database` in the accompanying [`ConnInfo`] is already updated.
    InitDb { database: String },

    /// Client sent `COM_QUIT`.
    Quit,

    // ── Command phase (server → client) ──────────────────────────────────────

    /// Server returned OK after a command.
    QueryOk {
        affected_rows:  u64,
        last_insert_id: Option<u64>,
        warnings:       u16,
    },

    /// Server returned an error in response to a command.
    QueryError {
        code:      u16,
        sql_state: Option<String>,
        message:   String,
    },
}

/// Serialize `Vec<mysql_common::value::Value>` as a JSON array, mapping each
/// MySQL value to the closest native JSON type:
///
/// | MySQL            | JSON                                        |
/// |------------------|---------------------------------------------|
/// | NULL             | `null`                                      |
/// | Int / UInt       | number                                      |
/// | Float / Double   | number                                      |
/// | Bytes (UTF-8)    | string                                      |
/// | Bytes (binary)   | `"0x<hex>"`                                 |
/// | Date / DateTime  | `"YYYY-MM-DDTHH:MM:SS.ffffff"` (ISO 8601)  |
/// | Time             | `"[-]HHH:MM:SS.ffffff"`                     |
fn serialize_mysql_params<S>(
    params:     &[mysql_common::value::Value],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use mysql_common::value::Value;
    use serde::ser::SerializeSeq;

    let mut seq = serializer.serialize_seq(Some(params.len()))?;
    for v in params {
        match v {
            Value::NULL => seq.serialize_element(&Option::<()>::None)?,
            Value::Int(i)    => seq.serialize_element(i)?,
            Value::UInt(u)   => seq.serialize_element(u)?,
            Value::Float(f)  => seq.serialize_element(f)?,
            Value::Double(d) => seq.serialize_element(d)?,
            Value::Bytes(b)  => match std::str::from_utf8(b) {
                Ok(s) => seq.serialize_element(s)?,
                Err(_) => {
                    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
                    seq.serialize_element(&format!("0x{hex}"))?
                }
            },
            Value::Date(y, mo, d, h, mi, s, us) => {
                let ts = format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{us:06}");
                seq.serialize_element(&ts)?
            }
            Value::Time(neg, days, h, mi, s, us) => {
                let total_h = days * 24 + u32::from(*h);
                let sign = if *neg { "-" } else { "" };
                let ts = format!("{sign}{total_h:02}:{mi:02}:{s:02}.{us:06}");
                seq.serialize_element(&ts)?
            }
        }
    }
    seq.end()
}

/// Sink that receives [`Event`]s produced by the analyzer.
///
/// Each call carries a [`ConnInfo`] snapshot describing the connection at the
/// time the event was emitted (proxy addr, upstream addr, client addr, user,
/// database, and a proxy-internal connection id for cross-event correlation).
///
/// Implementations should be **cheap to call** — the analyzer calls `on_event`
/// synchronously inside its processing loop.  If you need async work (e.g.
/// writing to a database), spawn a task internally and send events through a
/// channel.
///
/// The trait is object-safe and `Send + Sync` so it can be shared across
/// connection tasks via `Arc<dyn EventSink>`.
pub trait EventSink: Send + Sync + 'static {
    fn on_event(&self, conn: &ConnInfo, event: Event);
}


