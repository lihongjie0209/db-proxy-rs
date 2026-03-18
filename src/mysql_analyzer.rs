//! Independent MySQL protocol analyzer.
//!
//! Receives a copy of the raw byte stream (both directions) via a channel,
//! reassembles MySQL packets, parses MySQL protocol using `mysql_common`,
//! and emits structured [`Event`]s paired with [`ConnInfo`] to an [`EventSink`].
//!
//! **Completely decoupled from the forwarding path.**

use std::{collections::HashMap, sync::Arc};

use bytes::BytesMut;
use tokio::sync::mpsc::UnboundedReceiver;

use mysql_common::{
    constants::{CapabilityFlags, ColumnFlags, ColumnType, Command},
    io::ParseBuf,
    packets::{
        CommonOkPacket, ErrPacket, HandshakePacket, HandshakeResponse, NullBitmap,
        OkPacketDeserializer, StmtPacket,
    },
    proto::MyDeserialize,
    value::{BinValue, ClientSide, Value, ValueDeserializer},
};

use crate::{
    analyzer::{Chunk, Dir},
    event::{ConnInfo, Event, EventSink},
    packet::{RawPacket, try_read_packet},
    protocol::Protocol,
};

// ─────────────────────────────────────────────────────────────────────────────
// Prepared statement registry

#[derive(Debug)]
struct PreparedStmt {
    sql: String,
    num_params: u16,
    /// Type info from the most recent `new_params_bound = 1` execute.
    last_types: Option<Vec<(ColumnType, ColumnFlags)>>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Command-in-flight tracker

#[derive(Debug)]
enum Pending {
    None,
    /// A COM_STMT_PREPARE is in flight; holds the SQL template.
    StmtPrepare(String),
}

// ─────────────────────────────────────────────────────────────────────────────
// Connection phase

#[derive(Debug)]
enum Phase {
    ServerHello,
    ClientResponse { server_caps: CapabilityFlags },
    Authenticating { caps: CapabilityFlags },
    Command { caps: CapabilityFlags },
}

// ─────────────────────────────────────────────────────────────────────────────
// Analyzer

struct Analyzer {
    // Connection identity
    conn_id: u64,
    proxy_addr: String,
    upstream_addr: String,
    client_addr: String,
    // Mutable connection state (updated during handshake / USE)
    user: String,
    database: Option<String>,
    // Protocol state machine
    phase: Phase,
    pending: Pending,
    stmts: HashMap<u32, PreparedStmt>,
    // Reassembly buffers
    c2s_buf: BytesMut,
    s2c_buf: BytesMut,
    sink: Arc<dyn EventSink>,
}

impl Analyzer {
    fn new(
        conn_id: u64,
        proxy_addr: String,
        upstream_addr: String,
        client_addr: String,
        sink: Arc<dyn EventSink>,
    ) -> Self {
        Self {
            conn_id,
            proxy_addr,
            upstream_addr,
            client_addr,
            user: String::new(),
            database: None,
            phase: Phase::ServerHello,
            pending: Pending::None,
            stmts: HashMap::new(),
            c2s_buf: BytesMut::new(),
            s2c_buf: BytesMut::new(),
            sink,
        }
    }

    /// Snapshot of connection metadata at this moment.
    fn conn_info(&self) -> ConnInfo {
        ConnInfo {
            protocol: Protocol::Mysql,
            conn_id: self.conn_id,
            proxy: self.proxy_addr.clone(),
            upstream: self.upstream_addr.clone(),
            client: self.client_addr.clone(),
            user: self.user.clone(),
            database: self.database.clone(),
        }
    }

    fn emit(&self, event: Event) {
        self.sink.on_event(&self.conn_info(), event);
    }

    fn feed(&mut self, dir: Dir, data: &[u8]) {
        match dir {
            Dir::ClientToServer => {
                self.c2s_buf.extend_from_slice(data);
                while let Some(pkt) = try_read_packet(&mut self.c2s_buf) {
                    if let Err(e) = self.on_client_packet(&pkt) {
                        tracing::debug!(
                            conn_id = self.conn_id,
                            client  = %self.client_addr,
                            error   = %e,
                            "c2s parse error"
                        );
                    }
                }
            }
            Dir::ServerToClient => {
                self.s2c_buf.extend_from_slice(data);
                while let Some(pkt) = try_read_packet(&mut self.s2c_buf) {
                    if let Err(e) = self.on_server_packet(&pkt) {
                        tracing::debug!(
                            conn_id = self.conn_id,
                            client  = %self.client_addr,
                            error   = %e,
                            "s2c parse error"
                        );
                    }
                }
            }
        }
    }

    // ── Server → Client ──────────────────────────────────────────────────────

    fn on_server_packet(&mut self, pkt: &RawPacket) -> Result<(), std::io::Error> {
        match self.phase {
            Phase::ServerHello => {
                let hs = HandshakePacket::deserialize((), &mut ParseBuf(&pkt.payload))?;
                self.emit(Event::ServerHello {
                    server_version: hs.server_version_str().into_owned(),
                    mysql_conn_id: hs.connection_id(),
                    auth_plugin: hs
                        .auth_plugin_name_str()
                        .map(|c| c.into_owned())
                        .unwrap_or_default(),
                });
                self.phase = Phase::ClientResponse {
                    server_caps: hs.capabilities(),
                };
            }

            Phase::Authenticating { caps } => {
                match pkt.payload.first().copied().unwrap_or(0xff) {
                    0x00 => {
                        self.emit(Event::Authenticated);
                        self.phase = Phase::Command { caps };
                    }
                    0xff => {
                        let err = ErrPacket::deserialize(caps, &mut ParseBuf(&pkt.payload))?;
                        if let ErrPacket::Error(e) = err {
                            self.emit(Event::AuthFailed {
                                code: e.error_code(),
                                message: e.message_str().into_owned(),
                            });
                        }
                    }
                    _ => {} // AuthMoreData (0x01) or AuthSwitchRequest (0xFE) — extra auth rounds
                }
            }

            Phase::Command { caps } => {
                self.on_server_command_response(pkt, caps)?;
            }

            Phase::ClientResponse { .. } => {}
        }
        Ok(())
    }

    fn on_server_command_response(
        &mut self,
        pkt: &RawPacket,
        caps: CapabilityFlags,
    ) -> Result<(), std::io::Error> {
        let first = pkt.payload.first().copied().unwrap_or(0);

        // StmtPacket: first byte 0x00, exactly 12 bytes — response to COM_STMT_PREPARE.
        // Emit StmtPrepare here (not on client request) so we can include the stmt_id
        // assigned by the server, enabling consumers to correlate Prepare ↔ Execute.
        if first == 0x00 && pkt.payload.len() == 12 {
            if let Pending::StmtPrepare(ref sql) = self.pending {
                let stmt_pkt = StmtPacket::deserialize((), &mut ParseBuf(&pkt.payload))?;
                let stmt_id = stmt_pkt.statement_id();
                let num_params = stmt_pkt.num_params();
                self.stmts.insert(
                    stmt_id,
                    PreparedStmt {
                        sql: sql.clone(),
                        num_params,
                        last_types: None,
                    },
                );
                self.emit(Event::StmtPrepare {
                    stmt_id,
                    num_params,
                    sql: sql.clone(),
                });
                self.pending = Pending::None;
                return Ok(());
            }
        }

        self.pending = Pending::None;

        if first == 0xff && pkt.sequence == 1 {
            if let Ok(ErrPacket::Error(e)) =
                ErrPacket::deserialize(caps, &mut ParseBuf(&pkt.payload))
            {
                self.emit(Event::QueryError {
                    code: e.error_code(),
                    sql_state: e.sql_state_ref().map(|s| s.as_str().into_owned()),
                    message: e.message_str().into_owned(),
                });
            }
        } else if first == 0x00 && pkt.sequence == 1 {
            if let Ok(deser) = OkPacketDeserializer::<CommonOkPacket>::deserialize(
                caps,
                &mut ParseBuf(&pkt.payload),
            ) {
                let ok: mysql_common::packets::OkPacket = deser.into();
                self.emit(Event::QueryOk {
                    affected_rows: ok.affected_rows(),
                    last_insert_id: ok.last_insert_id(),
                    warnings: ok.warnings(),
                });
            }
        }
        Ok(())
    }

    // ── Client → Server ──────────────────────────────────────────────────────

    fn on_client_packet(&mut self, pkt: &RawPacket) -> Result<(), std::io::Error> {
        match self.phase {
            Phase::ClientResponse { server_caps } => {
                // An SSL Request packet is 32 bytes (capabilities + max-packet + charset + filler).
                // When the client sends it, TLS follows — we can't parse the encrypted stream.
                // Detect it by length and CLIENT_SSL bit, then go silent for this connection.
                if pkt.payload.len() == 32 {
                    if let Ok(arr) = pkt.payload[0..4].try_into() {
                        let caps = CapabilityFlags::from_bits_truncate(u32::from_le_bytes(arr));
                        if caps.contains(CapabilityFlags::CLIENT_SSL) {
                            tracing::debug!(
                                conn_id = self.conn_id,
                                client  = %self.client_addr,
                                "SSL negotiation detected – analyzer disabled for this connection"
                            );
                            return Ok(());
                        }
                    }
                }
                let resp = HandshakeResponse::deserialize((), &mut ParseBuf(&pkt.payload))?;
                // Update connection state before emitting so ConnInfo is already populated.
                self.user = String::from_utf8_lossy(resp.user()).into_owned();
                self.database = resp
                    .db_name()
                    .map(|b| String::from_utf8_lossy(b).into_owned());
                self.emit(Event::ClientHandshake);
                let caps = server_caps & resp.capabilities();
                self.phase = Phase::Authenticating { caps };
            }
            Phase::Authenticating { .. } => {}
            Phase::Command { caps } => {
                if pkt.sequence == 0 {
                    self.handle_command(pkt, caps);
                }
            }
            Phase::ServerHello => {}
        }
        Ok(())
    }

    fn handle_command(&mut self, pkt: &RawPacket, caps: CapabilityFlags) {
        let Some(&cmd_byte) = pkt.payload.first() else {
            return;
        };

        if cmd_byte == Command::COM_QUERY as u8 {
            self.pending = Pending::None;
            // MySQL 8.0+ CLIENT_QUERY_ATTRIBUTES prepends varint(param_count) + varint(param_set_count).
            let sql_bytes = if caps.contains(CapabilityFlags::CLIENT_QUERY_ATTRIBUTES) {
                parse_query_attrs_prefix(&pkt.payload[1..])
            } else {
                &pkt.payload[1..]
            };
            self.emit(Event::Query {
                sql: lossy_trim(sql_bytes),
            });
        } else if cmd_byte == Command::COM_STMT_PREPARE as u8 {
            // Only store the SQL; emit StmtPrepare when the server responds with the stmt_id.
            self.pending = Pending::StmtPrepare(lossy_trim(&pkt.payload[1..]));
        } else if cmd_byte == Command::COM_STMT_EXECUTE as u8 {
            self.pending = Pending::None;
            self.handle_stmt_execute(pkt);
        } else if cmd_byte == Command::COM_INIT_DB as u8 {
            self.pending = Pending::None;
            let db = lossy_trim(&pkt.payload[1..]);
            // Update state before emitting so ConnInfo.database reflects the new database.
            self.database = Some(db.clone());
            self.emit(Event::InitDb { database: db });
        } else if cmd_byte == Command::COM_QUIT as u8 {
            self.emit(Event::Quit);
        } else {
            self.pending = Pending::None;
        }
    }

    fn handle_stmt_execute(&mut self, pkt: &RawPacket) {
        let payload = &pkt.payload;
        if payload.len() < 10 {
            return;
        }

        let stmt_id = u32::from_le_bytes(payload[1..5].try_into().unwrap());
        let (sql, num_params, cached_types) = match self.stmts.get(&stmt_id) {
            Some(s) => (Some(s.sql.clone()), s.num_params, s.last_types.clone()),
            None => (None, 0, None),
        };

        let parsed = parse_execute_params(payload, num_params, cached_types.as_deref());

        if let Some(ref p) = parsed {
            if !p.types.is_empty() {
                if let Some(stmt) = self.stmts.get_mut(&stmt_id) {
                    stmt.last_types = Some(p.types.clone());
                }
            }
        }

        self.emit(Event::StmtExecute {
            stmt_id,
            sql,
            params: parsed.map(|p| p.values).unwrap_or_default(),
        });
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Binary parameter decoding

struct ParsedParams {
    types: Vec<(ColumnType, ColumnFlags)>,
    values: Vec<Value>,
}

fn parse_execute_params(
    payload: &[u8],
    num_params: u16,
    cached_types: Option<&[(ColumnType, ColumnFlags)]>,
) -> Option<ParsedParams> {
    if num_params == 0 {
        return Some(ParsedParams {
            types: vec![],
            values: vec![],
        });
    }
    if payload.len() < 10 {
        return None;
    }

    let rest = &payload[10..];
    let n = num_params as usize;
    let bmap_len = (n + 7) / 8;
    if rest.len() < bmap_len + 1 {
        return None;
    }

    let bitmap_bytes = &rest[..bmap_len];
    let new_params_bound = rest[bmap_len];
    let mut buf = ParseBuf(&rest[bmap_len + 1..]);

    let types: Vec<(ColumnType, ColumnFlags)> = if new_params_bound == 1 {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            if buf.0.len() < 2 {
                return None;
            }
            let col_type =
                ColumnType::try_from(buf.0[0]).unwrap_or(ColumnType::MYSQL_TYPE_VAR_STRING);
            let col_flags = if buf.0[1] & 0x80 != 0 {
                ColumnFlags::UNSIGNED_FLAG
            } else {
                ColumnFlags::empty()
            };
            buf.0 = &buf.0[2..];
            out.push((col_type, col_flags));
        }
        out
    } else if let Some(ct) = cached_types {
        ct.to_vec()
    } else {
        return Some(ParsedParams {
            types: vec![],
            values: vec![],
        });
    };

    let bitmap = NullBitmap::<ClientSide, &[u8]>::from_bytes(bitmap_bytes);
    let mut values = Vec::with_capacity(n);
    for (i, &(col_type, col_flags)) in types.iter().enumerate() {
        if bitmap.is_null(i) {
            values.push(Value::NULL);
            continue;
        }
        match ValueDeserializer::<BinValue>::deserialize((col_type, col_flags), &mut buf) {
            Ok(vd) => values.push(vd.0),
            Err(_) => break,
        }
    }
    Some(ParsedParams { types, values })
}

// ─────────────────────────────────────────────────────────────────────────────

/// Skip `CLIENT_QUERY_ATTRIBUTES` header from a COM_QUERY payload (after cmd byte).
fn parse_query_attrs_prefix(after_cmd: &[u8]) -> &[u8] {
    let Some((&param_count, rest)) = after_cmd.split_first() else {
        return after_cmd;
    };
    if param_count >= 0xFB {
        return after_cmd;
    }
    let Some((_, rest2)) = rest.split_first() else {
        return after_cmd;
    };
    if param_count == 0 {
        return rest2;
    }
    after_cmd
}

fn lossy_trim(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .trim_end_matches('\0')
        .to_string()
}

pub async fn run(
    mut rx: UnboundedReceiver<Chunk>,
    conn_id: u64,
    proxy_addr: String,
    upstream_addr: String,
    client_addr: String,
    sink: Arc<dyn EventSink>,
) {
    let mut analyzer = Analyzer::new(conn_id, proxy_addr, upstream_addr, client_addr, sink);
    while let Some((dir, data)) = rx.recv().await {
        analyzer.feed(dir, &data);
    }
}
