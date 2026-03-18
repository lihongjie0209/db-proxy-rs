use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use bytes::{Buf, BytesMut};
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
    analyzer::{Chunk, Dir},
    event::{ConnInfo, Event, EventSink},
    protocol::Protocol,
};

const SSL_REQUEST_CODE: i32 = 80_877_103;
const CANCEL_REQUEST_CODE: i32 = 80_877_102;

#[derive(Debug, Clone)]
struct PreparedStmt {
    sql: String,
}

#[derive(Debug, Clone)]
struct BoundPortal {
    statement: String,
    params: Vec<Option<String>>,
}

#[derive(Debug, Clone, Copy)]
enum Phase {
    Startup,
    AwaitingSslResponse,
    Authenticating,
    Command,
    Disabled,
}

#[derive(Debug)]
enum FrontendMessage {
    Startup {
        protocol_version: i32,
        params: BTreeMap<String, String>,
    },
    SslRequest,
    CancelRequest {
        process_id: i32,
        secret_key: i32,
    },
    PasswordMessage,
    SimpleQuery {
        sql: String,
    },
    Parse {
        statement: String,
        sql: String,
        param_types: Vec<u32>,
    },
    Bind {
        portal: String,
        statement: String,
        param_formats: Vec<i16>,
        params: Vec<Option<String>>,
        result_formats: Vec<i16>,
    },
    Describe {
        target: u8,
        name: String,
    },
    Execute {
        portal: String,
        max_rows: i32,
    },
    Close {
        target: u8,
        name: String,
    },
    Flush,
    Sync,
    Terminate,
    Unsupported,
}

struct Analyzer {
    conn_id: u64,
    proxy_addr: String,
    upstream_addr: String,
    client_addr: String,
    user: String,
    database: Option<String>,
    phase: Phase,
    c2s_buf: BytesMut,
    s2c_buf: BytesMut,
    stmts: HashMap<String, PreparedStmt>,
    portals: HashMap<String, BoundPortal>,
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
            phase: Phase::Startup,
            c2s_buf: BytesMut::new(),
            s2c_buf: BytesMut::new(),
            stmts: HashMap::new(),
            portals: HashMap::new(),
            sink,
        }
    }

    fn conn_info(&self) -> ConnInfo {
        ConnInfo {
            protocol: Protocol::Postgres,
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
        if matches!(self.phase, Phase::Disabled) {
            return;
        }

        match dir {
            Dir::ClientToServer => {
                self.c2s_buf.extend_from_slice(data);
                if let Err(error) = self.process_client_messages() {
                    tracing::debug!(
                        conn_id = self.conn_id,
                        client = %self.client_addr,
                        error = %error,
                        "postgres c2s parse error"
                    );
                }
            }
            Dir::ServerToClient => {
                self.s2c_buf.extend_from_slice(data);
                if let Err(error) = self.process_server_messages() {
                    tracing::debug!(
                        conn_id = self.conn_id,
                        client = %self.client_addr,
                        error = %error,
                        "postgres s2c parse error"
                    );
                }
            }
        }
    }

    fn process_client_messages(&mut self) -> std::io::Result<()> {
        loop {
            let message = match self.phase {
                Phase::Startup | Phase::AwaitingSslResponse => {
                    try_read_startup_message(&mut self.c2s_buf)?
                }
                Phase::Authenticating | Phase::Command => {
                    try_read_frontend_message(&mut self.c2s_buf)?
                }
                Phase::Disabled => return Ok(()),
            };

            let Some(message) = message else {
                return Ok(());
            };

            self.on_frontend_message(message);
        }
    }

    fn process_server_messages(&mut self) -> std::io::Result<()> {
        loop {
            if matches!(self.phase, Phase::AwaitingSslResponse) {
                let Some(&response) = self.s2c_buf.first() else {
                    return Ok(());
                };
                self.s2c_buf.advance(1);
                match response {
                    b'S' => {
                        tracing::debug!(
                            conn_id = self.conn_id,
                            client = %self.client_addr,
                            "PostgreSQL TLS negotiation accepted – analyzer disabled for this connection"
                        );
                        self.phase = Phase::Disabled;
                        return Ok(());
                    }
                    b'N' => {
                        self.phase = Phase::Startup;
                        continue;
                    }
                    other => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("unexpected SSL response byte {other}"),
                        ));
                    }
                }
            }

            let Some(message) = backend::Message::parse(&mut self.s2c_buf)? else {
                return Ok(());
            };
            self.on_backend_message(message)?;
        }
    }

    fn on_frontend_message(&mut self, message: FrontendMessage) {
        match message {
            FrontendMessage::Startup {
                protocol_version,
                params,
            } => {
                self.user = params.get("user").cloned().unwrap_or_default();
                self.database = params.get("database").cloned();
                self.emit(Event::PgStartup {
                    protocol_version,
                    params,
                });
                self.phase = Phase::Authenticating;
            }
            FrontendMessage::SslRequest => {
                self.emit(Event::PgSslRequest);
                self.phase = Phase::AwaitingSslResponse;
            }
            FrontendMessage::CancelRequest {
                process_id,
                secret_key,
            } => {
                self.emit(Event::PgCancelRequest {
                    process_id,
                    secret_key,
                });
                self.phase = Phase::Disabled;
            }
            FrontendMessage::PasswordMessage => {}
            FrontendMessage::SimpleQuery { sql } => {
                self.emit(Event::PgSimpleQuery { sql });
            }
            FrontendMessage::Parse {
                statement,
                sql,
                param_types,
            } => {
                self.stmts
                    .insert(statement.clone(), PreparedStmt { sql: sql.clone() });
                self.emit(Event::PgParse {
                    statement,
                    sql,
                    param_types,
                });
            }
            FrontendMessage::Bind {
                portal,
                statement,
                param_formats,
                params,
                result_formats,
            } => {
                self.portals.insert(
                    portal.clone(),
                    BoundPortal {
                        statement: statement.clone(),
                        params: params.clone(),
                    },
                );
                self.emit(Event::PgBind {
                    portal,
                    statement,
                    param_formats,
                    params,
                    result_formats,
                });
            }
            FrontendMessage::Describe { target, name } => {
                self.emit(Event::PgDescribe {
                    target: describe_target(target),
                    name,
                });
            }
            FrontendMessage::Execute { portal, max_rows } => {
                let (statement, sql, params) = self
                    .portals
                    .get(&portal)
                    .map(|portal_info| {
                        let statement = portal_info.statement.clone();
                        let sql = self.stmts.get(&statement).map(|stmt| stmt.sql.clone());
                        (Some(statement), sql, portal_info.params.clone())
                    })
                    .unwrap_or((None, None, Vec::new()));
                self.emit(Event::PgExecute {
                    portal,
                    statement,
                    sql,
                    params,
                    max_rows,
                });
            }
            FrontendMessage::Close { target, name } => {
                match target {
                    b'S' => {
                        self.stmts.remove(&name);
                    }
                    b'P' => {
                        self.portals.remove(&name);
                    }
                    _ => {}
                }
                self.emit(Event::PgClose {
                    target: describe_target(target),
                    name,
                });
            }
            FrontendMessage::Flush => {
                self.emit(Event::PgFlush);
            }
            FrontendMessage::Sync => {
                self.emit(Event::PgSync);
            }
            FrontendMessage::Terminate => {
                self.emit(Event::PgTerminate);
            }
            FrontendMessage::Unsupported => {}
        }
    }

    fn on_backend_message(&mut self, message: backend::Message) -> std::io::Result<()> {
        match message {
            backend::Message::AuthenticationCleartextPassword => {
                self.emit(Event::PgAuthRequest {
                    method: "cleartext_password".to_string(),
                });
            }
            backend::Message::AuthenticationMd5Password(_) => {
                self.emit(Event::PgAuthRequest {
                    method: "md5_password".to_string(),
                });
            }
            backend::Message::AuthenticationSasl(body) => {
                let mut mechanisms = body.mechanisms();
                let mut values = Vec::new();
                while let Some(mechanism) = mechanisms.next()? {
                    values.push(mechanism.to_string());
                }
                self.emit(Event::PgAuthRequest {
                    method: format!("sasl({})", values.join(",")),
                });
            }
            backend::Message::AuthenticationSaslContinue(_) => {
                self.emit(Event::PgAuthRequest {
                    method: "sasl_continue".to_string(),
                });
            }
            backend::Message::AuthenticationSaslFinal(_) => {
                self.emit(Event::PgAuthRequest {
                    method: "sasl_final".to_string(),
                });
            }
            backend::Message::AuthenticationOk => {
                self.emit(Event::PgAuthenticated);
            }
            backend::Message::ParseComplete => {
                self.emit(Event::PgParseComplete);
            }
            backend::Message::BindComplete => {
                self.emit(Event::PgBindComplete);
            }
            backend::Message::CloseComplete => {
                self.emit(Event::PgCloseComplete);
            }
            backend::Message::CommandComplete(body) => {
                self.emit(Event::PgCommandComplete {
                    tag: body.tag()?.to_string(),
                });
            }
            backend::Message::ErrorResponse(body) => {
                let error = extract_error(body.fields())?;
                if matches!(self.phase, Phase::Authenticating) {
                    self.emit(Event::PgAuthFailed {
                        severity: error.severity,
                        code: error.code,
                        message: error.message,
                    });
                } else {
                    self.emit(Event::PgError {
                        severity: error.severity,
                        code: error.code,
                        message: error.message,
                    });
                }
            }
            backend::Message::ReadyForQuery(body) => {
                self.phase = Phase::Command;
                self.emit(Event::PgReadyForQuery {
                    status: ready_status(body.status()),
                });
            }
            _ => {}
        }
        Ok(())
    }
}

struct PgError {
    severity: Option<String>,
    code: Option<String>,
    message: String,
}

fn extract_error(mut fields: backend::ErrorFields<'_>) -> std::io::Result<PgError> {
    let mut severity = None;
    let mut code = None;
    let mut message = None;

    while let Some(field) = fields.next()? {
        let value = String::from_utf8_lossy(field.value_bytes()).into_owned();
        match field.type_() {
            b'S' | b'V' if severity.is_none() => severity = Some(value),
            b'C' => code = Some(value),
            b'M' => message = Some(value),
            _ => {}
        }
    }

    Ok(PgError {
        severity,
        code,
        message: message.unwrap_or_else(|| "unknown PostgreSQL error".to_string()),
    })
}

fn try_read_startup_message(buf: &mut BytesMut) -> std::io::Result<Option<FrontendMessage>> {
    if buf.len() < 4 {
        return Ok(None);
    }

    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len < 8 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid PostgreSQL startup message length",
        ));
    }
    if buf.len() < len {
        return Ok(None);
    }

    let frame = buf.split_to(len);
    let code = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
    match code {
        SSL_REQUEST_CODE => Ok(Some(FrontendMessage::SslRequest)),
        CANCEL_REQUEST_CODE => {
            if frame.len() < 16 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid PostgreSQL cancel request length",
                ));
            }
            let process_id = i32::from_be_bytes([frame[8], frame[9], frame[10], frame[11]]);
            let secret_key = i32::from_be_bytes([frame[12], frame[13], frame[14], frame[15]]);
            Ok(Some(FrontendMessage::CancelRequest {
                process_id,
                secret_key,
            }))
        }
        protocol_version => {
            let params = parse_startup_params(&frame[8..])?;
            Ok(Some(FrontendMessage::Startup {
                protocol_version,
                params,
            }))
        }
    }
}

fn try_read_frontend_message(buf: &mut BytesMut) -> std::io::Result<Option<FrontendMessage>> {
    if buf.len() < 5 {
        return Ok(None);
    }

    let tag = buf[0];
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    if len < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid PostgreSQL frontend message length",
        ));
    }

    let total = len + 1;
    if buf.len() < total {
        return Ok(None);
    }

    let frame = buf.split_to(total);
    let body = &frame[5..];
    let message = match tag {
        b'Q' => FrontendMessage::SimpleQuery {
            sql: parse_single_cstr(body)?,
        },
        b'P' => parse_parse(body)?,
        b'B' => parse_bind(body)?,
        b'D' => parse_describe(body)?,
        b'E' => parse_execute(body)?,
        b'C' => parse_close(body)?,
        b'H' => FrontendMessage::Flush,
        b'S' => FrontendMessage::Sync,
        b'X' => FrontendMessage::Terminate,
        b'p' => FrontendMessage::PasswordMessage,
        _ => FrontendMessage::Unsupported,
    };
    Ok(Some(message))
}

fn parse_startup_params(body: &[u8]) -> std::io::Result<BTreeMap<String, String>> {
    let mut idx = 0;
    let mut params = BTreeMap::new();
    while idx < body.len() {
        if body[idx] == 0 {
            break;
        }
        let key = parse_cstr(body, &mut idx)?;
        let value = parse_cstr(body, &mut idx)?;
        params.insert(key, value);
    }
    Ok(params)
}

fn parse_parse(body: &[u8]) -> std::io::Result<FrontendMessage> {
    let mut idx = 0;
    let statement = parse_cstr(body, &mut idx)?;
    let sql = parse_cstr(body, &mut idx)?;
    let param_count = read_u16(body, &mut idx)? as usize;
    let mut param_types = Vec::with_capacity(param_count);
    for _ in 0..param_count {
        param_types.push(read_u32(body, &mut idx)?);
    }
    Ok(FrontendMessage::Parse {
        statement,
        sql,
        param_types,
    })
}

fn parse_bind(body: &[u8]) -> std::io::Result<FrontendMessage> {
    let mut idx = 0;
    let portal = parse_cstr(body, &mut idx)?;
    let statement = parse_cstr(body, &mut idx)?;
    let format_count = read_u16(body, &mut idx)? as usize;
    let mut param_formats = Vec::with_capacity(format_count);
    for _ in 0..format_count {
        param_formats.push(read_i16(body, &mut idx)?);
    }

    let param_count = read_u16(body, &mut idx)? as usize;
    let mut params = Vec::with_capacity(param_count);
    for param_idx in 0..param_count {
        let len = read_i32(body, &mut idx)?;
        if len < 0 {
            params.push(None);
            continue;
        }
        let len = len as usize;
        if idx + len > body.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated PostgreSQL bind value",
            ));
        }
        let value = &body[idx..idx + len];
        idx += len;
        let format = param_format(&param_formats, param_idx);
        params.push(Some(decode_param(value, format)));
    }

    let result_format_count = read_u16(body, &mut idx)? as usize;
    let mut result_formats = Vec::with_capacity(result_format_count);
    for _ in 0..result_format_count {
        result_formats.push(read_i16(body, &mut idx)?);
    }

    Ok(FrontendMessage::Bind {
        portal,
        statement,
        param_formats,
        params,
        result_formats,
    })
}

fn parse_describe(body: &[u8]) -> std::io::Result<FrontendMessage> {
    let mut idx = 0;
    let target = read_u8(body, &mut idx)?;
    let name = parse_cstr(body, &mut idx)?;
    Ok(FrontendMessage::Describe { target, name })
}

fn parse_execute(body: &[u8]) -> std::io::Result<FrontendMessage> {
    let mut idx = 0;
    let portal = parse_cstr(body, &mut idx)?;
    let max_rows = read_i32(body, &mut idx)?;
    Ok(FrontendMessage::Execute { portal, max_rows })
}

fn parse_close(body: &[u8]) -> std::io::Result<FrontendMessage> {
    let mut idx = 0;
    let target = read_u8(body, &mut idx)?;
    let name = parse_cstr(body, &mut idx)?;
    Ok(FrontendMessage::Close { target, name })
}

fn parse_single_cstr(body: &[u8]) -> std::io::Result<String> {
    let mut idx = 0;
    parse_cstr(body, &mut idx)
}

fn parse_cstr(body: &[u8], idx: &mut usize) -> std::io::Result<String> {
    let Some(end) = body[*idx..].iter().position(|&b| b == 0) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "unterminated PostgreSQL cstring",
        ));
    };
    let start = *idx;
    let end = start + end;
    *idx = end + 1;
    Ok(String::from_utf8_lossy(&body[start..end]).into_owned())
}

fn read_u8(body: &[u8], idx: &mut usize) -> std::io::Result<u8> {
    if *idx >= body.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "truncated PostgreSQL message",
        ));
    }
    let value = body[*idx];
    *idx += 1;
    Ok(value)
}

fn read_u16(body: &[u8], idx: &mut usize) -> std::io::Result<u16> {
    if *idx + 2 > body.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "truncated PostgreSQL message",
        ));
    }
    let value = u16::from_be_bytes([body[*idx], body[*idx + 1]]);
    *idx += 2;
    Ok(value)
}

fn read_i16(body: &[u8], idx: &mut usize) -> std::io::Result<i16> {
    if *idx + 2 > body.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "truncated PostgreSQL message",
        ));
    }
    let value = i16::from_be_bytes([body[*idx], body[*idx + 1]]);
    *idx += 2;
    Ok(value)
}

fn read_u32(body: &[u8], idx: &mut usize) -> std::io::Result<u32> {
    if *idx + 4 > body.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "truncated PostgreSQL message",
        ));
    }
    let value = u32::from_be_bytes([body[*idx], body[*idx + 1], body[*idx + 2], body[*idx + 3]]);
    *idx += 4;
    Ok(value)
}

fn read_i32(body: &[u8], idx: &mut usize) -> std::io::Result<i32> {
    if *idx + 4 > body.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "truncated PostgreSQL message",
        ));
    }
    let value = i32::from_be_bytes([body[*idx], body[*idx + 1], body[*idx + 2], body[*idx + 3]]);
    *idx += 4;
    Ok(value)
}

fn param_format(formats: &[i16], idx: usize) -> i16 {
    match formats {
        [] => 0,
        [single] => *single,
        many => many.get(idx).copied().unwrap_or(0),
    }
}

fn decode_param(value: &[u8], format: i16) -> String {
    if format == 0 {
        String::from_utf8_lossy(value).into_owned()
    } else {
        let hex: String = value.iter().map(|b| format!("{b:02x}")).collect();
        format!("0x{hex}")
    }
}

fn describe_target(target: u8) -> String {
    match target {
        b'S' => "statement".to_string(),
        b'P' => "portal".to_string(),
        _ => format!("unknown({target})"),
    }
}

fn ready_status(status: u8) -> String {
    match status {
        b'I' => "idle".to_string(),
        b'T' => "in_transaction".to_string(),
        b'E' => "failed_transaction".to_string(),
        other => format!("unknown({other})"),
    }
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

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};
    use postgres_protocol::{IsNull, message::frontend};

    use super::{FrontendMessage, try_read_frontend_message, try_read_startup_message};

    #[test]
    fn parses_startup_message() {
        let mut buf = BytesMut::new();
        frontend::startup_message([("user", "postgres"), ("database", "proxy_test")], &mut buf)
            .unwrap();

        let message = try_read_startup_message(&mut buf).unwrap().unwrap();
        match message {
            FrontendMessage::Startup {
                protocol_version,
                params,
            } => {
                assert_eq!(protocol_version, 0x00_03_00_00);
                assert_eq!(params.get("user").map(String::as_str), Some("postgres"));
                assert_eq!(
                    params.get("database").map(String::as_str),
                    Some("proxy_test")
                );
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn parses_simple_query() {
        let mut buf = BytesMut::new();
        frontend::query("SELECT 1", &mut buf).unwrap();

        let message = try_read_frontend_message(&mut buf).unwrap().unwrap();
        match message {
            FrontendMessage::SimpleQuery { sql } => assert_eq!(sql, "SELECT 1"),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn parses_bind_message() {
        let mut buf = BytesMut::new();
        let bind_result = frontend::bind(
            "",
            "stmt1",
            [0i16],
            ["hello", "world"],
            |value, out| {
                out.put_slice(value.as_bytes());
                Ok(IsNull::No)
            },
            [0i16],
            &mut buf,
        );
        assert!(bind_result.is_ok());

        let message = try_read_frontend_message(&mut buf).unwrap().unwrap();
        match message {
            FrontendMessage::Bind {
                portal,
                statement,
                params,
                ..
            } => {
                assert_eq!(portal, "");
                assert_eq!(statement, "stmt1");
                assert_eq!(
                    params,
                    vec![Some("hello".to_string()), Some("world".to_string())]
                );
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }
}
