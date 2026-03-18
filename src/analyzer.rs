use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{event::EventSink, protocol::Protocol};

/// Direction of a byte chunk inside the ordered channel.
#[derive(Debug, Clone, Copy)]
pub enum Dir {
    ClientToServer,
    ServerToClient,
}

/// Entry sent over the tee channel.
pub type Chunk = (Dir, Bytes);

pub async fn run(
    rx: UnboundedReceiver<Chunk>,
    conn_id: u64,
    proxy_addr: String,
    upstream_addr: String,
    client_addr: String,
    protocol: Protocol,
    sink: Arc<dyn EventSink>,
) {
    match protocol {
        Protocol::Mysql => {
            crate::mysql_analyzer::run(rx, conn_id, proxy_addr, upstream_addr, client_addr, sink)
                .await;
        }
        Protocol::Postgres => {
            crate::postgres_analyzer::run(
                rx,
                conn_id,
                proxy_addr,
                upstream_addr,
                client_addr,
                sink,
            )
            .await;
        }
    }
}
