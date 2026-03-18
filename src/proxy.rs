//! Pure TCP transparent proxy with tee.
//!
//! The forwarder knows **nothing** about MySQL protocol.
//! It just copies bytes in both directions and sends a tagged copy to the
//! analyzer channel — exactly like the Unix `tee(1)` command.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result};
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::analyzer::{self, Chunk, Dir};
use crate::event::EventSink;
use crate::protocol::Protocol;

const BUF_SIZE: usize = 64 * 1024;

/// Proxy-internal connection counter — each accepted TCP connection gets a
/// unique monotonically increasing id starting at 1.
static CONN_ID: AtomicU64 = AtomicU64::new(1);

pub async fn run(
    listen_addr: &str,
    upstream_addr: &str,
    protocol: Protocol,
    sink: Arc<dyn EventSink>,
) -> Result<()> {
    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("binding to {listen_addr}"))?;

    tracing::info!(
        listen = listen_addr,
        upstream = upstream_addr,
        protocol = protocol.as_str(),
        "proxy ready"
    );

    loop {
        let (client, peer_addr) = listener.accept().await.context("accept")?;
        let conn_id = CONN_ID.fetch_add(1, Ordering::Relaxed);
        let upstream = upstream_addr.to_owned();
        let proxy = listen_addr.to_owned();
        let peer = peer_addr.to_string();
        let protocol = protocol;
        let sink = Arc::clone(&sink);

        tokio::spawn(async move {
            tracing::info!(conn_id, client = %peer, "connected");
            match pipe(
                client,
                conn_id,
                proxy,
                upstream,
                peer.clone(),
                protocol,
                sink,
            )
            .await
            {
                Ok(()) => tracing::info!(conn_id, client = %peer, "disconnected"),
                Err(e) => tracing::warn!(conn_id, client = %peer, error = %e, "connection error"),
            }
        });
    }
}

async fn pipe(
    client: TcpStream,
    conn_id: u64,
    proxy_addr: String,
    upstream_addr: String,
    client_addr: String,
    protocol: Protocol,
    sink: Arc<dyn EventSink>,
) -> Result<()> {
    let server = TcpStream::connect(&upstream_addr)
        .await
        .with_context(|| format!("connecting to {upstream_addr}"))?;

    let (tx, rx) = mpsc::unbounded_channel::<Chunk>();

    tokio::spawn(async move {
        analyzer::run(
            rx,
            conn_id,
            proxy_addr,
            upstream_addr,
            client_addr,
            protocol,
            sink,
        )
        .await;
    });

    let (mut c_read, mut c_write) = client.into_split();
    let (mut s_read, mut s_write) = server.into_split();

    let tx_c2s = tx.clone();
    let c2s = async move {
        let mut buf = vec![0u8; BUF_SIZE];
        loop {
            let n = c_read.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let _ = tx_c2s.send((Dir::ClientToServer, Bytes::copy_from_slice(&buf[..n])));
            s_write.write_all(&buf[..n]).await?;
        }
        Ok::<_, anyhow::Error>(())
    };

    let s2c = async move {
        let mut buf = vec![0u8; BUF_SIZE];
        loop {
            let n = s_read.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let _ = tx.send((Dir::ServerToClient, Bytes::copy_from_slice(&buf[..n])));
            c_write.write_all(&buf[..n]).await?;
        }
        Ok::<_, anyhow::Error>(())
    };

    tokio::select! {
        r = c2s => r,
        r = s2c => r,
    }
}
