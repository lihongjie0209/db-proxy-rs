//! MySQL packet framing: extract complete packets from a streaming byte buffer.
//!
//! Wire format: [payload_len: 3 bytes LE] [sequence_id: 1 byte] [payload: payload_len bytes]
//!
//! This module is used **only** by the analyzer. The forwarder deals in raw bytes.

use bytes::{Bytes, BytesMut};

/// A fully reassembled MySQL protocol packet.
#[derive(Debug)]
pub struct RawPacket {
    pub sequence: u8,
    pub payload: Bytes,
}

/// Try to extract one complete MySQL packet from the front of `buf`.
///
/// Returns `None` when there is not yet enough data (leaves `buf` unchanged).
/// Consumes exactly `4 + payload_len` bytes on success.
pub fn try_read_packet(buf: &mut BytesMut) -> Option<RawPacket> {
    if buf.len() < 4 {
        return None;
    }
    let payload_len =
        (buf[0] as usize) | ((buf[1] as usize) << 8) | ((buf[2] as usize) << 16);
    if buf.len() < 4 + payload_len {
        return None;
    }
    let header = buf.split_to(4);
    let payload = buf.split_to(payload_len).freeze();
    Some(RawPacket {
        sequence: header[3],
        payload,
    })
}
