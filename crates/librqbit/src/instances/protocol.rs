use std::net::SocketAddr;

use anyhow::{Context, bail};
#[cfg(not(feature = "postcard-rpc"))]
use byteorder::{BE, ByteOrder};
use serde::{Deserialize, Serialize};

pub(crate) const MODE_CONTROL: u8 = 0x01;
pub(crate) const MODE_FORWARD_TCP: u8 = 0x02;

#[cfg(not(feature = "postcard-rpc"))]
pub(crate) const MSG_HELLO: u8 = 0x01;
#[cfg(not(feature = "postcard-rpc"))]
pub(crate) const MSG_TORRENTS_ADDED: u8 = 0x02;
#[cfg(not(feature = "postcard-rpc"))]
pub(crate) const MSG_TORRENTS_REMOVED: u8 = 0x03;
#[cfg(not(feature = "postcard-rpc"))]
pub(crate) const MSG_WHO_HAS: u8 = 0x04;
#[cfg(not(feature = "postcard-rpc"))]
pub(crate) const MSG_I_HAS: u8 = 0x05;
#[cfg(not(feature = "postcard-rpc"))]
pub(crate) const MSG_GOODBYE: u8 = 0x06;

// ---------------------------------------------------------------------------
// Typed message definitions (always compiled, format-agnostic)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ControlMessage {
    Hello { instance_id: String },
    TorrentsAdded { info_hashes: Vec<[u8; 20]> },
    TorrentsRemoved { info_hashes: Vec<[u8; 20]> },
    WhoHas { info_hash: [u8; 20] },
    IHas { info_hash: [u8; 20] },
    Goodbye,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ForwardTcpMeta {
    pub peer_addr: SocketAddr,
    pub handshake: Vec<u8>,
    pub extra: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Framing helpers (format-agnostic)
// ---------------------------------------------------------------------------

/// Write a length-prefixed payload frame into `buf`.
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn write_payload_frame(buf: &mut Vec<u8>, payload: &[u8]) {
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(payload);
}

// ---------------------------------------------------------------------------
// Control message encode/decode (cfg-gated: postcard vs manual)
// ---------------------------------------------------------------------------

#[cfg(feature = "postcard-rpc")]
pub(crate) fn encode_control(msg: &ControlMessage) -> Vec<u8> {
    postcard::to_allocvec(msg).expect("postcard serialization infallible for these types")
}

#[cfg(feature = "postcard-rpc")]
pub(crate) fn decode_control(buf: &[u8]) -> anyhow::Result<ControlMessage> {
    postcard::from_bytes(buf).context("postcard decode error")
}

#[cfg(not(feature = "postcard-rpc"))]
pub(crate) fn encode_control(msg: &ControlMessage) -> Vec<u8> {
    let mut buf = Vec::new();
    let (msg_type, payload) = match msg {
        ControlMessage::Hello { instance_id } => (MSG_HELLO, encode_hello(instance_id)),
        ControlMessage::TorrentsAdded { info_hashes } => {
            let refs: Vec<&[u8; 20]> = info_hashes.iter().collect();
            (MSG_TORRENTS_ADDED, encode_torrents(&refs))
        }
        ControlMessage::TorrentsRemoved { info_hashes } => {
            let refs: Vec<&[u8; 20]> = info_hashes.iter().collect();
            (MSG_TORRENTS_REMOVED, encode_torrents(&refs))
        }
        ControlMessage::WhoHas { info_hash } => (MSG_WHO_HAS, info_hash.to_vec()),
        ControlMessage::IHas { info_hash } => (MSG_I_HAS, info_hash.to_vec()),
        ControlMessage::Goodbye => (MSG_GOODBYE, Vec::new()),
    };
    buf.push(msg_type);
    buf.extend_from_slice(&payload);
    buf
}

#[cfg(not(feature = "postcard-rpc"))]
pub(crate) fn decode_control(buf: &[u8]) -> anyhow::Result<ControlMessage> {
    if buf.is_empty() {
        bail!("empty control payload");
    }
    let msg_type = buf[0];
    let payload = &buf[1..];
    match msg_type {
        MSG_HELLO => {
            let hello = decode_hello(payload)?;
            Ok(ControlMessage::Hello {
                instance_id: hello.instance_id,
            })
        }
        MSG_TORRENTS_ADDED => Ok(ControlMessage::TorrentsAdded {
            info_hashes: decode_torrents(payload)?,
        }),
        MSG_TORRENTS_REMOVED => Ok(ControlMessage::TorrentsRemoved {
            info_hashes: decode_torrents(payload)?,
        }),
        MSG_WHO_HAS => Ok(ControlMessage::WhoHas {
            info_hash: decode_single_info_hash(payload)?,
        }),
        MSG_I_HAS => Ok(ControlMessage::IHas {
            info_hash: decode_single_info_hash(payload)?,
        }),
        MSG_GOODBYE => Ok(ControlMessage::Goodbye),
        other => bail!("unknown message type {other}"),
    }
}

// ---------------------------------------------------------------------------
// Forward metadata encode/decode (cfg-gated: postcard vs manual)
// ---------------------------------------------------------------------------

#[cfg(feature = "postcard-rpc")]
pub(crate) fn encode_forward(meta: &ForwardTcpMeta) -> Vec<u8> {
    postcard::to_allocvec(meta).expect("postcard serialization infallible for these types")
}

#[cfg(feature = "postcard-rpc")]
pub(crate) fn decode_forward(buf: &[u8]) -> anyhow::Result<ForwardTcpMeta> {
    postcard::from_bytes(buf).context("postcard decode error")
}

#[cfg(not(feature = "postcard-rpc"))]
pub(crate) fn encode_forward(meta: &ForwardTcpMeta) -> Vec<u8> {
    encode_forward_metadata(meta.peer_addr, &meta.handshake, &meta.extra)
}

#[cfg(not(feature = "postcard-rpc"))]
pub(crate) fn decode_forward(buf: &[u8]) -> anyhow::Result<ForwardTcpMeta> {
    // Manual format: [addr_len][addr][hs_len][hs][extra_len][extra]
    let mut off = 0;
    if buf.len() < 4 {
        bail!("forward payload too short for addr length");
    }
    let addr_len = BE::read_u32(&buf[off..off + 4]) as usize;
    off += 4;
    if buf.len() < off + addr_len {
        bail!("forward payload truncated at addr");
    }
    let peer_addr = decode_socket_addr(&buf[off..off + addr_len])?;
    off += addr_len;

    if buf.len() < off + 4 {
        bail!("forward payload too short for handshake length");
    }
    let hs_len = BE::read_u32(&buf[off..off + 4]) as usize;
    off += 4;
    if buf.len() < off + hs_len {
        bail!("forward payload truncated at handshake");
    }
    let handshake = buf[off..off + hs_len].to_vec();
    off += hs_len;

    if buf.len() < off + 4 {
        bail!("forward payload too short for extra length");
    }
    let extra_len = BE::read_u32(&buf[off..off + 4]) as usize;
    off += 4;
    if buf.len() < off + extra_len {
        bail!("forward payload truncated at extra");
    }
    let extra = buf[off..off + extra_len].to_vec();

    Ok(ForwardTcpMeta {
        peer_addr,
        handshake,
        extra,
    })
}

/// Read a length-prefixed forward metadata blob from an async reader.
pub(crate) async fn read_forward_metadata<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> anyhow::Result<ForwardTcpMeta> {
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 1024 * 1024 {
        bail!("forward metadata too large: {len}");
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    decode_forward(&buf)
}

// ---------------------------------------------------------------------------
// Manual encoding helpers (used only without postcard-rpc feature)
// ---------------------------------------------------------------------------

#[cfg(not(feature = "postcard-rpc"))]
#[allow(clippy::cast_possible_truncation)]
fn encode_hello(instance_id: &str) -> Vec<u8> {
    let id_bytes = instance_id.as_bytes();
    let mut payload = Vec::with_capacity(2 + id_bytes.len());
    BE::write_u16(&mut payload, id_bytes.len() as u16);
    payload.extend_from_slice(id_bytes);
    payload
}

#[cfg(not(feature = "postcard-rpc"))]
struct DecodedHello {
    instance_id: String,
}

#[cfg(not(feature = "postcard-rpc"))]
fn decode_hello(buf: &[u8]) -> anyhow::Result<DecodedHello> {
    if buf.len() < 2 {
        bail!("hello payload too short");
    }
    let id_len = BE::read_u16(buf) as usize;
    if buf.len() < 2 + id_len {
        bail!("hello payload truncated");
    }
    let instance_id = std::str::from_utf8(&buf[2..2 + id_len])
        .context("invalid utf-8 in instance id")?
        .to_string();
    Ok(DecodedHello { instance_id })
}

#[cfg(not(feature = "postcard-rpc"))]
#[allow(clippy::cast_possible_truncation)]
fn encode_torrents(info_hashes: &[&[u8; 20]]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(2 + info_hashes.len() * 20);
    BE::write_u16(&mut payload, info_hashes.len() as u16);
    for ih in info_hashes {
        payload.extend_from_slice(&ih[..]);
    }
    payload
}

#[cfg(not(feature = "postcard-rpc"))]
fn decode_torrents(buf: &[u8]) -> anyhow::Result<Vec<[u8; 20]>> {
    if buf.len() < 2 {
        bail!("torrents payload too short");
    }
    let count = BE::read_u16(buf) as usize;
    let needed = 2 + count * 20;
    if buf.len() < needed {
        bail!("torrents payload truncated");
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let off = 2 + i * 20;
        let mut ih = [0u8; 20];
        ih.copy_from_slice(&buf[off..off + 20]);
        out.push(ih);
    }
    Ok(out)
}

#[cfg(not(feature = "postcard-rpc"))]
fn decode_single_info_hash(buf: &[u8]) -> anyhow::Result<[u8; 20]> {
    if buf.len() < 20 {
        bail!("info hash payload too short");
    }
    let mut ih = [0u8; 20];
    ih.copy_from_slice(&buf[..20]);
    Ok(ih)
}

#[cfg(not(feature = "postcard-rpc"))]
#[allow(clippy::cast_possible_truncation)]
fn encode_forward_metadata(peer_addr: SocketAddr, handshake: &[u8], extra: &[u8]) -> Vec<u8> {
    let addr_bytes = encode_socket_addr(peer_addr);
    let mut buf = Vec::with_capacity(4 + addr_bytes.len() + 4 + handshake.len() + 4 + extra.len());
    buf.extend_from_slice(&(addr_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(&addr_bytes);
    buf.extend_from_slice(&(handshake.len() as u32).to_be_bytes());
    buf.extend_from_slice(handshake);
    buf.extend_from_slice(&(extra.len() as u32).to_be_bytes());
    buf.extend_from_slice(extra);
    buf
}

#[cfg(not(feature = "postcard-rpc"))]
fn encode_socket_addr(addr: SocketAddr) -> Vec<u8> {
    match addr {
        SocketAddr::V4(v4) => {
            let mut buf = Vec::with_capacity(1 + 4 + 2);
            buf.push(4);
            buf.extend_from_slice(&v4.ip().octets());
            buf.extend_from_slice(&v4.port().to_be_bytes());
            buf
        }
        SocketAddr::V6(v6) => {
            let mut buf = Vec::with_capacity(1 + 16 + 2 + 4 + 4);
            buf.push(6);
            buf.extend_from_slice(&v6.ip().octets());
            buf.extend_from_slice(&v6.port().to_be_bytes());
            buf.extend_from_slice(&v6.flowinfo().to_be_bytes());
            buf.extend_from_slice(&v6.scope_id().to_be_bytes());
            buf
        }
    }
}

#[cfg(not(feature = "postcard-rpc"))]
fn decode_socket_addr(buf: &[u8]) -> anyhow::Result<SocketAddr> {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
    if buf.is_empty() {
        bail!("empty socket addr");
    }
    match buf[0] {
        4 => {
            if buf.len() < 1 + 4 + 2 {
                bail!("ipv4 addr too short");
            }
            let octets: [u8; 4] = buf[1..5].try_into().unwrap();
            let port = BE::read_u16(&buf[5..7]);
            Ok(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(octets),
                port,
            )))
        }
        6 => {
            if buf.len() < 1 + 16 + 2 + 4 + 4 {
                bail!("ipv6 addr too short");
            }
            let octets: [u8; 16] = buf[1..17].try_into().unwrap();
            let port = BE::read_u16(&buf[17..19]);
            let flowinfo = BE::read_u32(&buf[19..23]);
            let scope_id = BE::read_u32(&buf[23..27]);
            Ok(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(octets),
                port,
                flowinfo,
                scope_id,
            )))
        }
        other => bail!("unknown address family {other}"),
    }
}
