use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

use anyhow::{Context, bail};
use byteorder::{BE, ByteOrder};

pub(crate) const MODE_CONTROL: u8 = 0x01;
pub(crate) const MODE_FORWARD_TCP: u8 = 0x02;

pub(crate) const MSG_HELLO: u8 = 0x01;
pub(crate) const MSG_TORRENTS_ADDED: u8 = 0x02;
pub(crate) const MSG_TORRENTS_REMOVED: u8 = 0x03;
pub(crate) const MSG_WHO_HAS: u8 = 0x04;
pub(crate) const MSG_I_HAS: u8 = 0x05;
pub(crate) const MSG_GOODBYE: u8 = 0x06;

#[allow(dead_code)]
pub(crate) const FRAME_LEN_SIZE: usize = 4;
#[allow(dead_code)]
pub(crate) const MSG_TYPE_SIZE: usize = 1;

#[allow(clippy::cast_possible_truncation)]
pub(crate) fn write_frame(buf: &mut Vec<u8>, msg_type: u8, payload: &[u8]) {
    let frame_len = (MSG_TYPE_SIZE + payload.len()) as u32;
    buf.extend_from_slice(&frame_len.to_be_bytes());
    buf.push(msg_type);
    buf.extend_from_slice(payload);
}

#[allow(clippy::cast_possible_truncation)]
pub(crate) fn encode_hello(instance_id: &str) -> Vec<u8> {
    let id_bytes = instance_id.as_bytes();
    let mut payload = Vec::with_capacity(2 + id_bytes.len());
    BE::write_u16(&mut payload, id_bytes.len() as u16);
    payload.extend_from_slice(id_bytes);
    payload
}

pub(crate) struct DecodedHello {
    pub instance_id: String,
}

pub(crate) fn decode_hello(buf: &[u8]) -> anyhow::Result<DecodedHello> {
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

#[allow(clippy::cast_possible_truncation)]
pub(crate) fn encode_torrents(info_hashes: &[&[u8; 20]]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(2 + info_hashes.len() * 20);
    BE::write_u16(&mut payload, info_hashes.len() as u16);
    for ih in info_hashes {
        payload.extend_from_slice(&ih[..]);
    }
    payload
}

pub(crate) fn encode_single_info_hash(info_hash: &[u8; 20]) -> Vec<u8> {
    info_hash.to_vec()
}

#[allow(dead_code)]
pub(crate) fn decode_msg_type(buf: &[u8]) -> anyhow::Result<u8> {
    if buf.is_empty() {
        bail!("empty message");
    }
    Ok(buf[0])
}

pub(crate) fn decode_torrents(buf: &[u8]) -> anyhow::Result<Vec<[u8; 20]>> {
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

pub(crate) fn decode_single_info_hash(buf: &[u8]) -> anyhow::Result<[u8; 20]> {
    if buf.len() < 20 {
        bail!("info hash payload too short");
    }
    let mut ih = [0u8; 20];
    ih.copy_from_slice(&buf[..20]);
    Ok(ih)
}

pub(crate) fn encode_socket_addr(addr: SocketAddr) -> Vec<u8> {
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

pub(crate) fn decode_socket_addr(buf: &[u8]) -> anyhow::Result<SocketAddr> {
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

#[allow(clippy::cast_possible_truncation)]
pub(crate) fn encode_forward_metadata(
    peer_addr: SocketAddr,
    handshake: &[u8],
    extra: &[u8],
) -> Vec<u8> {
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

pub(crate) struct ForwardMetadata {
    pub peer_addr: SocketAddr,
    pub handshake: Vec<u8>,
    pub extra: Vec<u8>,
}

pub(crate) async fn read_forward_metadata<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> anyhow::Result<ForwardMetadata> {
    use tokio::io::AsyncReadExt;
    async fn read_u32_be<R: tokio::io::AsyncRead + Unpin>(r: &mut R) -> anyhow::Result<u32> {
        let mut buf = [0u8; 4];
        r.read_exact(&mut buf).await.context("reading u32")?;
        Ok(u32::from_be_bytes(buf))
    }
    async fn read_blob<R: tokio::io::AsyncRead + Unpin>(
        r: &mut R,
        len: usize,
    ) -> anyhow::Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        r.read_exact(&mut buf).await.context("reading blob")?;
        Ok(buf)
    }

    let addr_len = read_u32_be(reader).await? as usize;
    let addr_bytes = read_blob(reader, addr_len).await?;
    let peer_addr = decode_socket_addr(&addr_bytes)?;

    let hs_len = read_u32_be(reader).await? as usize;
    let handshake = read_blob(reader, hs_len).await?;

    let extra_len = read_u32_be(reader).await? as usize;
    let extra = read_blob(reader, extra_len).await?;

    Ok(ForwardMetadata {
        peer_addr,
        handshake,
        extra,
    })
}

#[allow(unused)]
pub(crate) fn ip_addr_is_localhost(addr: &IpAddr) -> bool {
    match addr {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => v6.is_loopback(),
    }
}
