use std::net::SocketAddr;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};

pub(crate) const MODE_CONTROL: u8 = 0x01;
pub(crate) const MODE_FORWARD_TCP: u8 = 0x02;
pub(crate) const MODE_FORWARD_TCP_FD: u8 = 0x03;

// ---------------------------------------------------------------------------
// Typed messages (format-agnostic)
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

/// Metadata for fd-pass forwarding (MODE_FORWARD_TCP_FD).
///
/// Unlike `ForwardTcpMeta`, this carries no handshake/extra bytes: the sender
/// never consumes them (the BT handshake is peeked, not read), so the receiver
/// reads them fresh from the kernel buffer on the passed fd.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ForwardTcpFdMeta {
    pub peer_addr: SocketAddr,
}

// ---------------------------------------------------------------------------
// Framing (format-agnostic)
// ---------------------------------------------------------------------------

#[allow(clippy::cast_possible_truncation)]
pub(crate) fn write_payload_frame(buf: &mut Vec<u8>, payload: &[u8]) {
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(payload);
}

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
    PostcardCodec.decode_forward(&buf)
}

pub(crate) async fn read_forward_fd_metadata<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> anyhow::Result<ForwardTcpFdMeta> {
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 1024 * 1024 {
        bail!("forward-fd metadata too large: {len}");
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    PostcardCodec.decode_forward_fd(&buf)
}

// ---------------------------------------------------------------------------
// WireCodec trait: each format owns its encode/decode/probe logic
// ---------------------------------------------------------------------------

/// A wire format codec that can probe, decode, and encode messages.
///
/// Each serialization system (postcard, JSON/varlink, CBOR, ...) implements
/// this trait as a self-contained unit. The coordinator aggregates codecs
/// into a probe chain for inbound detection, and uses a single codec for
/// outbound encoding.
pub(crate) trait WireCodec: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    /// Attempt to recognize and decode a control message.
    /// - `None`: bytes don't match this format, try next codec
    /// - `Some(Ok(msg))`: successfully decoded
    /// - `Some(Err(e))`: format recognized but malformed (don't try others)
    fn try_decode_control(&self, buf: &[u8]) -> Option<anyhow::Result<ControlMessage>>;

    fn encode_control(&self, msg: &ControlMessage) -> Vec<u8>;

    #[allow(dead_code)]
    fn try_decode_forward(&self, buf: &[u8]) -> Option<anyhow::Result<ForwardTcpMeta>>;

    fn encode_forward(&self, meta: &ForwardTcpMeta) -> Vec<u8>;

    fn encode_forward_fd(&self, meta: &ForwardTcpFdMeta) -> Vec<u8>;

    #[allow(dead_code)]
    fn try_decode_forward_fd(&self, buf: &[u8]) -> Option<anyhow::Result<ForwardTcpFdMeta>>;
}

// ---------------------------------------------------------------------------
// Postcard codec (the real implementation)
// ---------------------------------------------------------------------------

pub(crate) struct PostcardCodec;

impl WireCodec for PostcardCodec {
    fn name(&self) -> &'static str {
        "postcard"
    }

    fn try_decode_control(&self, buf: &[u8]) -> Option<anyhow::Result<ControlMessage>> {
        match postcard::from_bytes::<ControlMessage>(buf) {
            Ok(msg) => Some(Ok(msg)),
            Err(_) => None,
        }
    }

    fn encode_control(&self, msg: &ControlMessage) -> Vec<u8> {
        postcard::to_allocvec(msg).expect("postcard serialization infallible for ControlMessage")
    }

    fn try_decode_forward(&self, buf: &[u8]) -> Option<anyhow::Result<ForwardTcpMeta>> {
        match postcard::from_bytes::<ForwardTcpMeta>(buf) {
            Ok(meta) => Some(Ok(meta)),
            Err(_) => None,
        }
    }

    fn encode_forward(&self, meta: &ForwardTcpMeta) -> Vec<u8> {
        postcard::to_allocvec(meta).expect("postcard serialization infallible for ForwardTcpMeta")
    }

    fn encode_forward_fd(&self, meta: &ForwardTcpFdMeta) -> Vec<u8> {
        postcard::to_allocvec(meta).expect("postcard serialization infallible for ForwardTcpFdMeta")
    }

    fn try_decode_forward_fd(&self, buf: &[u8]) -> Option<anyhow::Result<ForwardTcpFdMeta>> {
        match postcard::from_bytes::<ForwardTcpFdMeta>(buf) {
            Ok(meta) => Some(Ok(meta)),
            Err(_) => None,
        }
    }
}

impl PostcardCodec {
    pub(crate) fn decode_forward(&self, buf: &[u8]) -> anyhow::Result<ForwardTcpMeta> {
        postcard::from_bytes(buf).context("postcard decode error")
    }

    pub(crate) fn decode_forward_fd(&self, buf: &[u8]) -> anyhow::Result<ForwardTcpFdMeta> {
        postcard::from_bytes(buf).context("postcard decode error")
    }
}

// ---------------------------------------------------------------------------
// JSON/varlink probe (detects, bails with actionable message)
// ---------------------------------------------------------------------------

pub(crate) struct JsonProbeCodec;

impl WireCodec for JsonProbeCodec {
    fn name(&self) -> &'static str {
        "json/varlink"
    }

    fn try_decode_control(&self, buf: &[u8]) -> Option<anyhow::Result<ControlMessage>> {
        if buf.first() == Some(&b'{') {
            Some(Err(anyhow::anyhow!(
                "JSON/varlink wire format detected but not yet supported. \
                 Ensure all rqbit instances use the same build (postcard). \
                 Varlink support tracked in rqbit-reuseport-varlink."
            )))
        } else {
            None
        }
    }

    fn encode_control(&self, _: &ControlMessage) -> Vec<u8> {
        unimplemented!("JSON encoding not supported")
    }

    fn try_decode_forward(&self, buf: &[u8]) -> Option<anyhow::Result<ForwardTcpMeta>> {
        if buf.first() == Some(&b'{') {
            Some(Err(anyhow::anyhow!(
                "JSON/varlink forward metadata not yet supported"
            )))
        } else {
            None
        }
    }

    fn encode_forward(&self, _: &ForwardTcpMeta) -> Vec<u8> {
        unimplemented!("JSON encoding not supported")
    }

    fn encode_forward_fd(&self, _: &ForwardTcpFdMeta) -> Vec<u8> {
        unimplemented!("JSON encoding not supported")
    }

    fn try_decode_forward_fd(&self, buf: &[u8]) -> Option<anyhow::Result<ForwardTcpFdMeta>> {
        if buf.first() == Some(&b'{') {
            Some(Err(anyhow::anyhow!(
                "JSON/varlink forward-fd metadata not yet supported"
            )))
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Codec chain construction + probing
// ---------------------------------------------------------------------------

/// codecs tried in order for inbound message detection.
pub(crate) fn default_inbound_codecs() -> Vec<Box<dyn WireCodec>> {
    vec![Box::new(PostcardCodec), Box::new(JsonProbeCodec)]
}

/// Codec used for outbound encoding.
pub(crate) fn default_outbound_codec() -> Box<dyn WireCodec> {
    Box::new(PostcardCodec)
}

/// Try each codec in order; first successful decode wins.
pub(crate) fn probe_decode_control(
    buf: &[u8],
    codecs: &[Box<dyn WireCodec>],
) -> anyhow::Result<(usize, ControlMessage)> {
    for (i, codec) in codecs.iter().enumerate() {
        match codec.try_decode_control(buf) {
            Some(Ok(msg)) => return Ok((i, msg)),
            Some(Err(e)) => return Err(e),
            None => continue,
        }
    }
    bail!(
        "unrecognized wire format (tried: {})",
        codecs
            .iter()
            .map(|c| c.name())
            .collect::<Vec<_>>()
            .join(", ")
    );
}
