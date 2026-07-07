use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, bail};
use librqbit_core::spawn_utils::spawn_with_cancel;
use notify::Watcher;
use parking_lot::RwLock;
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::instances::protocol::{
    self, ControlMessage, ForwardTcpFdMeta, ForwardTcpMeta, MODE_CONTROL, MODE_FORWARD_TCP,
    MODE_FORWARD_TCP_FD, WireCodec, default_inbound_codecs, default_outbound_codec,
    probe_decode_control,
};
use crate::instances::routing::RoutingTable;
use crate::instances::{ForwardHandler, ForwardMode, InstanceId};
use crate::type_aliases::{BoxAsyncReadVectored, BoxAsyncWrite};
use crate::vectored_traits::AsyncReadVectoredIntoCompat;

const DISCOVERY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
const DISCOVERY_FALLBACK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const MAX_FRAME_SIZE: usize = 1024 * 1024;

struct PeerHandle {
    socket_path: PathBuf,
    sender: mpsc::UnboundedSender<Vec<u8>>,
}

pub struct InstanceCoordinator {
    instance_id: InstanceId,
    socket_path: PathBuf,
    socket_dir: PathBuf,
    routing: RoutingTable,
    peers: RwLock<HashMap<InstanceId, PeerHandle>>,
    forward_handler: RwLock<Option<Arc<dyn ForwardHandler>>>,
    cancellation: CancellationToken,
    local_torrents: RwLock<Vec<[u8; 20]>>,
    inbound_codecs: Vec<Box<dyn WireCodec>>,
    outbound_codec: Box<dyn WireCodec>,
    forward_mode: RwLock<ForwardMode>,
}

impl InstanceCoordinator {
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn set_forward_handler(&self, handler: Arc<dyn ForwardHandler>) {
        *self.forward_handler.write() = Some(handler);
    }

    /// Current outgoing-forward strategy (fd-pass vs stream-proxy).
    pub fn forward_mode(&self) -> ForwardMode {
        *self.forward_mode.read()
    }

    /// Set the outgoing-forward strategy. Receiver is always mode-agnostic.
    pub fn set_forward_mode(&self, mode: ForwardMode) {
        *self.forward_mode.write() = mode;
    }

    pub async fn start() -> anyhow::Result<Arc<Self>> {
        let instance_id = generate_instance_id();
        let socket_dir = get_socket_dir()?;
        std::fs::create_dir_all(&socket_dir)
            .with_context(|| format!("creating socket dir {:?}", socket_dir))?;

        cleanup_stale_sockets(&socket_dir);

        let socket_path = socket_dir.join(format!("{instance_id}.sock"));
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("binding {:?}", socket_path))?;

        info!(%instance_id, path = ?socket_path, "instance coordinator started");

        let coord = Arc::new(Self {
            instance_id: instance_id.clone(),
            socket_path: socket_path.clone(),
            socket_dir: socket_dir.clone(),
            routing: RoutingTable::new(),
            peers: RwLock::new(HashMap::new()),
            forward_handler: RwLock::new(None),
            cancellation: CancellationToken::new(),
            local_torrents: RwLock::new(Vec::new()),
            inbound_codecs: default_inbound_codecs(),
            outbound_codec: default_outbound_codec(),
            forward_mode: RwLock::new(ForwardMode::default()),
        });

        coord.spawn_accept_loop(listener);
        coord.spawn_discovery_loop();

        Ok(coord)
    }

    pub fn shutdown(&self) {
        self.cancellation.cancel();
        let goodbye_payload = self.outbound_codec.encode_control(&ControlMessage::Goodbye);
        let mut frame = Vec::new();
        protocol::write_payload_frame(&mut frame, &goodbye_payload);
        let senders: Vec<_> = self
            .peers
            .read()
            .values()
            .map(|p| p.sender.clone())
            .collect();
        for sender in senders {
            let _ = sender.send(frame.clone());
        }
        let _ = std::fs::remove_file(&self.socket_path);
    }

    fn spawn_accept_loop(self: &Arc<Self>, listener: UnixListener) {
        let coord = self.clone();
        let cancel = self.cancellation.clone();
        spawn_with_cancel(
            tracing::debug_span!("instance_accept"),
            "instance_accept",
            cancel,
            async move {
                loop {
                    tokio::select! {
                        biased;
                        accept = listener.accept() => {
                            let (stream, _) = accept?;
                            let coord = coord.clone();
                            spawn_with_cancel(
                                tracing::debug_span!("instance_conn"),
                                "instance_conn",
                                coord.cancellation.clone(),
                                coord.handle_incoming_connection(stream),
                            );
                        }
                        else => break,
                    }
                }
                Ok::<(), anyhow::Error>(())
            },
        );
    }

    fn spawn_discovery_loop(self: &Arc<Self>) {
        let coord = self.clone();
        let cancel = self.cancellation.clone();
        spawn_with_cancel(
            tracing::debug_span!("instance_discovery"),
            "instance_discovery",
            cancel,
            async move {
                coord.discover_peers().await;

                let (event_tx, mut event_rx) = mpsc::unbounded_channel::<()>();
                let watcher: Option<notify::RecommendedWatcher> = {
                    let event_tx = event_tx.clone();
                    match notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
                        let Ok(ev) = res else {
                            return;
                        };
                        if ev
                            .paths
                            .iter()
                            .any(|p| p.extension().is_some_and(|e| e == "sock"))
                        {
                            let _ = event_tx.send(());
                        }
                    }) {
                        Ok(mut w) => {
                            match w.watch(&coord.socket_dir, notify::RecursiveMode::NonRecursive) {
                                Ok(()) => {
                                    debug!(dir = ?coord.socket_dir, "watching socket dir for events");
                                    Some(w)
                                }
                                Err(e) => {
                                    warn!(
                                        error = %e,
                                        dir = ?coord.socket_dir,
                                        "failed to watch socket dir; poll fallback only"
                                    );
                                    None
                                }
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to create notify watcher; poll fallback only");
                            None
                        }
                    }
                };
                drop(event_tx);
                let _watcher = watcher;
                let watcher_alive_at_start = _watcher.is_some();

                let mut interval = tokio::time::interval(if watcher_alive_at_start {
                    DISCOVERY_INTERVAL
                } else {
                    DISCOVERY_FALLBACK_INTERVAL
                });
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut events_alive = watcher_alive_at_start;
                loop {
                    tokio::select! {
                        biased;
                        _ = coord.cancellation.cancelled() => break,
                        _ = interval.tick() => {
                            coord.discover_peers().await;
                        }
                        msg = event_rx.recv(), if events_alive => {
                            match msg {
                                Some(()) => {
                                    coord.discover_peers().await;
                                }
                                None => {
                                    warn!(
                                        "notify watcher stream closed; falling back to {}s poll",
                                        DISCOVERY_FALLBACK_INTERVAL.as_secs()
                                    );
                                    events_alive = false;
                                    interval = tokio::time::interval(DISCOVERY_FALLBACK_INTERVAL);
                                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                                }
                            }
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            },
        );
    }

    async fn discover_peers(self: &Arc<Self>) {
        let entries = match std::fs::read_dir(&self.socket_dir) {
            Ok(e) => e,
            Err(_) => return,
        };

        let mut found: Vec<(InstanceId, PathBuf)> = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && let Some(id) = name.strip_suffix(".sock")
                && id != self.instance_id
            {
                found.push((id.to_string(), path));
            }
        }

        let connected: Vec<InstanceId> = self.peers.read().keys().cloned().collect();

        for (id, path) in &found {
            if !connected.contains(id)
                && let Err(e) = self.connect_to_peer(id, path).await
            {
                debug!(instance_id = %id, error=%e, "failed to connect to peer");
            }
        }

        for id in &connected {
            if !found.iter().any(|(d, _)| d == id) {
                debug!(instance_id = %id, "peer instance disappeared");
                self.remove_peer(id);
            }
        }
    }

    async fn connect_to_peer(
        self: &Arc<Self>,
        instance_id: &str,
        socket_path: &Path,
    ) -> anyhow::Result<()> {
        let mut stream = UnixStream::connect(socket_path)
            .await
            .with_context(|| format!("connecting to {:?}", socket_path))?;

        let mut buf = Vec::new();
        buf.push(MODE_CONTROL);
        let hello_payload = self.outbound_codec.encode_control(&ControlMessage::Hello {
            instance_id: self.instance_id.clone(),
        });
        protocol::write_payload_frame(&mut buf, &hello_payload);

        let local_torrents = self.local_torrents.read().clone();
        if !local_torrents.is_empty() {
            let added_payload =
                self.outbound_codec
                    .encode_control(&ControlMessage::TorrentsAdded {
                        info_hashes: local_torrents,
                    });
            protocol::write_payload_frame(&mut buf, &added_payload);
        }

        stream.writable().await?;
        stream.write_all(&buf).await?;

        let (sender, receiver) = mpsc::unbounded_channel();
        self.peers.write().insert(
            instance_id.to_string(),
            PeerHandle {
                socket_path: socket_path.to_path_buf(),
                sender,
            },
        );

        let coord = self.clone();
        let peer_id = instance_id.to_string();
        let (read_half, write_half) = stream.into_split();
        spawn_with_cancel(
            tracing::debug_span!("peer_write", peer = %peer_id),
            "peer_write",
            coord.cancellation.clone(),
            coord
                .clone()
                .peer_writer_task(write_half, receiver, peer_id.clone()),
        );
        spawn_with_cancel(
            tracing::debug_span!("peer_read", peer = %peer_id),
            "peer_read",
            coord.cancellation.clone(),
            coord.peer_reader_task_wrapper(read_half, peer_id, 0),
        );

        debug!(instance_id = %instance_id, "connected to peer instance");
        Ok(())
    }

    async fn peer_writer_task(
        self: Arc<Self>,
        mut stream: tokio::net::unix::OwnedWriteHalf,
        mut receiver: mpsc::UnboundedReceiver<Vec<u8>>,
        peer_id: InstanceId,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => break,
                msg = receiver.recv() => {
                    match msg {
                        Some(frame) => stream.write_all(&frame).await?,
                        None => break,
                    }
                }
            }
        }
        let _ = stream.shutdown().await;
        trace!(peer = %peer_id, "peer writer done");
        Ok(())
    }

    async fn peer_reader_task(
        self: Arc<Self>,
        mut stream: tokio::net::unix::OwnedReadHalf,
        peer_id: InstanceId,
        codec_idx: usize,
    ) {
        use tokio::io::AsyncReadExt;
        let mut len_buf = [0u8; 4];
        let codec = &self.inbound_codecs[codec_idx];
        loop {
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => break,
                r = stream.read_exact(&mut len_buf) => {
                    if let Err(e) = r {
                        if e.kind() != std::io::ErrorKind::UnexpectedEof {
                            debug!(peer = %peer_id, error=%e, "peer reader error");
                        }
                        break;
                    }
                    let frame_len = u32::from_be_bytes(len_buf) as usize;
                    if frame_len == 0 || frame_len > MAX_FRAME_SIZE {
                        warn!(peer = %peer_id, frame_len, "invalid frame length");
                        break;
                    }
                    let mut frame = vec![0u8; frame_len];
                    if let Err(e) = stream.read_exact(&mut frame).await {
                        debug!(peer = %peer_id, error=%e, "error reading frame");
                        break;
                    }
                    let msg = match codec.try_decode_control(&frame) {
                        Some(Ok(msg)) => msg,
                        Some(Err(e)) => {
                            warn!(peer = %peer_id, error=%e, "error decoding control message");
                            break;
                        }
                        None => {
                            warn!(peer = %peer_id, "codec {} returned None for subsequent frame", codec.name());
                            break;
                        }
                    };
                    if let Err(e) = self.handle_control_message(&peer_id, msg).await {
                        warn!(peer = %peer_id, error=%e, "error handling control message");
                    }
                }
            }
        }
        trace!(peer = %peer_id, "peer reader done");
        self.remove_peer(&peer_id);
    }

    async fn peer_reader_task_wrapper(
        self: Arc<Self>,
        stream: tokio::net::unix::OwnedReadHalf,
        peer_id: InstanceId,
        codec_idx: usize,
    ) -> anyhow::Result<()> {
        self.peer_reader_task(stream, peer_id, codec_idx).await;
        Ok(())
    }

    async fn handle_control_message(
        &self,
        peer_id: &str,
        msg: ControlMessage,
    ) -> anyhow::Result<()> {
        match &msg {
            ControlMessage::Hello { instance_id } => {
                debug!(peer = %peer_id, id = %instance_id, "received Hello");
            }
            ControlMessage::TorrentsAdded { info_hashes } => {
                debug!(peer = %peer_id, count = info_hashes.len(), "torrents added");
                self.routing.add_many(info_hashes, &peer_id.to_string());
            }
            ControlMessage::TorrentsRemoved { info_hashes } => {
                debug!(peer = %peer_id, count = info_hashes.len(), "torrents removed");
                for ih in info_hashes {
                    self.routing.remove(ih, &peer_id.to_string());
                }
            }
            ControlMessage::WhoHas { info_hash } => {
                if self.local_torrents.read().contains(info_hash) {
                    self.send_to_peer(
                        peer_id,
                        &ControlMessage::IHas {
                            info_hash: *info_hash,
                        },
                    )?;
                }
            }
            ControlMessage::IHas { info_hash } => {
                self.routing.add(info_hash, peer_id.to_string());
            }
            ControlMessage::Goodbye => {
                debug!(peer = %peer_id, "received Goodbye");
                self.remove_peer(peer_id);
            }
        }
        Ok(())
    }

    fn send_to_peer(&self, peer_id: &str, msg: &ControlMessage) -> anyhow::Result<()> {
        let peers = self.peers.read();
        let peer = peers
            .get(peer_id)
            .with_context(|| format!("peer {peer_id} not connected"))?;
        let payload = self.outbound_codec.encode_control(msg);
        let mut frame = Vec::new();
        protocol::write_payload_frame(&mut frame, &payload);
        peer.sender.send(frame)?;
        Ok(())
    }

    fn broadcast(&self, msg: &ControlMessage) {
        let payload = self.outbound_codec.encode_control(msg);
        let mut frame = Vec::new();
        protocol::write_payload_frame(&mut frame, &payload);
        for peer in self.peers.read().values() {
            let _ = peer.sender.send(frame.clone());
        }
    }

    fn remove_peer(&self, instance_id: &str) {
        if self.peers.write().remove(instance_id).is_some() {
            self.routing.remove_instance(&instance_id.to_string());
            debug!(instance_id, "removed peer instance");
        }
    }

    pub fn announce_torrent(&self, info_hash: &[u8; 20]) {
        self.local_torrents.write().push(*info_hash);
        self.routing.add(info_hash, self.instance_id.clone());
        self.broadcast(&ControlMessage::TorrentsAdded {
            info_hashes: vec![*info_hash],
        });
    }

    pub fn unannounce_torrent(&self, info_hash: &[u8; 20]) {
        self.local_torrents.write().retain(|ih| ih != info_hash);
        self.routing.remove(info_hash, &self.instance_id);
        self.broadcast(&ControlMessage::TorrentsRemoved {
            info_hashes: vec![*info_hash],
        });
    }

    pub fn lookup(&self, info_hash: &[u8; 20]) -> Option<InstanceId> {
        match self.routing.lookup(info_hash) {
            Some(id) if id != self.instance_id => Some(id),
            _ => None,
        }
    }

    pub fn who_has(&self, info_hash: &[u8; 20]) -> Option<InstanceId> {
        if let Some(id) = self.lookup(info_hash) {
            return Some(id);
        }
        self.broadcast(&ControlMessage::WhoHas {
            info_hash: *info_hash,
        });
        None
    }
    async fn handle_incoming_connection(
        self: Arc<Self>,
        mut stream: UnixStream,
    ) -> anyhow::Result<()> {
        use tokio::io::AsyncReadExt;
        let mut mode_buf = [0u8; 1];
        stream.read_exact(&mut mode_buf).await?;
        match mode_buf[0] {
            MODE_CONTROL => self.handle_incoming_control(stream).await,
            MODE_FORWARD_TCP => {
                self.handle_incoming_forward(stream).await;
                Ok(())
            }
            MODE_FORWARD_TCP_FD => self.handle_incoming_forward_fd(stream).await,
            other => {
                warn!(mode = other, "unknown connection mode");
                Ok(())
            }
        }
    }

    async fn handle_incoming_control(self: Arc<Self>, stream: UnixStream) -> anyhow::Result<()> {
        use tokio::io::AsyncReadExt;
        let mut stream = stream;

        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        if frame_len == 0 || frame_len > MAX_FRAME_SIZE {
            bail!("invalid first frame length {frame_len}");
        }
        let mut frame = vec![0u8; frame_len];
        stream.read_exact(&mut frame).await?;

        let (codec_idx, msg) = probe_decode_control(&frame, &self.inbound_codecs)?;
        let ControlMessage::Hello {
            instance_id: peer_id,
        } = msg
        else {
            bail!("expected Hello as first message");
        };

        let mut hello_back = Vec::new();
        let hello_payload = self.outbound_codec.encode_control(&ControlMessage::Hello {
            instance_id: self.instance_id.clone(),
        });
        protocol::write_payload_frame(&mut hello_back, &hello_payload);

        let torrents = self.local_torrents.read().clone();
        if !torrents.is_empty() {
            let added_payload =
                self.outbound_codec
                    .encode_control(&ControlMessage::TorrentsAdded {
                        info_hashes: torrents,
                    });
            protocol::write_payload_frame(&mut hello_back, &added_payload);
        }
        stream.write_all(&hello_back).await?;

        let (sender, receiver) = mpsc::unbounded_channel();
        self.peers.write().insert(
            peer_id.clone(),
            PeerHandle {
                socket_path: self.socket_dir.join(format!("{peer_id}.sock")),
                sender,
            },
        );

        debug!(peer = %peer_id, codec = %self.inbound_codecs[codec_idx].name(), "incoming control connection established");
        let (read_half, write_half) = stream.into_split();
        let coord = self.clone();
        spawn_with_cancel(
            tracing::debug_span!("peer_write", peer = %peer_id),
            "peer_write",
            coord.cancellation.clone(),
            coord
                .clone()
                .peer_writer_task(write_half, receiver, peer_id.clone()),
        );
        coord.peer_reader_task(read_half, peer_id, codec_idx).await;
        Ok(())
    }

    async fn handle_incoming_forward(self: Arc<Self>, stream: UnixStream) {
        let mut stream = stream;
        let meta = match protocol::read_forward_metadata(&mut stream).await {
            Ok(m) => m,
            Err(e) => {
                warn!(error=%e, "error reading forward metadata");
                return;
            }
        };

        let handler = self.forward_handler.read().clone();
        let Some(handler) = handler else {
            warn!("forward received but no handler registered");
            return;
        };

        debug!(
            peer_addr = ?meta.peer_addr,
            hs_len = meta.handshake.len(),
            extra_len = meta.extra.len(),
            "received forwarded TCP connection"
        );

        let mut prefix = meta.handshake.clone();
        prefix.extend_from_slice(&meta.extra);

        let (read_half, write_half) = stream.into_split();
        let reader = PrefixedReader::new(prefix, read_half);

        let boxed_reader: BoxAsyncReadVectored = Box::new(reader.into_vectored_compat());
        let boxed_writer: BoxAsyncWrite = Box::new(write_half);

        handler
            .handle_forwarded(meta.peer_addr, boxed_reader, boxed_writer)
            .await;
    }

    pub async fn forward_tcp_stream(
        &self,
        instance_id: &str,
        peer_addr: std::net::SocketAddr,
        handshake_bytes: &[u8],
        extra_bytes: &[u8],
        reader: BoxAsyncReadVectored,
        writer: BoxAsyncWrite,
    ) -> anyhow::Result<()> {
        let socket_path = {
            let peers = self.peers.read();
            let peer = peers
                .get(instance_id)
                .with_context(|| format!("peer {instance_id} not connected"))?;
            peer.socket_path.clone()
        };

        let mut stream = UnixStream::connect(&socket_path).await?;
        stream.write_all(&[MODE_FORWARD_TCP]).await?;

        let meta = ForwardTcpMeta {
            peer_addr,
            handshake: handshake_bytes.to_vec(),
            extra: extra_bytes.to_vec(),
        };
        let meta_payload = self.outbound_codec.encode_forward(&meta);
        let mut meta_frame = Vec::new();
        protocol::write_payload_frame(&mut meta_frame, &meta_payload);
        stream.write_all(&meta_frame).await?;

        debug!(
            target_instance = instance_id,
            ?peer_addr,
            "forwarding TCP stream"
        );

        let (mut unix_read, mut unix_write) = stream.into_split();
        let mut reader = reader;
        let mut writer = writer;

        tokio::select! {
            r = tokio::io::copy(&mut reader, &mut unix_write) => {
                trace!(result=?r, "BT->Unix copy done");
            }
            r = tokio::io::copy(&mut unix_read, &mut writer) => {
                trace!(result=?r, "Unix->BT copy done");
            }
        }
        let _ = unix_write.shutdown().await;
        Ok(())
    }

    /// Forward a TCP connection to a peer instance by passing the raw fd via
    /// SCM_RIGHTS. The sender never reads from the stream; bytes stay in the
    /// kernel buffer and the receiver reads them fresh.
    ///
    /// Borrows the TcpStream so the caller can fall back to stream-proxy if
    /// fd-pass fails. On success, the caller must drop the stream promptly:
    /// the kernel has dup'd the fd to the receiver, and our local reference
    /// is redundant.
    pub async fn forward_tcp_fd(
        &self,
        instance_id: &str,
        peer_addr: std::net::SocketAddr,
        tcp: &tokio::net::TcpStream,
    ) -> anyhow::Result<()> {
        use std::os::unix::io::AsRawFd;
        use tokio::io::AsyncWriteExt;

        let socket_path = {
            let peers = self.peers.read();
            let peer = peers
                .get(instance_id)
                .with_context(|| format!("peer {instance_id} not connected"))?;
            peer.socket_path.clone()
        };

        let mut unix = UnixStream::connect(&socket_path).await?;

        // 1. Mode byte + metadata frame via standard async write.
        let meta = ForwardTcpFdMeta { peer_addr };
        let meta_payload = self.outbound_codec.encode_forward_fd(&meta);
        let mut buf = Vec::with_capacity(1 + 4 + meta_payload.len());
        buf.push(MODE_FORWARD_TCP_FD);
        protocol::write_payload_frame(&mut buf, &meta_payload);
        unix.write_all(&buf).await?;

        // 2. sendmsg with SCM_RIGHTS — one syscall, readiness-polled via tokio.
        //    Sentinel byte forces at least one iov entry (some kernels reject
        //    empty-iov sendmsg); receiver discards it.
        let unix_fd = unix.as_raw_fd();
        let tcp_fd = tcp.as_raw_fd();
        let sentinel = [0u8];
        let iov = [std::io::IoSlice::new(&sentinel)];
        let cmsgs = [nix::sys::socket::ControlMessage::ScmRights(&[tcp_fd])];

        loop {
            unix.writable().await?;
            match nix::sys::socket::sendmsg::<()>(
                unix_fd,
                &iov,
                &cmsgs,
                nix::sys::socket::MsgFlags::empty(),
                None,
            ) {
                Ok(_) => break,
                Err(nix::errno::Errno::EAGAIN) => continue,
                Err(e) => return Err(anyhow::Error::new(e).context("sendmsg SCM_RIGHTS")),
            }
        }

        debug!(
            target_instance = instance_id,
            ?peer_addr,
            "forwarded TCP fd via SCM_RIGHTS"
        );
        // Drop the Unix socket: closes our end of the control connection. The
        // receiver has already received the SCM_RIGHTS message and its own fd
        // reference is independent of ours.
        drop(unix);
        Ok(())
    }

    /// Receiver side of fd-pass forwarding: read metadata, recvmsg to collect
    /// the SCM_RIGHTS fd, wrap as tokio TcpStream, hand off to ForwardHandler.
    /// The BT handshake is still in the kernel buffer; the handler reads it
    /// fresh via `check_incoming_connection` — no PrefixedReader needed.
    async fn handle_incoming_forward_fd(
        self: Arc<Self>,
        mut stream: UnixStream,
    ) -> anyhow::Result<()> {
        use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

        let meta = match protocol::read_forward_fd_metadata(&mut stream).await {
            Ok(m) => m,
            Err(e) => {
                warn!(error=%e, "error reading forward-fd metadata");
                return Ok(());
            }
        };

        // recvmsg with cmsg buffer to collect the SCM_RIGHTS fd.
        let unix_fd = stream.as_raw_fd();
        let mut cmsg_buf = nix::cmsg_space!(RawFd);
        let mut sentinel = [0u8; 1];

        let tcp_fd: RawFd = loop {
            stream.readable().await?;
            let mut iov = [std::io::IoSliceMut::new(&mut sentinel)];
            match nix::sys::socket::recvmsg::<()>(
                unix_fd,
                &mut iov,
                Some(&mut cmsg_buf),
                nix::sys::socket::MsgFlags::empty(),
            ) {
                Ok(msg) => {
                    let found = msg.cmsgs()?.find_map(|cmsg| match cmsg {
                        nix::sys::socket::ControlMessageOwned::ScmRights(fds) => {
                            fds.first().copied()
                        }
                        _ => None,
                    });
                    match found {
                        Some(fd) => break fd,
                        None => bail!("forward-fd message arrived without SCM_RIGHTS"),
                    }
                }
                Err(nix::errno::Errno::EAGAIN) => continue,
                Err(e) => return Err(anyhow::Error::new(e).context("recvmsg SCM_RIGHTS")),
            }
        };

        debug!(
            peer_addr = ?meta.peer_addr,
            fd = tcp_fd,
            "received forwarded TCP fd via SCM_RIGHTS"
        );

        // fd → tokio TcpStream. The fd was created by the kernel during
        // SCM_RIGHTS handoff; it points to a valid TCP socket. O_NONBLOCK is
        // inherited from the sender, but we set it explicitly to be safe.
        let std_stream = unsafe { std::net::TcpStream::from_raw_fd(tcp_fd) };
        std_stream.set_nonblocking(true)?;
        let tokio_stream = tokio::net::TcpStream::from_std(std_stream)
            .context("wrapping received fd as tokio TcpStream")?;
        let (read_half, write_half) = tokio_stream.into_split();

        let handler = self.forward_handler.read().clone();
        let Some(handler) = handler else {
            warn!("forward-fd received but no handler registered");
            return Ok(());
        };

        handler
            .handle_forwarded(
                meta.peer_addr,
                Box::new(read_half) as BoxAsyncReadVectored,
                Box::new(write_half) as BoxAsyncWrite,
            )
            .await;

        Ok(())
    }
}

fn generate_instance_id() -> InstanceId {
    let pid = std::process::id();
    let random: u64 = rand::random();
    format!("{pid:08x}-{random:016x}")
}

fn get_socket_dir() -> anyhow::Result<PathBuf> {
    let base = std::env::var("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp"));
    Ok(base.join("rqbit"))
}

fn cleanup_stale_sockets(dir: &Path) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "sock") {
                if let Ok(stream) = std::os::unix::net::UnixStream::connect(&path) {
                    drop(stream);
                    continue;
                }
                trace!(path = ?path, "removing stale socket");
                let _ = std::fs::remove_file(&path);
            }
        }
    }
}

struct PrefixedReader<R> {
    prefix: Vec<u8>,
    pos: usize,
    reader: R,
}

impl<R> PrefixedReader<R> {
    fn new(prefix: Vec<u8>, reader: R) -> Self {
        Self {
            prefix,
            pos: 0,
            reader,
        }
    }
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for PrefixedReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.pos < self.prefix.len() {
            let remaining = &self.prefix[self.pos..];
            let n = buf.remaining().min(remaining.len());
            buf.put_slice(&remaining[..n]);
            self.pos += n;
            return std::task::Poll::Ready(Ok(()));
        }
        std::pin::Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}
