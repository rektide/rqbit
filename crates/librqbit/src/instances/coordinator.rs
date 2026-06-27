use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, bail};
use librqbit_core::spawn_utils::spawn_with_cancel;
use parking_lot::RwLock;
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::instances::protocol::{
    self, MODE_CONTROL, MODE_FORWARD_TCP, MSG_GOODBYE, MSG_HELLO, MSG_I_HAS, MSG_TORRENTS_ADDED,
    MSG_TORRENTS_REMOVED, MSG_WHO_HAS,
};
use crate::instances::routing::RoutingTable;
use crate::instances::{ForwardHandler, InstanceId};
use crate::type_aliases::{BoxAsyncReadVectored, BoxAsyncWrite};
use crate::vectored_traits::AsyncReadVectoredIntoCompat;

const DISCOVERY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
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
        });

        coord.spawn_accept_loop(listener);
        coord.spawn_discovery_loop();

        Ok(coord)
    }

    pub fn shutdown(&self) {
        self.cancellation.cancel();
        let mut goodbye = Vec::new();
        protocol::write_frame(&mut goodbye, MSG_GOODBYE, &[]);
        let senders: Vec<_> = self
            .peers
            .read()
            .values()
            .map(|p| p.sender.clone())
            .collect();
        for sender in senders {
            let _ = sender.send(goodbye.clone());
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
                let mut interval = tokio::time::interval(DISCOVERY_INTERVAL);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        biased;
                        _ = coord.cancellation.cancelled() => break,
                        _ = interval.tick() => {
                            coord.discover_peers().await;
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
        protocol::write_frame(
            &mut buf,
            MSG_HELLO,
            &protocol::encode_hello(&self.instance_id),
        );
        let local_torrents = self.local_torrents.read().clone();
        if !local_torrents.is_empty() {
            let refs: Vec<&[u8; 20]> = local_torrents.iter().collect();
            protocol::write_frame(
                &mut buf,
                MSG_TORRENTS_ADDED,
                &protocol::encode_torrents(&refs),
            );
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
            coord.peer_reader_task_wrapper(read_half, peer_id),
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
    ) {
        use tokio::io::AsyncReadExt;
        let mut len_buf = [0u8; 4];
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
                    if let Err(e) = self.handle_control_message(&peer_id, frame[0], &frame[1..]).await {
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
    ) -> anyhow::Result<()> {
        self.peer_reader_task(stream, peer_id).await;
        Ok(())
    }

    async fn handle_control_message(
        &self,
        peer_id: &str,
        msg_type: u8,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        match msg_type {
            MSG_HELLO => {
                let hello = protocol::decode_hello(payload)?;
                debug!(peer = %peer_id, id = %hello.instance_id, "received Hello");
            }
            MSG_TORRENTS_ADDED => {
                let torrents = protocol::decode_torrents(payload)?;
                debug!(peer = %peer_id, count = torrents.len(), "torrents added");
                self.routing.add_many(&torrents, &peer_id.to_string());
            }
            MSG_TORRENTS_REMOVED => {
                let torrents = protocol::decode_torrents(payload)?;
                debug!(peer = %peer_id, count = torrents.len(), "torrents removed");
                for ih in &torrents {
                    self.routing.remove(ih, &peer_id.to_string());
                }
            }
            MSG_WHO_HAS => {
                let ih = protocol::decode_single_info_hash(payload)?;
                if self.local_torrents.read().contains(&ih) {
                    self.send_to_peer(peer_id, MSG_I_HAS, &protocol::encode_single_info_hash(&ih))?;
                }
            }
            MSG_I_HAS => {
                let ih = protocol::decode_single_info_hash(payload)?;
                self.routing.add(&ih, peer_id.to_string());
            }
            MSG_GOODBYE => {
                debug!(peer = %peer_id, "received Goodbye");
                self.remove_peer(peer_id);
            }
            other => warn!(msg_type = other, "unknown control message"),
        }
        Ok(())
    }

    fn send_to_peer(&self, peer_id: &str, msg_type: u8, payload: &[u8]) -> anyhow::Result<()> {
        let peers = self.peers.read();
        let peer = peers
            .get(peer_id)
            .with_context(|| format!("peer {peer_id} not connected"))?;
        let mut frame = Vec::new();
        protocol::write_frame(&mut frame, msg_type, payload);
        peer.sender.send(frame)?;
        Ok(())
    }

    fn broadcast(&self, msg_type: u8, payload: &[u8]) {
        let mut frame = Vec::new();
        protocol::write_frame(&mut frame, msg_type, payload);
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
        self.broadcast(MSG_TORRENTS_ADDED, &protocol::encode_torrents(&[info_hash]));
    }

    pub fn unannounce_torrent(&self, info_hash: &[u8; 20]) {
        self.local_torrents.write().retain(|ih| ih != info_hash);
        self.routing.remove(info_hash, &self.instance_id);
        self.broadcast(
            MSG_TORRENTS_REMOVED,
            &protocol::encode_torrents(&[info_hash]),
        );
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
        self.broadcast(MSG_WHO_HAS, &protocol::encode_single_info_hash(info_hash));
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
        if frame[0] != MSG_HELLO {
            bail!("expected Hello, got msg_type {}", frame[0]);
        }
        let hello = protocol::decode_hello(&frame[1..])?;
        let peer_id = hello.instance_id;

        let mut hello_back = Vec::new();
        protocol::write_frame(
            &mut hello_back,
            MSG_HELLO,
            &protocol::encode_hello(&self.instance_id),
        );
        let torrents = self.local_torrents.read().clone();
        if !torrents.is_empty() {
            let refs: Vec<&[u8; 20]> = torrents.iter().collect();
            protocol::write_frame(
                &mut hello_back,
                MSG_TORRENTS_ADDED,
                &protocol::encode_torrents(&refs),
            );
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

        debug!(peer = %peer_id, "incoming control connection established");
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
        coord.peer_reader_task(read_half, peer_id).await;
        Ok(())
    }

    async fn handle_incoming_forward(self: Arc<Self>, stream: UnixStream) {
        let mut stream = stream;
        let metadata = match protocol::read_forward_metadata(&mut stream).await {
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
            peer_addr = ?metadata.peer_addr,
            hs_len = metadata.handshake.len(),
            extra_len = metadata.extra.len(),
            "received forwarded TCP connection"
        );

        let mut prefix = metadata.handshake.clone();
        prefix.extend_from_slice(&metadata.extra);

        let (read_half, write_half) = stream.into_split();
        let reader = PrefixedReader::new(prefix, read_half);

        let boxed_reader: BoxAsyncReadVectored = Box::new(reader.into_vectored_compat());
        let boxed_writer: BoxAsyncWrite = Box::new(write_half);

        handler
            .handle_forwarded(metadata.peer_addr, boxed_reader, boxed_writer)
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
        let metadata = protocol::encode_forward_metadata(peer_addr, handshake_bytes, extra_bytes);
        stream.write_all(&metadata).await?;

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
