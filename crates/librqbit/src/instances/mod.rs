mod coordinator;
mod protocol;
mod routing;
#[cfg(test)]
mod tests;

pub use coordinator::InstanceCoordinator;

pub type InstanceId = String;

/// Forwarding strategy for outgoing peer connections that don't belong to us.
///
/// - `FdPass`: hand off the raw TCP fd to the owning instance via SCM_RIGHTS.
///   The forwarder bows out entirely; zero proxy overhead.
/// - `StreamProxy`: connect a Unix socket to the owning instance and pipe bytes
///   bidirectionally via `tokio::io::copy`. Works on any transport; higher overhead.
///
/// The receiver always accepts both modes (dispatched on the mode byte), so a
/// mixed-version cluster operates transparently. This setting only governs what
/// the local instance emits when it has the choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ForwardMode {
    /// SCM_RIGHTS fd passing. Default for new code paths.
    #[default]
    FdPass,
    /// Bidirectional stream proxy over the Unix IPC socket.
    StreamProxy,
}

#[async_trait::async_trait]
pub trait ForwardHandler: Send + Sync + 'static {
    async fn handle_forwarded(
        &self,
        peer_addr: std::net::SocketAddr,
        reader: crate::type_aliases::BoxAsyncReadVectored,
        writer: crate::type_aliases::BoxAsyncWrite,
    );
}
