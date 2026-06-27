mod coordinator;
mod protocol;
mod routing;

pub use coordinator::InstanceCoordinator;

pub type InstanceId = String;

#[async_trait::async_trait]
pub trait ForwardHandler: Send + Sync + 'static {
    async fn handle_forwarded(
        &self,
        peer_addr: std::net::SocketAddr,
        reader: crate::type_aliases::BoxAsyncReadVectored,
        writer: crate::type_aliases::BoxAsyncWrite,
    );
}
