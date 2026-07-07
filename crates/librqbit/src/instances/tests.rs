//! Tests for SCM_RIGHTS fd passing — the riskiest piece of the fd-pass path.
//!
//! These exercise the wire-level round-trip (sendmsg + recvmsg with cmsg),
//! not the higher-level coordinator integration. The patterns here mirror
//! what `forward_tcp_fd` and `handle_incoming_forward_fd` do.

use std::{
    io::IoSliceMut,
    os::unix::io::{AsRawFd, FromRawFd, RawFd},
    time::Duration,
};

use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

/// End-to-end SCM_RIGHTS round-trip: pass a TCP fd from sender to receiver
/// over a Unix socket pair, then read bytes from the received fd to prove
/// the kernel actually wired it up to the same socket.
#[tokio::test]
async fn scm_rights_tcp_fd_round_trip() -> anyhow::Result<()> {
    // A Unix socket pair serves as the IPC channel between two "instances".
    let (unix_sender, unix_receiver) = UnixStream::pair()?;

    // A TCP socket pair gives us a real fd to pass. The listener side is
    // the "peer" — it writes data that the fd-holder should be able to read.
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let addr = listener.local_addr()?;
    let tcp_to_pass = tokio::net::TcpStream::connect(addr).await?;
    let (mut tcp_peer, _peer_addr) = listener.accept().await?;

    // Peer writes data into the kernel buffer *before* we pass the fd.
    // The data sits in the socket's receive queue; whoever has a fd pointing
    // at this socket can read it.
    let payload = b"hello via SCM_RIGHTS";
    tcp_peer.write_all(payload).await?;
    // Don't close the peer side yet — we want the receiver to read.

    // --- Sender side: forward_tcp_fd-style sendmsg ---
    let unix_send_fd = unix_sender.as_raw_fd();
    let tcp_fd_to_send = tcp_to_pass.as_raw_fd();
    let sentinel = [0u8; 1];
    let iov = [std::io::IoSlice::new(&sentinel)];
    let cmsgs = [nix::sys::socket::ControlMessage::ScmRights(&[
        tcp_fd_to_send,
    ])];

    unix_sender.writable().await?;
    let sent = nix::sys::socket::sendmsg::<()>(
        unix_send_fd,
        &iov,
        &cmsgs,
        nix::sys::socket::MsgFlags::empty(),
        None,
    )
    .context("sendmsg with SCM_RIGHTS")?;
    assert_eq!(sent, 1, "sentinel byte should be delivered");

    // Sender drops its TcpStream. Tokio's TcpStream::Drop just closes the fd
    // (no shutdown); the receiver's fd (created by the kernel during
    // SCM_RIGHTS) keeps the socket alive.
    drop(tcp_to_pass);

    // --- Receiver side: handle_incoming_forward_fd-style recvmsg ---
    let unix_recv_fd = unix_receiver.as_raw_fd();
    let mut cmsg_buf = nix::cmsg_space!(RawFd);
    let mut data_buf = [0u8; 1];

    unix_receiver.readable().await?;
    let mut recv_iov = [IoSliceMut::new(&mut data_buf)];
    let msg = nix::sys::socket::recvmsg::<()>(
        unix_recv_fd,
        &mut recv_iov,
        Some(&mut cmsg_buf),
        nix::sys::socket::MsgFlags::empty(),
    )
    .context("recvmsg with SCM_RIGHTS")?;
    assert_eq!(msg.bytes, 1, "sentinel byte should arrive");

    let received_fd: RawFd = msg
        .cmsgs()?
        .find_map(|cmsg| match cmsg {
            nix::sys::socket::ControlMessageOwned::ScmRights(fds) => fds.first().copied(),
            _ => None,
        })
        .context("no SCM_RIGHTS fd in received message")?;

    // Wrap as tokio TcpStream. This is the unsafe step: we're claiming
    // ownership of a raw fd. Safe here because the kernel created this fd for
    // us during SCM_RIGHTS and we're its sole owner.
    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(received_fd) };
    std_stream.set_nonblocking(true)?;
    let mut received_tcp = tokio::net::TcpStream::from_std(std_stream)?;

    // Read the payload. If the received fd actually points to the same
    // socket as tcp_to_pass did, the bytes the peer wrote will come out.
    let mut read_buf = vec![0u8; payload.len()];
    let n = tokio::time::timeout(
        Duration::from_secs(2),
        received_tcp.read_exact(&mut read_buf),
    )
    .await
    .context("timeout reading from received fd")??;
    assert_eq!(n, payload.len());
    assert_eq!(&read_buf[..n], payload, "data should match what peer wrote");

    // Cleanup: drop everything so fds are closed.
    drop(received_tcp);
    drop(tcp_peer);
    drop(unix_sender);
    drop(unix_receiver);
    Ok(())
}

/// Verify the receiver-side WouldBlock loop: if recvmsg returns EAGAIN, the
/// helper should poll readiness via tokio and retry. We simulate by calling
/// recvmsg on an empty socket.
///
/// This is more of a smoke test for the loop structure than a strict
/// behavioral check — if readiness polling was broken, this test would hang.
#[tokio::test]
async fn scm_rights_receiver_handles_eagain() -> anyhow::Result<()> {
    let (mut unix_sender, unix_receiver) = UnixStream::pair()?;

    // Receiver tries to read before anything is sent. This MUST not block
    // forever — it should await readable() and only proceed once data arrives.
    let receiver_task = tokio::spawn(async move {
        let unix_recv_fd = unix_receiver.as_raw_fd();
        let mut cmsg_buf = nix::cmsg_space!(RawFd);
        let mut data_buf = [0u8; 1];

        loop {
            // We don't actually have a guard on readable() here; this mirrors
            // the production loop. If readiness wasn't working, we'd busy-loop.
            unix_receiver.readable().await?;
            let mut recv_iov = [IoSliceMut::new(&mut data_buf)];
            match nix::sys::socket::recvmsg::<()>(
                unix_recv_fd,
                &mut recv_iov,
                Some(&mut cmsg_buf),
                nix::sys::socket::MsgFlags::empty(),
            ) {
                Ok(_) => return Ok::<_, anyhow::Error>(()),
                Err(nix::errno::Errno::EAGAIN) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    });

    // Give the receiver a moment to enter its await. Then send.
    tokio::time::sleep(Duration::from_millis(50)).await;

    unix_sender.write_all(&[42u8]).await?;
    drop(unix_sender);

    tokio::time::timeout(Duration::from_secs(2), receiver_task)
        .await
        .context("receiver task timed out")???;

    Ok(())
}
