//! TCP connection setup for the ClickHouse native transport.
//!
//! `connect_plain` opens a [`TcpStream`] to the supplied address,
//! enables `TCP_NODELAY`, and configures TCP keepalive via
//! [`socket2::SockRef`]. Defaults are 60s idle / 20s interval / 3
//! retries -- chosen to survive Kubernetes kube-proxy iptables
//! connection-tracking idle timeouts (~30-60s) without producing
//! excessive probe traffic.
//!
//! `connect_tls` (under the `tls` feature) shares that socket setup,
//! then drives the rustls handshake against the supplied SNI.
//!
//! [`open_handshaken`] is the pool's entry point: connect, drive
//! [`crate::tcp::handshake`] against the unsplit stream, return the
//! ready stream plus the negotiated [`ServerHello`]. `split_buffered`
//! then hands the actor its halves.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{BufReader, BufWriter, ReadHalf, WriteHalf};
use tokio::net::TcpStream;

use crate::error::{Error, Result};
use crate::tcp::handshake::{HandshakeConfig, handshake};
use crate::tcp::protocol::ServerHello;
use crate::tcp::transport::{CONN_READ_BUFFER, CONN_WRITE_BUFFER, MaybeTlsStream};

/// Idle time before the first keepalive probe is sent. Matches the
/// rationale in the module docstring.
const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(60);

/// Interval between successive keepalive probes once probing starts.
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);

/// Number of failed probes before the connection is declared dead.
const TCP_KEEPALIVE_RETRIES: u32 = 3;

/// Open a raw [`TcpStream`] to `addr`, then apply the
/// `TCP_NODELAY` + keepalive socket-level setup shared between the
/// plain and TLS paths.
///
/// Factored out so `connect_plain` and `connect_tls` cannot drift
/// on socket-level dials: both call this, both inherit the same
/// keepalive cadence.
async fn connect_socket(addr: SocketAddr) -> Result<TcpStream> {
    let socket = TcpStream::connect(addr).await.map_err(Error::from)?;
    socket.set_nodelay(true).map_err(Error::from)?;

    // socket2 borrows the raw fd / SOCKET from the tokio socket; the
    // mirrored handle is dropped at end of scope without closing the
    // underlying descriptor. Same approach hyper-util uses internally.
    let sock_ref = socket2::SockRef::from(&socket);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(TCP_KEEPALIVE_IDLE)
        .with_interval(TCP_KEEPALIVE_INTERVAL)
        .with_retries(TCP_KEEPALIVE_RETRIES);
    sock_ref
        .set_tcp_keepalive(&keepalive)
        .map_err(Error::from)?;

    Ok(socket)
}

/// Open a plain TCP connection to `addr`, configure `TCP_NODELAY` and
/// keepalive, and return it wrapped in [`MaybeTlsStream::Plain`]. The
/// TLS variant is `connect_tls`, gated on the `tls`
/// feature.
pub(crate) async fn connect_plain(addr: SocketAddr) -> Result<MaybeTlsStream> {
    let socket = connect_socket(addr).await?;
    Ok(MaybeTlsStream::Plain(socket))
}

/// Open a TCP connection, then upgrade it to TLS with rustls and
/// return it wrapped in [`MaybeTlsStream::Tls`].
///
/// `server_name` is the SNI / hostname the server certificate will be
/// validated against. It is independent of the address `addr` so a
/// caller can connect to an IP literal while presenting a
/// hostname-based SNI -- the common pattern when the resolver runs
/// outside the rust client (e.g. a service mesh).
///
/// `config` is the trust the pool resolved once and clones into each
/// connect; see [`crate::tls`] for the resolution rules.
#[cfg(feature = "tls")]
pub(crate) async fn connect_tls(
    addr: SocketAddr,
    server_name: &str,
    config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
) -> Result<MaybeTlsStream> {
    use tokio_rustls::TlsConnector;

    let connector = TlsConnector::from(config);

    let socket = connect_socket(addr).await?;

    // `ServerName::try_from` accepts both DNS names and IP literals;
    // an invalid SNI shape (empty string, illegal characters, etc.)
    // surfaces as a typed Custom error rather than a panic.
    let sni = rustls_pki_types::ServerName::try_from(server_name.to_string())
        .map_err(|e| Error::Custom(format!("tcp: invalid SNI {server_name:?}: {e}")))?;
    let tls = connector.connect(sni, socket).await.map_err(Error::from)?;
    Ok(MaybeTlsStream::Tls(Box::new(tls)))
}

/// Split the stream into buffered read/write halves for the connection
/// actor's long-lived loop, at the [`CONN_READ_BUFFER`] /
/// [`CONN_WRITE_BUFFER`] caps.
///
/// [`tokio::io::split`] rather than `TcpStream::into_split` because
/// `MaybeTlsStream` is a wrapper enum and its TLS variant has no
/// `into_split`; the halves cannot re-merge, which the actor never does.
pub(crate) fn split_buffered(
    stream: MaybeTlsStream,
) -> (
    BufReader<ReadHalf<MaybeTlsStream>>,
    BufWriter<WriteHalf<MaybeTlsStream>>,
) {
    let (r, w) = tokio::io::split(stream);
    (
        BufReader::with_capacity(CONN_READ_BUFFER, r),
        BufWriter::with_capacity(CONN_WRITE_BUFFER, w),
    )
}

/// Connect-side selector: plain TCP versus TLS.
///
/// Separate from [`HandshakeConfig`] because the SNI is a property of
/// the connection, not of the post-connect exchange. The TLS variants
/// are feature-gated on `tls`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ConnectKind {
    /// Plain TCP. No TLS upgrade.
    Plain,
    /// TLS over TCP.
    #[cfg(feature = "tls")]
    Tls {
        /// SNI sent in the ClientHello and the name the server
        /// certificate is validated against.
        server_name: String,
        /// Resolved rustls trust the pool built once and shares (cloned
        /// `Arc`) across reconnects.
        config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
    },
    /// A TLS trust was configured but could not be resolved. The pool
    /// still builds (so we never silently downgrade the transport or
    /// broaden trust), but every connection attempt fails closed here.
    #[cfg(feature = "tls")]
    TlsFailClosed,
}

/// Connect to `addr` and drive the handshake to completion. Returns
/// the ready-to-use stream plus the negotiated [`ServerHello`].
///
/// `kind` selects plain or TLS transport; `handshake()` itself is
/// transport-agnostic over the [`MaybeTlsStream`] adapter.
///
/// # Errors
///
/// [`Error::Custom`] when a configured TLS trust could not be resolved
/// or the SNI is malformed, [`crate::Error::ServerException`] when the
/// server rejects the Hello, and I/O errors from the connect itself.
pub async fn open_handshaken(
    addr: SocketAddr,
    kind: &ConnectKind,
    cfg: &HandshakeConfig,
) -> Result<(MaybeTlsStream, ServerHello)> {
    let mut stream = match kind {
        ConnectKind::Plain => connect_plain(addr).await?,
        #[cfg(feature = "tls")]
        ConnectKind::Tls {
            server_name,
            config,
        } => connect_tls(addr, server_name, config.clone()).await?,
        #[cfg(feature = "tls")]
        ConnectKind::TlsFailClosed => {
            return Err(Error::Custom(
                "tcp: TLS trust was configured but could not be resolved; \
                 refusing to connect with default trust"
                    .into(),
            ));
        }
    };
    let hello = handshake(&mut stream, cfg).await?;
    Ok((stream, hello))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// Nagle would batch small protocol packets behind a 40ms delayed
    /// ACK, and without keepalive a kube-proxy conntrack entry drops an
    /// idle pooled connection with no FIN.
    #[tokio::test]
    async fn connect_plain_sets_nodelay_and_keepalive() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move { listener.accept().await.unwrap().0 });

        let stream = connect_plain(addr).await.expect("loopback connect");
        let MaybeTlsStream::Plain(sock) = &stream else {
            panic!("connect_plain must yield the plain variant");
        };
        assert!(sock.nodelay().unwrap(), "TCP_NODELAY must be set");

        let sock_ref = socket2::SockRef::from(sock);
        assert!(sock_ref.keepalive().unwrap(), "SO_KEEPALIVE must be set");
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            assert_eq!(sock_ref.tcp_keepalive_time().unwrap(), TCP_KEEPALIVE_IDLE);
            assert_eq!(
                sock_ref.tcp_keepalive_interval().unwrap(),
                TCP_KEEPALIVE_INTERVAL
            );
            assert_eq!(
                sock_ref.tcp_keepalive_retries().unwrap(),
                TCP_KEEPALIVE_RETRIES
            );
        }

        let _accepted = accept.await.unwrap();
    }
}
