//! ClickHouse TCP transport (native binary protocol, port 9000).
//!
//! Submodules:
//! - `protocol` -- wire constants, packet IDs, server response staging types.
//! - `transport` -- `MaybeTlsStream` plain-or-TLS adapter + buffer sizes.
//! - `client_info` -- `ClientInfo` block emitted inside the Query packet.
//! - `writer` / `reader` -- client-side packet encoders and server-side
//!   decoders over `crate::native::io`.
//! - [`connect`] -- socket setup and `open_handshaken`.
//! - [`handshake`] -- `HandshakeConfig` and the Hello exchange.
//! - [`connection_actor`] -- the socket-owning actor; `ConnectionHandle`
//!   is its cheap-clone send-side.
//! - [`pool`] -- `TcpConnectionManager` + `NativePool`, the only
//!   construction site for `ConnectionHandle`s outside tests.
//! - [`query`] -- `TcpQuery`, the `sql` -> `execute` / `fetch_blocks`
//!   builder over [`client::TcpClient`].
//!
//! Wire-format primitives (varint, length-prefixed string, fixed-width
//! LE) and the Native columnar codec come from [`crate::native`].

pub mod client;
pub mod client_ext;
pub(crate) mod client_info;
pub mod connect;
pub mod connection_actor;
pub mod cursor;
pub mod handshake;
#[cfg(test)]
pub(crate) mod mock;
pub mod pool;
pub(crate) mod protocol;
pub mod query;
pub(crate) mod reader;
pub mod retry;
pub(crate) mod transport;
pub(crate) mod writer;

// Re-exports for the public TCP API: `HandshakeConfig` drives the
// handshake, `ServerHello` reports what the server advertised, and
// `MaybeTlsStream` is the opaque connected-stream return type.
pub use self::client::TcpClient;
pub use self::client_ext::TcpInsertSession;
pub use self::connection_actor::ConnectionHandle;
pub use self::cursor::TcpRawCursor;
pub use self::handshake::HandshakeConfig;
pub use self::protocol::ServerHello;
pub use self::query::TcpQuery;
pub use self::retry::RetryPolicy;
pub use self::transport::MaybeTlsStream;
