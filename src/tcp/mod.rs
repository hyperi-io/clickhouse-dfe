//! ClickHouse TCP transport (native binary protocol, port 9000).
//!
//! Submodules:
//! - `protocol` -- wire constants, packet IDs, server response staging types.
//! - `transport` -- `MaybeTlsStream` plain-or-TLS adapter + buffer sizes.
//! - `client_info` -- `ClientInfo` block emitted inside the Query packet.
//! - `writer` -- client-side packet encoders (Hello, Query, Data, Cancel,
//!   Ping, Addendum) over the `crate::native::io::ClickHouseWrite` trait.
//! - `reader` -- server-side packet decoders (Hello, Data header,
//!   Exception, Progress, ProfileInfo, TableColumns, Pong, EndOfStream,
//!   Log, ProfileEvents, TimezoneUpdate) over the
//!   `crate::native::io::ClickHouseRead` trait.
//! - [`connect`] -- `connect_plain` (TcpStream + TCP_NODELAY + keepalive)
//!   and `open_handshaken` (connect + handshake) entry points.
//! - [`handshake`] -- `HandshakeConfig` + `handshake()` orchestrator
//!   driving send-Hello / recv-ServerHello / send-addendum.
//! - [`connection_actor`] -- `CommandWorker` impl owning the writer half
//!   and a packet-receiver fed by an independent reader sub-task;
//!   `ConnectionHandle` is the cheap-clone send-side.
//! - [`pool`] -- `TcpConnectionManager` + `NativePool` (deadpool managed
//!   pool) with poison-on-error recycle; the only construction site for
//!   `ConnectionHandle`s outside tests.
//!
//! Wire-format primitives (varint, length-prefixed string, fixed-width
//! LE) come from [`crate::native::io`]; this module does not duplicate
//! them. The Native columnar encoder / decoder also lives under
//! [`crate::native`].

// Some protocol staging types exist for wire completeness and have no
// in-tree caller.
#![allow(dead_code)]

pub mod client;
pub mod client_ext;
pub(crate) mod client_info;
pub mod connect;
pub mod connection_actor;
pub mod cursor;
pub mod handshake;
pub mod pool;
pub(crate) mod protocol;
pub(crate) mod reader;
pub mod retry;
pub(crate) mod transport;
pub(crate) mod writer;

// Re-exports for the public TCP API: `HandshakeConfig` drives the
// handshake, `ServerHello` reports what the server advertised, and
// `MaybeTlsStream` is the opaque connected-stream return type.
pub use self::client::TcpClient;
pub use self::client_ext::TcpInsertSession;
pub use self::cursor::TcpRawCursor;
pub use self::handshake::HandshakeConfig;
pub use self::protocol::ServerHello;
pub use self::retry::RetryPolicy;
pub use self::transport::MaybeTlsStream;
