// Project:   clickhouse-dfe
// File:      src/tcp/mock.rs
// Purpose:   Server-side wire script shared by the TCP unit tests
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Server halves of the native protocol, scripted over a loopback
//! `TcpStream`. Test-only -- no ClickHouse server is involved.

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::native::io::{ClickHouseRead, ClickHouseWrite};
use crate::tcp::protocol::{ClientPacketId, DBMS_TCP_PROTOCOL_VERSION, ServerPacketId};
use crate::tcp::writer::CLIENT_NAME;

/// Consume the client Hello (8 fields), emit a ServerHello, consume the
/// addendum quota key -- the smallest server
/// [`crate::tcp::connect::open_handshaken`] accepts. The advertised
/// revision is above the timezone, display-name, version-patch and
/// addendum gates, so all four of those fields are on the wire.
///
/// The three transport-invariant fields are asserted here rather than
/// discarded: a real server that read a shifted Hello would fail the
/// connection, and a mock that replies regardless cannot. Database, user
/// and password vary per caller, so they are only read for framing.
pub(crate) async fn serve_one_handshake(sock: &mut TcpStream) {
    assert_eq!(
        sock.read_var_uint().await.unwrap(),
        ClientPacketId::Hello as u64,
        "client must open with a Hello packet"
    );
    assert_eq!(sock.read_utf8_string().await.unwrap(), CLIENT_NAME);
    let _major = sock.read_var_uint().await.unwrap();
    let _minor = sock.read_var_uint().await.unwrap();
    assert_eq!(
        sock.read_var_uint().await.unwrap(),
        DBMS_TCP_PROTOCOL_VERSION,
        "client must advertise the revision this crate is built against"
    );
    let _database = sock.read_utf8_string().await.unwrap();
    let _user = sock.read_utf8_string().await.unwrap();
    let _password = sock.read_utf8_string().await.unwrap();

    let _ = sock.write_var_uint(ServerPacketId::Hello as u64).await;
    let _ = sock.write_string("mock-ch".as_bytes()).await;
    let _ = sock.write_var_uint(25).await;
    let _ = sock.write_var_uint(4).await;
    let _ = sock.write_var_uint(DBMS_TCP_PROTOCOL_VERSION).await;
    let _ = sock.write_string("Etc/UTC".as_bytes()).await;
    let _ = sock.write_string("mock".as_bytes()).await;
    let _ = sock.write_var_uint(7).await;
    let _ = sock.flush().await;

    let _ = sock.read_utf8_string().await;
}

/// Write a Data packet with `num_rows = 0` and the supplied
/// `(name, type_name)` pairs as the schema body -- the header block a
/// server emits before any rows, and the schema echo an INSERT gets back.
pub(crate) async fn write_schema_block(server: &mut TcpStream, columns: &[(&str, &str)]) {
    write_data_header(server, columns.len() as u64, 0).await;
    for (name, ty) in columns {
        server.write_string(name.as_bytes()).await.unwrap();
        server.write_string(ty.as_bytes()).await.unwrap();
        // Custom-serialization flag; the reader expects it above the gate.
        AsyncWriteExt::write_u8(server, 0).await.unwrap();
    }
    server.flush().await.unwrap();
}

/// Write a Data packet carrying one `UInt64` column named `n`.
pub(crate) async fn write_uint64_payload_block(server: &mut TcpStream, values: &[u64]) {
    write_data_header(server, 1, values.len() as u64).await;
    server.write_string(b"n").await.unwrap();
    server.write_string(b"UInt64").await.unwrap();
    AsyncWriteExt::write_u8(server, 0).await.unwrap();
    for v in values {
        server.write_u64_le(*v).await.unwrap();
    }
    server.flush().await.unwrap();
}

/// One `String` column per pair; every column supplies the same row count.
pub(crate) async fn write_string_payload_block(
    server: &mut TcpStream,
    columns: &[(&str, &[&str])],
) {
    let rows = columns.first().map_or(0, |(_, values)| values.len());
    write_data_header(server, columns.len() as u64, rows as u64).await;
    for (name, values) in columns {
        server.write_string(name.as_bytes()).await.unwrap();
        server.write_string(b"String").await.unwrap();
        AsyncWriteExt::write_u8(server, 0).await.unwrap();
        for value in *values {
            server.write_string(value.as_bytes()).await.unwrap();
        }
    }
    server.flush().await.unwrap();
}

/// Packet id, table name, block-info field pairs and the
/// `(num_columns, num_rows)` varuint pair that open every Data packet.
async fn write_data_header(server: &mut TcpStream, num_columns: u64, num_rows: u64) {
    server
        .write_var_uint(ServerPacketId::Data as u64)
        .await
        .unwrap();
    server.write_string(b"").await.unwrap();
    server.write_var_uint(1).await.unwrap();
    AsyncWriteExt::write_u8(server, 0).await.unwrap();
    server.write_var_uint(2).await.unwrap();
    server.write_i32_le(-1).await.unwrap();
    server.write_var_uint(0).await.unwrap();
    server.write_var_uint(num_columns).await.unwrap();
    server.write_var_uint(num_rows).await.unwrap();
}
