//! Client-side TCP packet encoders.
//!
//! Mirrors clickhouse-cpp-client `Client::Impl`:
//!
//! - [`send_hello`] -- `SendHello()` lines 1192-1205.
//! - [`send_addendum`] -- writes quota_key when the server advertises
//!   at least `DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM`.
//! - [`send_query`] -- `SendQuery()` lines ~970-1130, settings serialised
//!   as `(name, flags varint, value)` triples.
//! - [`send_data_block`] / [`send_empty_block`] -- `SendData()` 1172-1181
//!   plus `WriteBlock()` 1140-1170. The caller passes pre-encoded
//!   Native-format `column_bytes`; this layer is transport-only.
//! - [`send_cancel`] -- `SendCancel()` 1006-1009.
//! - [`send_ping`] -- `Ping()` 596-606.
//!
//! Wire primitives come from [`crate::native::io::ClickHouseWrite`]
//! (varint, length-prefixed string) and tokio's `AsyncWriteExt`
//! (fixed-width LE). No new io.rs in this module.

use tokio::io::AsyncWriteExt;

use crate::error::{Error, Result};
use crate::native::io::ClickHouseWrite;
use crate::tcp::client_info::ClientInfo;
use crate::tcp::protocol::{
    ClientPacketId, DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM,
    DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS, DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS,
    DBMS_MIN_REVISION_WITH_BLOCK_INFO, DBMS_MIN_REVISION_WITH_CLIENT_INFO,
    DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET,
    DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS, DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES,
    DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL, DBMS_TCP_PROTOCOL_VERSION,
    QueryProcessingStage,
};

/// Client name advertised in Hello and ClientInfo. cpp-client sends
/// "clickhouse-cpp"; this is the rust-client equivalent. The server
/// records it for `system.query_log` so a distinct value helps operators
/// distinguish rust-client traffic from other drivers.
pub(crate) const CLIENT_NAME: &str = "ClickHouse rust-client";

/// Client major version; parsed to `u64` at each use site.
pub(crate) const CLIENT_VERSION_MAJOR_STR: &str = env!("CARGO_PKG_VERSION_MAJOR");
/// Client minor version; parsed to `u64` at each use site.
pub(crate) const CLIENT_VERSION_MINOR_STR: &str = env!("CARGO_PKG_VERSION_MINOR");

/// `flags` byte on a query-parameter entry. ClickHouse serialises bound
/// parameters through its settings writer, where a name it does not know
/// as a setting carries the CUSTOM flag; matches clickhouse-go's
/// `Parameters.Encode`.
const SETTING_FLAG_CUSTOM: u64 = 2;

#[inline]
fn client_version_major() -> u64 {
    CLIENT_VERSION_MAJOR_STR.parse().unwrap_or(0)
}

#[inline]
fn client_version_minor() -> u64 {
    CLIENT_VERSION_MINOR_STR.parse().unwrap_or(0)
}

/// Send the client Hello packet. Matches cpp-client `SendHello()`
/// lines 1192-1205.
pub(crate) async fn send_hello<W: ClickHouseWrite>(
    w: &mut W,
    database: &str,
    user: &str,
    password: &str,
) -> Result<()> {
    w.write_var_uint(ClientPacketId::Hello as u64).await?;
    w.write_string(CLIENT_NAME.as_bytes()).await?;
    w.write_var_uint(client_version_major()).await?;
    w.write_var_uint(client_version_minor()).await?;
    w.write_var_uint(DBMS_TCP_PROTOCOL_VERSION).await?;
    w.write_string(database.as_bytes()).await?;
    w.write_string(user.as_bytes()).await?;
    w.write_string(password.as_bytes()).await?;
    w.flush().await?;
    Ok(())
}

/// Send the post-Hello addendum, in the field order
/// `TCPHandler::receiveAddendum` reads: quota key, chunked capabilities, then
/// the parallel-replicas protocol version. Older servers expect no addendum
/// bytes at all, so this is a silent no-op there and the caller can invoke it
/// unconditionally.
///
/// The server gates each field on the revision WE advertised, so the effective
/// revision -- the lower of the two -- is what decides: below it the server has
/// no code for the field, above it the server is reading on our value.
pub(crate) async fn send_addendum<W: ClickHouseWrite>(
    w: &mut W,
    server_revision: u64,
    quota_key: &str,
) -> Result<()> {
    let effective = server_revision.min(DBMS_TCP_PROTOCOL_VERSION);
    if effective >= DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM {
        w.write_string(quota_key.as_bytes()).await?;
    }
    if effective >= DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS {
        // Decline chunked framing in both directions, which is the server's
        // own default, so the packet framing stays as it is.
        w.write_string(b"notchunked").await?;
        w.write_string(b"notchunked").await?;
    }
    if effective >= DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL {
        // We drive no parallel-replicas reads; 0 is the "unversioned" value.
        w.write_var_uint(0).await?;
    }
    if effective >= DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM {
        w.flush().await?;
    }
    Ok(())
}

/// Send a Query packet. Mirrors cpp-client `SendQuery()` lines ~970-1130
/// in field order. The caller has already negotiated `server_revision`
/// from the post-Hello handshake.
///
/// `extra_settings` is `(name, value)` pairs serialised with `flags = 0`
/// per cpp; the flag is reserved for server-side use (custom = 2 etc.)
/// and zero is the correct value for plain client-supplied settings.
/// Servers older than `DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS`
/// (54429) cannot accept string-serialised settings; this function
/// returns `Error::Other` rather than silently dropping them (matches
/// cpp `UnimplementedError`).
///
/// `params` are the server-side query parameters a `{name:Type}`
/// placeholder in `query` resolves against. They ride the same
/// `(name, flags, value)` triple shape as settings, in their own
/// revision-gated section, but with the CUSTOM flag, because the server
/// reads them through its custom-setting path
/// (`TCPHandler.cpp:2268-2270` -> `BaseSettings::read`). Each value is
/// therefore a ClickHouse Field dump, not free literal text.
///
/// Setting names and values are emitted verbatim as length-prefixed
/// strings; the server consumes them as plain settings (no SQL parsing
/// of the value at this layer). Validating the contents -- rejecting
/// control bytes or unexpected values -- is the caller's
/// responsibility; this function transmits whatever bytes it is given.
pub(crate) async fn send_query<W: ClickHouseWrite>(
    w: &mut W,
    server_revision: u64,
    query_id: &str,
    query: &str,
    extra_settings: &[(String, String)],
    params: &[(String, String)],
    client_info: &ClientInfo,
) -> Result<()> {
    w.write_var_uint(ClientPacketId::Query as u64).await?;
    w.write_string(query_id.as_bytes()).await?;

    if server_revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO {
        client_info.write_to(w, server_revision).await?;
    }

    if server_revision >= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS {
        for (name, value) in extra_settings {
            w.write_string(name.as_bytes()).await?;
            // flags = 0 for plain client-supplied settings; cpp uses
            // non-zero only for server-side custom settings.
            w.write_var_uint(0).await?;
            w.write_string(value.as_bytes()).await?;
        }
    } else if !extra_settings.is_empty() {
        return Err(Error::Other(
            "tcp: cannot send query settings to server older than 20.1.2.4 \
             (revision 54429); upgrade the server or drop the settings"
                .into(),
        ));
    }
    // Empty string marks end-of-settings, written unconditionally.
    w.write_string(b"").await?;

    if server_revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
        // Interserver secret is empty for non-distributed clients.
        w.write_string(b"").await?;
    }

    w.write_var_uint(QueryProcessingStage::Complete as u64)
        .await?;
    // Compression off: the handshake never negotiates a block codec.
    w.write_var_uint(0).await?;
    w.write_string(query.as_bytes()).await?;

    if server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
        for (name, value) in params {
            w.write_string(name.as_bytes()).await?;
            w.write_var_uint(SETTING_FLAG_CUSTOM).await?;
            w.write_string(value.as_bytes()).await?;
        }
        // Empty name marks end-of-parameters (cpp SendQuery line 1124).
        w.write_string(b"").await?;
    } else if !params.is_empty() {
        return Err(Error::Other(
            "tcp: server revision predates bound query parameters \
             (revision 54459); upgrade the server or inline the values"
                .into(),
        ));
    }

    w.flush().await?;
    Ok(())
}

/// Send a Data packet. Mirrors cpp `SendData()` (1172-1181) + `WriteBlock()`
/// (1140-1170). `column_bytes` is the pre-encoded Native-format payload
/// from [`crate::native::encode`] -- this layer does not re-encode.
///
/// `table_name` is "" for INSERT-into-default-target. cpp writes this
/// only when the server advertises at least
/// `DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES` (50264); a 25.x server is
/// always above this threshold.
pub(crate) async fn send_data_block<W: ClickHouseWrite>(
    w: &mut W,
    server_revision: u64,
    table_name: &str,
    column_bytes: &[u8],
    num_columns: u64,
    num_rows: u64,
) -> Result<()> {
    w.write_var_uint(ClientPacketId::Data as u64).await?;

    if server_revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
        w.write_string(table_name.as_bytes()).await?;
    }

    // Block info -- three (field_id, value) pairs + terminator per cpp
    // WriteBlock lines 1142-1148. bucket_num default is -1 from the cpp
    // BlockInfo struct (block.h line 9); a non-distributed client never
    // overrides this.
    if server_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
        w.write_var_uint(1).await?;
        w.write_u8(0).await?; // is_overflows = false
        w.write_var_uint(2).await?;
        w.write_i32_le(-1).await?; // bucket_num = -1
        w.write_var_uint(0).await?; // terminator
    }

    w.write_var_uint(num_columns).await?;
    w.write_var_uint(num_rows).await?;
    w.write_all(column_bytes).await?;
    w.flush().await?;
    Ok(())
}

/// Send an empty Data block. The server uses an empty client-side Data
/// block as the end-of-input sentinel for INSERTs (cpp `FinalizeQuery()`
/// at line 1132-1138).
pub(crate) async fn send_empty_block<W: ClickHouseWrite>(
    w: &mut W,
    server_revision: u64,
) -> Result<()> {
    send_data_block(w, server_revision, "", &[], 0, 0).await
}

/// Send a Cancel packet (single varint, then flush). cpp `SendCancel()`
/// 1006-1009.
pub(crate) async fn send_cancel<W: ClickHouseWrite>(w: &mut W) -> Result<()> {
    w.write_var_uint(ClientPacketId::Cancel as u64).await?;
    w.flush().await?;
    Ok(())
}

/// Send a Ping packet (single varint, then flush). The server replies
/// with `ServerCodes::Pong` (4). cpp `Ping()` lines 596-606.
pub(crate) async fn send_ping<W: ClickHouseWrite>(w: &mut W) -> Result<()> {
    w.write_var_uint(ClientPacketId::Ping as u64).await?;
    w.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    // Cursor positions into a buffer this module just built: the value cannot
    // exceed the buffer, so a 32-bit `usize` narrowing is not reachable.
    #![allow(clippy::cast_possible_truncation)]

    use super::*;
    use crate::native::io::ClickHouseRead;

    #[tokio::test]
    async fn send_ping_writes_single_byte_varint() {
        let mut buf = Vec::new();
        send_ping(&mut buf).await.unwrap();
        // ClientPacketId::Ping = 4, single-byte varint.
        assert_eq!(buf, vec![4]);
    }

    #[tokio::test]
    async fn send_cancel_writes_single_byte_varint() {
        let mut buf = Vec::new();
        send_cancel(&mut buf).await.unwrap();
        // ClientPacketId::Cancel = 3, single-byte varint.
        assert_eq!(buf, vec![3]);
    }

    #[tokio::test]
    async fn send_hello_byte_layout() {
        let mut buf = Vec::new();
        send_hello(&mut buf, "default", "user", "pw").await.unwrap();
        // First byte is ClientPacketId::Hello (0).
        assert_eq!(buf[0], 0);
        let mut cur = std::io::Cursor::new(&buf[1..]);
        let name = cur.read_utf8_string().await.unwrap();
        assert_eq!(name, CLIENT_NAME);
        let major = cur.read_var_uint().await.unwrap();
        let minor = cur.read_var_uint().await.unwrap();
        let revision = cur.read_var_uint().await.unwrap();
        assert_eq!(major, client_version_major());
        assert_eq!(minor, client_version_minor());
        assert_eq!(revision, DBMS_TCP_PROTOCOL_VERSION);
        assert_eq!(cur.read_utf8_string().await.unwrap(), "default");
        assert_eq!(cur.read_utf8_string().await.unwrap(), "user");
        assert_eq!(cur.read_utf8_string().await.unwrap(), "pw");
    }

    #[tokio::test]
    async fn send_data_block_empty_layout() {
        let mut buf = Vec::new();
        send_empty_block(&mut buf, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();

        let mut cur = std::io::Cursor::new(&buf[..]);
        // Packet ID = Data (2).
        assert_eq!(
            cur.read_var_uint().await.unwrap(),
            ClientPacketId::Data as u64
        );
        // table_name = "" (revision is well above
        // DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES).
        assert_eq!(cur.read_utf8_string().await.unwrap(), "");
        // Block info field 1: id=1, u8 is_overflows=0.
        assert_eq!(cur.read_var_uint().await.unwrap(), 1);
        let mut b = [0u8; 1];
        tokio::io::AsyncReadExt::read_exact(&mut cur, &mut b)
            .await
            .unwrap();
        assert_eq!(b[0], 0);
        // Block info field 2: id=2, i32_le bucket_num=-1.
        assert_eq!(cur.read_var_uint().await.unwrap(), 2);
        let bucket = tokio::io::AsyncReadExt::read_i32_le(&mut cur)
            .await
            .unwrap();
        assert_eq!(bucket, -1);
        // Block info terminator: id=0.
        assert_eq!(cur.read_var_uint().await.unwrap(), 0);
        // num_columns = 0, num_rows = 0, then no column payload.
        assert_eq!(cur.read_var_uint().await.unwrap(), 0);
        assert_eq!(cur.read_var_uint().await.unwrap(), 0);
        // Whole buffer consumed.
        assert_eq!(cur.position() as usize, buf.len());
    }

    /// Walk the whole Query frame at the pinned revision: settings
    /// triples, the settings terminator, the interserver-secret slot,
    /// stage, compression, the SQL, then the parameters section and its
    /// terminator. A misplaced field silently shifts every later one.
    #[tokio::test]
    async fn send_query_byte_layout_at_current_revision() {
        let mut buf = Vec::new();
        let ci = ClientInfo::for_initial_query(
            CLIENT_NAME,
            client_version_major(),
            client_version_minor(),
            DBMS_TCP_PROTOCOL_VERSION,
            "",
        );
        let settings = vec![
            ("max_block_size".to_string(), "1024".to_string()),
            ("database".to_string(), "dfe".to_string()),
        ];
        let params = vec![
            ("db".to_string(), "'default'".to_string()),
            ("n".to_string(), "42".to_string()),
        ];
        send_query(
            &mut buf,
            DBMS_TCP_PROTOCOL_VERSION,
            "qid",
            "SELECT {n:UInt64}",
            &settings,
            &params,
            &ci,
        )
        .await
        .unwrap();

        let mut cur = std::io::Cursor::new(&buf[..]);
        assert_eq!(
            cur.read_var_uint().await.unwrap(),
            ClientPacketId::Query as u64
        );
        assert_eq!(cur.read_utf8_string().await.unwrap(), "qid");

        // ClientInfo, verified field-by-field in client_info.rs; skip to
        // its end by re-reading the same shape.
        skip_client_info(&mut cur).await;

        // Settings: (name, flags, value) triples then an empty name.
        for (name, value) in &settings {
            assert_eq!(&cur.read_utf8_string().await.unwrap(), name);
            assert_eq!(cur.read_var_uint().await.unwrap(), 0, "settings flags");
            assert_eq!(&cur.read_utf8_string().await.unwrap(), value);
        }
        assert_eq!(cur.read_utf8_string().await.unwrap(), "");

        // Interserver secret, empty for a non-distributed client.
        assert_eq!(cur.read_utf8_string().await.unwrap(), "");

        assert_eq!(
            cur.read_var_uint().await.unwrap(),
            QueryProcessingStage::Complete as u64
        );
        assert_eq!(cur.read_var_uint().await.unwrap(), 0, "compression off");
        assert_eq!(cur.read_utf8_string().await.unwrap(), "SELECT {n:UInt64}");

        // Parameters: the same triple shape, flags = CUSTOM, then the
        // empty-name terminator.
        for (name, value) in &params {
            assert_eq!(&cur.read_utf8_string().await.unwrap(), name);
            assert_eq!(
                cur.read_var_uint().await.unwrap(),
                SETTING_FLAG_CUSTOM,
                "a bound parameter is a custom setting"
            );
            assert_eq!(&cur.read_utf8_string().await.unwrap(), value);
        }
        assert_eq!(cur.read_utf8_string().await.unwrap(), "");

        assert_eq!(
            cur.position() as usize,
            buf.len(),
            "the frame must be fully consumed"
        );
    }

    /// Consume a `ClientInfo` block written at the pinned revision.
    async fn skip_client_info(cur: &mut std::io::Cursor<&[u8]>) {
        use tokio::io::AsyncReadExt;
        let mut one = [0u8; 1];
        cur.read_exact(&mut one).await.unwrap(); // query_kind
        for _ in 0..3 {
            let _ = cur.read_utf8_string().await.unwrap(); // initial_user/query_id/address
        }
        let _ = cur.read_i64_le().await.unwrap(); // initial_query_start_time
        cur.read_exact(&mut one).await.unwrap(); // iface_type
        for _ in 0..3 {
            let _ = cur.read_utf8_string().await.unwrap(); // os_user/hostname/client_name
        }
        for _ in 0..3 {
            let _ = cur.read_var_uint().await.unwrap(); // major/minor/revision
        }
        let _ = cur.read_utf8_string().await.unwrap(); // quota_key
        let _ = cur.read_var_uint().await.unwrap(); // distributed_depth
        let _ = cur.read_var_uint().await.unwrap(); // version_patch
        cur.read_exact(&mut one).await.unwrap(); // opentelemetry marker
        for _ in 0..3 {
            let _ = cur.read_var_uint().await.unwrap(); // parallel-replicas zeros
        }
    }

    /// Bound parameters cannot be expressed below revision 54459, so
    /// sending them silently would drop the values and the server would
    /// reject the `{name:Type}` placeholder.
    #[tokio::test]
    async fn send_query_rejects_parameters_below_the_revision_gate() {
        let mut buf = Vec::new();
        let ci = ClientInfo::for_initial_query(
            CLIENT_NAME,
            client_version_major(),
            client_version_minor(),
            DBMS_TCP_PROTOCOL_VERSION,
            "",
        );
        let params = vec![("db".to_string(), "'default'".to_string())];
        let old = DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS - 1;
        let err = send_query(
            &mut buf,
            old,
            "qid",
            "SELECT {db:String}",
            &[],
            &params,
            &ci,
        )
        .await
        .expect_err("parameters below the gate must be refused");
        match err {
            Error::Other(_) => {}
            other => panic!("expected Error::Other, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_query_rejects_old_revision() {
        let mut buf = Vec::new();
        let ci = ClientInfo::for_initial_query(
            CLIENT_NAME,
            client_version_major(),
            client_version_minor(),
            DBMS_TCP_PROTOCOL_VERSION,
            "",
        );
        // Pick a revision below DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
        // (54429) but high enough that send_query reaches the settings
        // loop before failing.
        let old_revision = DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS - 1;
        let settings = vec![("max_block_size".to_string(), "1024".to_string())];
        let err = send_query(
            &mut buf,
            old_revision,
            "qid",
            "SELECT 1",
            &settings,
            &[],
            &ci,
        )
        .await
        .expect_err("expected an error for a too-old server revision");
        // `Error::Other` alone would also match an unrelated write failure,
        // so the message is what pins this to the revision gate.
        let msg = err.to_string();
        assert!(
            msg.contains("54429") && msg.contains("query settings"),
            "error must name the revision gate, got: {msg}"
        );
    }
}
