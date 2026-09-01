//! `ClientInfo` block written inside the client-side Query packet.
//!
//! Mirrors clickhouse-cpp-client `SendQuery()` lines 1026-1086, with the
//! negotiated revision driving which fields go on the wire. A
//! non-distributed, non-OpenTelemetry, non-parallel-replicas client
//! still writes the revision-gated extras as zeros or empty strings,
//! because the server reads them positionally.

use tokio::io::AsyncWriteExt;

use crate::error::Result;
use crate::native::io::ClickHouseWrite;
use crate::tcp::protocol::{
    DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH,
    DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME,
    DBMS_MIN_PROTOCOL_VERSION_WITH_PARALLEL_REPLICAS, DBMS_MIN_REVISION_WITH_OPENTELEMETRY,
    DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO, DBMS_MIN_REVISION_WITH_VERSION_PATCH,
};

/// `query_kind` field. InitialQuery = client-initiated query, the only
/// kind a non-server-side client ever emits. NoQuery (0) and SecondaryQuery
/// (2) are reserved for distributed-server internal traffic.
pub(crate) const QUERY_KIND_INITIAL_QUERY: u8 = 1;

/// `iface_type` field. TCP = direct native protocol; HTTP (2) is what an
/// HTTP-transport client would advertise. We emit TCP unconditionally
/// because this module only runs over TCP transport.
pub(crate) const INTERFACE_TYPE_TCP: u8 = 1;

/// ClientInfo payload written inside the Query packet.
///
/// Field order and naming match cpp-client `Client::Impl::ClientInfo`.
/// Strings default to empty; numeric counters default to zero. The
/// caller fills in `client_name`, `client_version_major/minor`, and
/// `client_revision` from the same constants the Hello packet uses, so
/// the server sees a consistent client identity.
#[derive(Debug, Clone, Default)]
pub(crate) struct ClientInfo {
    pub(crate) query_kind: u8,
    pub(crate) initial_user: String,
    pub(crate) initial_query_id: String,
    pub(crate) initial_address: String,
    pub(crate) os_user: String,
    pub(crate) client_hostname: String,
    pub(crate) client_name: String,
    pub(crate) client_version_major: u64,
    pub(crate) client_version_minor: u64,
    pub(crate) client_version_patch: u64,
    pub(crate) client_revision: u64,
    pub(crate) interface: u8,
    pub(crate) quota_key: String,
    pub(crate) distributed_depth: u64,
}

impl ClientInfo {
    /// Build a ClientInfo for an initial (client-originated) query. The
    /// fields not exposed as parameters either default to empty/zero
    /// (initial_user, initial_address, os_user, etc.) or get filled in
    /// from the same constants the Hello packet uses.
    pub(crate) fn for_initial_query(
        client_name: &str,
        client_version_major: u64,
        client_version_minor: u64,
        client_revision: u64,
        quota_key: &str,
    ) -> Self {
        Self {
            query_kind: QUERY_KIND_INITIAL_QUERY,
            initial_user: String::new(),
            initial_query_id: String::new(),
            // Must parse as Poco::Net::SocketAddress on the server side;
            // empty string throws. cpp-client uses the same v4-mapped
            // loopback sentinel when no real client-side address exists.
            initial_address: "[::ffff:127.0.0.1]:0".to_string(),
            os_user: String::new(),
            client_hostname: String::new(),
            client_name: client_name.to_string(),
            client_version_major,
            client_version_minor,
            client_version_patch: 0,
            client_revision,
            interface: INTERFACE_TYPE_TCP,
            quota_key: quota_key.to_string(),
            distributed_depth: 0,
        }
    }

    /// Write the ClientInfo block, revision-gated per cpp-client
    /// `SendQuery()` lines 1026-1086. The caller has already checked
    /// `server_revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO`.
    pub(crate) async fn write_to<W: ClickHouseWrite>(
        &self,
        w: &mut W,
        server_revision: u64,
    ) -> Result<()> {
        // query_kind is a fixed u8 (cpp WriteFixed on info.query_kind).
        w.write_u8(self.query_kind).await?;
        w.write_string(self.initial_user.as_bytes()).await?;
        w.write_string(self.initial_query_id.as_bytes()).await?;
        w.write_string(self.initial_address.as_bytes()).await?;

        if server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME {
            // initial_query_start_time_microseconds, fixed i64. We do
            // not track client-initiated query start time at this layer;
            // leave zero -- the server treats this as "unset".
            w.write_i64_le(0).await?;
        }

        // iface_type is a fixed u8 (cpp WriteFixed on info.iface_type).
        w.write_u8(self.interface).await?;

        w.write_string(self.os_user.as_bytes()).await?;
        w.write_string(self.client_hostname.as_bytes()).await?;
        w.write_string(self.client_name.as_bytes()).await?;
        w.write_var_uint(self.client_version_major).await?;
        w.write_var_uint(self.client_version_minor).await?;
        w.write_var_uint(self.client_revision).await?;

        if server_revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
            w.write_string(self.quota_key.as_bytes()).await?;
        }
        if server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH {
            w.write_var_uint(self.distributed_depth).await?;
        }
        if server_revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
            w.write_var_uint(self.client_version_patch).await?;
        }

        if server_revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
            // No OpenTelemetry tracing context at this layer; emit the
            // "absent" marker (single zero byte). Adding tracing
            // propagation is a deliberate later layer -- shape matches
            // cpp lines 1071-1074.
            w.write_u8(0).await?;
        }

        if server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARALLEL_REPLICAS {
            // collaborate_with_initiator, count_participating_replicas,
            // number_of_current_replica. Not a parallel-replicas client;
            // emit the three zero varints cpp does at lines 1083-1085.
            w.write_var_uint(0).await?;
            w.write_var_uint(0).await?;
            w.write_var_uint(0).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // Cursor positions into a buffer this module just built: the value cannot
    // exceed the buffer, so a 32-bit `usize` narrowing is not reachable.
    #![allow(clippy::cast_possible_truncation)]

    use super::*;
    use crate::native::io::ClickHouseRead;
    use crate::tcp::protocol::DBMS_TCP_PROTOCOL_VERSION;
    use tokio::io::AsyncReadExt;

    /// The server reads `ClientInfo` positionally, so one field out of
    /// order shifts every field after it and the query is rejected or
    /// silently misattributed. Walks the whole block in cpp order.
    #[tokio::test]
    async fn client_info_write_to_matches_cpp_field_order() {
        let info = ClientInfo::for_initial_query("test-client", 25, 4, 54_459, "quota-1");
        let mut buf = Vec::new();
        info.write_to(&mut buf, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();

        let mut cur = std::io::Cursor::new(&buf[..]);
        let mut one = [0u8; 1];

        cur.read_exact(&mut one).await.unwrap();
        assert_eq!(one[0], QUERY_KIND_INITIAL_QUERY, "query_kind");
        assert_eq!(cur.read_utf8_string().await.unwrap(), "", "initial_user");
        assert_eq!(
            cur.read_utf8_string().await.unwrap(),
            "",
            "initial_query_id"
        );
        assert_eq!(
            cur.read_utf8_string().await.unwrap(),
            "[::ffff:127.0.0.1]:0",
            "initial_address must parse as a Poco SocketAddress"
        );
        assert_eq!(
            cur.read_i64_le().await.unwrap(),
            0,
            "initial_query_start_time_microseconds"
        );

        cur.read_exact(&mut one).await.unwrap();
        assert_eq!(one[0], INTERFACE_TYPE_TCP, "iface_type");
        assert_eq!(cur.read_utf8_string().await.unwrap(), "", "os_user");
        assert_eq!(cur.read_utf8_string().await.unwrap(), "", "client_hostname");
        assert_eq!(
            cur.read_utf8_string().await.unwrap(),
            "test-client",
            "client_name"
        );
        assert_eq!(cur.read_var_uint().await.unwrap(), 25, "version_major");
        assert_eq!(cur.read_var_uint().await.unwrap(), 4, "version_minor");
        assert_eq!(
            cur.read_var_uint().await.unwrap(),
            54_459,
            "client_revision"
        );
        assert_eq!(
            cur.read_utf8_string().await.unwrap(),
            "quota-1",
            "quota_key"
        );
        assert_eq!(cur.read_var_uint().await.unwrap(), 0, "distributed_depth");
        assert_eq!(cur.read_var_uint().await.unwrap(), 0, "version_patch");

        cur.read_exact(&mut one).await.unwrap();
        assert_eq!(one[0], 0, "opentelemetry absent marker");
        for field in [
            "collaborate_with_initiator",
            "count_participating_replicas",
            "number_of_current_replica",
        ] {
            assert_eq!(cur.read_var_uint().await.unwrap(), 0, "{field}");
        }

        assert_eq!(
            cur.position() as usize,
            buf.len(),
            "the block must be fully consumed"
        );
    }

    /// The quota key reaches the server twice: once in the handshake
    /// addendum and once per query here, so a query issued without it
    /// is attributed to the wrong quota.
    #[tokio::test]
    async fn quota_key_reaches_the_wire() {
        let info = ClientInfo::for_initial_query("c", 1, 0, 54_459, "team-a");
        let mut buf = Vec::new();
        info.write_to(&mut buf, DBMS_TCP_PROTOCOL_VERSION)
            .await
            .unwrap();
        let needle = b"team-a";
        assert!(
            buf.windows(needle.len()).any(|w| w == needle),
            "quota_key must appear in the ClientInfo block"
        );
    }
}
