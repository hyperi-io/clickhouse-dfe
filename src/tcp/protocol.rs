//! ClickHouse TCP transport (port 9000) -- native binary protocol.
//!
//! Packet IDs, handshake structures, revision-gated feature constants.
//! The protocol is wire-revision-negotiated: client advertises
//! [`DBMS_TCP_PROTOCOL_VERSION`] in Hello, server replies with its own
//! revision, and the lower of the two governs every revision-gated
//! field thereafter.
//!
//! Bridge for ClickHouse-docs readers: ClickHouse's own docs call this
//! the "native protocol". Our codebase reserves "native" for the
//! columnar payload format (see [`crate::native`]). "TCP transport"
//! here means the wire protocol on port 9000.
//!
//! Wire primitives (varint, length-prefixed strings, fixed-width LE)
//! come from [`crate::native::io`]; this module is types-only.

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Revision constants
//
// Each constant gates a wire-format change. The two endpoints negotiate
// to `min(client_advertised, server_advertised)` after Hello; every
// field added at revision R is only on-wire when both peers advertise
// at least R. Values match ClickHouse server `Core/ProtocolDefines.h`
// and clickhouse-cpp-client `protocol.h` (mainline, commit e903492).
// ---------------------------------------------------------------------------

pub(crate) const DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES: u64 = 50264;
pub(crate) const DBMS_MIN_REVISION_WITH_BLOCK_INFO: u64 = 51903;
pub(crate) const DBMS_MIN_REVISION_WITH_CLIENT_INFO: u64 = 54032;
pub(crate) const DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE: u64 = 54058;
pub(crate) const DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO: u64 = 54060;
pub(crate) const DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME: u64 = 54372;
pub(crate) const DBMS_MIN_REVISION_WITH_VERSION_PATCH: u64 = 54401;
pub(crate) const DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO: u64 = 54420;
pub(crate) const DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS: u64 = 54429;
pub(crate) const DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET: u64 = 54441;
pub(crate) const DBMS_MIN_REVISION_WITH_OPENTELEMETRY: u64 = 54442;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH: u64 = 54448;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME: u64 = 54449;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PARALLEL_REPLICAS: u64 = 54453;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM: u64 = 54458;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS: u64 = 54459;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS: u64 = 54460;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES: u64 = 54461;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS: u64 = 54463;
pub(crate) const DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION: u64 = 54469;
/// At or above this the server serialises the modern `JSON` and `Dynamic`
/// types in their V2 wire format rather than V1.
pub(crate) const DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION: u64 = 54473;
pub(crate) const DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2: u64 = 54462;
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS: u64 = 54470;
pub(crate) const DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL: u64 = 54471;
/// Despite the name the server reads this Query-packet field from EVERY
/// client, not only an interserver peer (`TCPHandler.cpp:2245-2250`).
pub(crate) const DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES: u64 = 54472;

/// Active protocol revision this client advertises in Hello. Bump only when
/// the wire-format support for the higher revision is in, never to "stay
/// current".
///
/// Pinned to the V2 JSON/Dynamic gate, which is the highest revision whose
/// packets this client reads in full. Everything between it and the server's
/// own 54484 costs fields we do not parse -- `SERVER_SETTINGS` (54474) puts a
/// whole settings block in Hello, and 54477 adds a query-plan version.
pub(crate) const DBMS_TCP_PROTOCOL_VERSION: u64 =
    DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION;

/// Cap on the `stack_trace` field of an [`Error::ServerException`]. The
/// wire string is only bounded by `native::io`'s 1 GiB `MAX_STRING_SIZE`,
/// so this is what keeps hostile or runaway server output out of an
/// error value callers log.
pub(crate) const TCP_EXCEPTION_STACK_TRACE_CAP: usize = 1 << 20;

/// Cap on the `message` field of an [`Error::ServerException`], for the
/// same reason as [`TCP_EXCEPTION_STACK_TRACE_CAP`].
pub(crate) const TCP_EXCEPTION_MESSAGE_CAP: usize = 64 * 1024;

/// Truncate `s` to at most `cap` bytes on a UTF-8 character boundary.
/// `String::truncate` panics mid-character, and server text is arbitrary
/// UTF-8.
pub(crate) fn truncate_on_char_boundary(s: &mut String, cap: usize) {
    if s.len() <= cap {
        return;
    }
    let mut end = cap;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
}

// ---------------------------------------------------------------------------
// Packet IDs
// ---------------------------------------------------------------------------

/// Packet IDs sent client -> server. Mirrors `ClientCodes` in
/// clickhouse-cpp-client `protocol.h`.
#[repr(u64)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientPacketId {
    Hello = 0,
    Query = 1,
    Data = 2,
    Cancel = 3,
    Ping = 4,
}

/// Packet IDs sent server -> client. Mirrors `ServerCodes` in
/// clickhouse-cpp-client `protocol.h`; unknown IDs surface as
/// [`Error::BadResponse`] via [`ServerPacketId::from_u64`].
#[repr(u64)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ServerPacketId {
    Hello = 0,
    Data = 1,
    Exception = 2,
    Progress = 3,
    Pong = 4,
    EndOfStream = 5,
    ProfileInfo = 6,
    Totals = 7,
    Extremes = 8,
    Log = 10,
    TableColumns = 11,
    ProfileEvents = 14,
    TimezoneUpdate = 17,
}

impl ServerPacketId {
    /// # Errors
    ///
    /// [`Error::BadResponse`] for an id this client does not handle.
    pub(crate) fn from_u64(i: u64) -> Result<Self> {
        Ok(match i {
            0 => Self::Hello,
            1 => Self::Data,
            2 => Self::Exception,
            3 => Self::Progress,
            4 => Self::Pong,
            5 => Self::EndOfStream,
            6 => Self::ProfileInfo,
            7 => Self::Totals,
            8 => Self::Extremes,
            10 => Self::Log,
            11 => Self::TableColumns,
            14 => Self::ProfileEvents,
            17 => Self::TimezoneUpdate,
            x => {
                return Err(Error::BadResponse(format!(
                    "tcp: unknown server packet id {x}"
                )));
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Misc protocol enums
// ---------------------------------------------------------------------------

/// Query processing stage sent in the Query packet. The client always
/// requests `Complete`; lower stages exist for distributed-server
/// internal traffic that this client does not generate.
#[repr(u64)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueryProcessingStage {
    Complete = 2,
}

// ---------------------------------------------------------------------------
// Handshake + packet payload staging types
// ---------------------------------------------------------------------------

/// Server Hello payload parsed by the handshake reader. Returned to
/// callers from [`crate::tcp::connect::open_handshaken`] so they can
/// pin connection state to the negotiated revision.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ServerHello {
    /// Server build name, e.g. `"ClickHouse server"`.
    pub server_name: String,
    /// `(major, minor, patch)` as the server reported them.
    pub version: (u64, u64, u64),
    /// Raw protocol revision the server advertised, before negotiation.
    pub revision: u64,
    /// Session timezone; `None` below the timezone revision gate.
    pub timezone: Option<String>,
    /// Operator-facing server display name; `None` below its revision gate.
    pub display_name: Option<String>,
}

/// TCP-protocol staging type for a server exception: one flat wire
/// frame, converted to [`Error::ServerException`] at the dispatch
/// boundary by [`Exception::into_error`].
///
/// Not exposed publicly. Callers only ever see the flat
/// [`Error::ServerException`].
#[derive(Debug, Clone)]
pub(crate) struct Exception {
    /// `code` is signed on the wire (matches `Error::ServerException`).
    pub(crate) code: i32,
    pub(crate) name: String,
    pub(crate) message: String,
    pub(crate) stack_trace: String,
}

impl Exception {
    /// Convert the wire frame into an [`Error::ServerException`], with
    /// an empty `name` / `stack_trace` becoming `None`.
    ///
    /// The message is capped at [`TCP_EXCEPTION_MESSAGE_CAP`] and the
    /// stack trace at [`TCP_EXCEPTION_STACK_TRACE_CAP`], both cut on a
    /// character boundary because server text is arbitrary UTF-8;
    /// truncation emits a `tracing::warn!` so an operator can see the cap
    /// was hit.
    pub(crate) fn into_error(self) -> Error {
        let Exception {
            code,
            name,
            mut message,
            mut stack_trace,
        } = self;

        if message.len() > TCP_EXCEPTION_MESSAGE_CAP {
            tracing::warn!(
                original_len = message.len(),
                cap = TCP_EXCEPTION_MESSAGE_CAP,
                "tcp: server exception message truncated"
            );
            truncate_on_char_boundary(&mut message, TCP_EXCEPTION_MESSAGE_CAP);
        }
        if stack_trace.len() > TCP_EXCEPTION_STACK_TRACE_CAP {
            tracing::warn!(
                original_len = stack_trace.len(),
                cap = TCP_EXCEPTION_STACK_TRACE_CAP,
                "tcp: server exception stack_trace truncated"
            );
            truncate_on_char_boundary(&mut stack_trace, TCP_EXCEPTION_STACK_TRACE_CAP);
        }

        Error::ServerException {
            code,
            name: if name.is_empty() { None } else { Some(name) },
            message,
            stack_trace: if stack_trace.is_empty() {
                None
            } else {
                Some(stack_trace)
            },
        }
    }
}

/// Progress packet payload; which fields the wire carries depends on the
/// negotiated revision, and an omitted one reads back as zero.
///
/// The fields exist because the bytes must be consumed to keep the
/// stream aligned, not because a caller reads them; surfacing progress
/// to callers is a separate feature.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub(crate) struct Progress {
    pub(crate) rows_read: u64,
    pub(crate) bytes_read: u64,
    pub(crate) total_rows_to_read: u64,
    pub(crate) written_rows: u64,
    pub(crate) written_bytes: u64,
}

/// ProfileInfo packet payload; read to keep the stream aligned, not yet
/// surfaced to callers.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub(crate) struct ProfileInfo {
    pub(crate) rows: u64,
    pub(crate) blocks: u64,
    pub(crate) bytes: u64,
    pub(crate) applied_limit: bool,
    pub(crate) rows_before_limit: u64,
}

/// TableColumns packet payload (sent before INSERT to describe the
/// destination schema); read to keep the stream aligned, not yet
/// surfaced to callers.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub(crate) struct TableColumns {
    pub(crate) external_table_name: String,
    pub(crate) columns_definition: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_packet_id_from_u64_round_trips_known() {
        let cases = [
            (0, ServerPacketId::Hello),
            (1, ServerPacketId::Data),
            (2, ServerPacketId::Exception),
            (3, ServerPacketId::Progress),
            (4, ServerPacketId::Pong),
            (5, ServerPacketId::EndOfStream),
            (6, ServerPacketId::ProfileInfo),
            (7, ServerPacketId::Totals),
            (8, ServerPacketId::Extremes),
            (10, ServerPacketId::Log),
            (11, ServerPacketId::TableColumns),
            (14, ServerPacketId::ProfileEvents),
            (17, ServerPacketId::TimezoneUpdate),
        ];
        for (i, expected) in cases {
            assert_eq!(ServerPacketId::from_u64(i).unwrap(), expected);
        }
    }

    #[test]
    fn server_packet_id_from_u64_rejects_unknown() {
        let err = ServerPacketId::from_u64(99).unwrap_err();
        match err {
            Error::BadResponse(msg) => {
                assert!(msg.contains("99"), "msg should mention id, got: {msg}");
            }
            other => panic!("expected BadResponse, got {other:?}"),
        }
    }

    #[test]
    fn exception_into_error_flat() {
        let exc = Exception {
            code: 60,
            name: "UNKNOWN_TABLE".to_string(),
            message: "table foo does not exist".to_string(),
            stack_trace: "frame0".to_string(),
        };
        match exc.into_error() {
            Error::ServerException {
                code,
                name,
                message,
                stack_trace,
            } => {
                assert_eq!(code, 60);
                assert_eq!(name.as_deref(), Some("UNKNOWN_TABLE"));
                assert_eq!(message, "table foo does not exist");
                assert_eq!(stack_trace.as_deref(), Some("frame0"));
            }
            other => panic!("expected ServerException, got {other:?}"),
        }
    }

    #[test]
    fn exception_into_error_caps_stack_trace() {
        let big = "x".repeat(TCP_EXCEPTION_STACK_TRACE_CAP + 1024);
        let exc = Exception {
            code: 1,
            name: "N".to_string(),
            message: "m".to_string(),
            stack_trace: big,
        };
        match exc.into_error() {
            Error::ServerException { stack_trace, .. } => {
                let s = stack_trace.expect("stack_trace populated");
                assert_eq!(s.len(), TCP_EXCEPTION_STACK_TRACE_CAP);
            }
            other => panic!("expected ServerException, got {other:?}"),
        }
    }

    /// `String::truncate` panics mid-character, and a server stack trace
    /// is arbitrary UTF-8, so the cap has to land on a boundary.
    #[test]
    fn into_error_stack_trace_truncation_is_char_boundary_safe() {
        // Three-byte characters, so no multiple of 3 lands on the 1 MiB
        // cap: 1 << 20 is not divisible by 3.
        let big = "\u{20ac}".repeat(TCP_EXCEPTION_STACK_TRACE_CAP);
        assert!(!big.is_char_boundary(TCP_EXCEPTION_STACK_TRACE_CAP));
        let exc = Exception {
            code: 1,
            name: "N".to_string(),
            message: "m".to_string(),
            stack_trace: big,
        };
        match exc.into_error() {
            Error::ServerException { stack_trace, .. } => {
                let s = stack_trace.expect("stack_trace populated");
                assert!(s.len() <= TCP_EXCEPTION_STACK_TRACE_CAP);
                // Cutting on the boundary below the cap loses at most
                // two bytes of a three-byte character.
                assert!(s.len() > TCP_EXCEPTION_STACK_TRACE_CAP - 3);
            }
            other => panic!("expected ServerException, got {other:?}"),
        }
    }

    /// A wire message is bounded only by `MAX_STRING_SIZE` (1 GiB), so
    /// the cap has to hold and cut on a character boundary.
    #[test]
    fn into_error_message_truncation_is_char_boundary_safe() {
        // 64 KiB is not divisible by 3, so a run of three-byte characters
        // guarantees the cap lands mid-character.
        let big = "\u{20ac}".repeat(TCP_EXCEPTION_MESSAGE_CAP);
        assert!(!big.is_char_boundary(TCP_EXCEPTION_MESSAGE_CAP));
        let exc = Exception {
            code: 60,
            name: "N".to_string(),
            message: big,
            stack_trace: String::new(),
        };
        match exc.into_error() {
            Error::ServerException { message, .. } => {
                assert!(
                    message.len() <= TCP_EXCEPTION_MESSAGE_CAP,
                    "message must be capped, got {} bytes",
                    message.len()
                );
                assert!(message.len() > TCP_EXCEPTION_MESSAGE_CAP - 3);
                // A valid `String` proves the cut landed on a boundary.
                assert!(message.starts_with('\u{20ac}'));
            }
            other => panic!("expected ServerException, got {other:?}"),
        }
    }

    #[test]
    fn truncate_on_char_boundary_never_splits_a_character() {
        for cap in 0..12 {
            let mut s = "\u{20ac}\u{20ac}\u{20ac}".to_string();
            truncate_on_char_boundary(&mut s, cap);
            assert!(s.len() <= cap);
            assert_eq!(s.len() % 3, 0, "cut mid-character at cap {cap}");
        }
    }

    #[test]
    fn exception_into_error_blank_fields_become_none() {
        let exc = Exception {
            code: 1,
            name: String::new(),
            message: "m".to_string(),
            stack_trace: String::new(),
        };
        match exc.into_error() {
            Error::ServerException {
                name, stack_trace, ..
            } => {
                assert!(name.is_none());
                assert!(stack_trace.is_none());
            }
            other => panic!("expected ServerException, got {other:?}"),
        }
    }
}
