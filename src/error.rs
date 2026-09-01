//! Contains [`Error`] and corresponding [`Result`].

use serde::{de, ser};
use std::{error::Error as StdError, fmt, io, result, str::Utf8Error};

/// A result with a specified [`Error`] type.
pub type Result<T, E = Error> = result::Result<T, E>;

type BoxedError = Box<dyn StdError + Send + Sync>;

/// Represents all possible errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum Error {
    #[error("invalid params: {0}")]
    InvalidParams(#[source] BoxedError),
    #[error("network error: {0}")]
    Network(#[source] BoxedError),
    #[error("compression error: {0}")]
    Compression(#[source] BoxedError),
    #[error("decompression error: {0}")]
    Decompression(#[source] BoxedError),
    #[error("no rows returned by a query that expected to return at least one row")]
    RowNotFound,
    #[error("sequences must have a known size ahead of time")]
    SequenceMustHaveLength,
    #[error("`deserialize_any` is not supported")]
    DeserializeAnyNotSupported,
    #[error("not enough data, probably a row type mismatches a database schema")]
    NotEnoughData,
    #[error("string is not valid utf8")]
    InvalidUtf8Encoding(#[from] Utf8Error),
    #[error("tag for enum is not valid")]
    InvalidTagEncoding(usize),
    #[error("max number of types in the Variant data type is 255, got {0}")]
    VariantDiscriminatorIsOutOfBound(usize),
    #[error("a custom error message from serde: {0}")]
    Custom(String),
    /// Background worker task exited; the inserter is no longer
    /// accepting commands. Terminal: construct a fresh inserter,
    /// don't retry the same handle.
    #[error("background worker task has exited; the inserter is no longer accepting commands")]
    WorkerExited,
    /// Structured server-side exception parsed from the response.
    /// Preferred over [`Error::BadResponse`] when the server returned
    /// a recognisable `Code: NNN. DB::Exception: ...` body. Callers
    /// can match on `code` for typed handling and use
    /// [`Error::is_retriable`] for conservative retry classification.
    #[error("server error code {code}: {message}")]
    ServerException {
        /// `X-ClickHouse-Exception-Code` from the response header.
        /// Signed `int` on the wire (cpp-client + server source);
        /// codes are small positives in practice.
        code: i32,
        /// Exception name extracted from `(UPPERCASE_NAME)` near
        /// the end of the message. `None` when the parser couldn't
        /// recognise it (older CH versions, custom error paths).
        name: Option<String>,
        /// Cleaned message body, stripped of the `Code: N.`,
        /// `DB::Exception:` prefix, exception-name parens, and
        /// `(version ...)` suffix.
        message: String,
        /// Server-side stack trace if `Stack trace:` was present in
        /// the body.
        stack_trace: Option<String>,
    },
    #[error("bad response: {0}")]
    BadResponse(String),
    /// A transport-level failure that a fresh attempt may not hit --
    /// a pool-acquire timeout, a closed pool, a hook failure. Typed so
    /// retry classification does not have to match on message text.
    #[error("transient transport failure: {0}")]
    Transient(String),
    /// A connection could not be opened: an unresolvable host, a host
    /// that resolved to no addresses, an empty endpoint list. Typed for
    /// the same reason as [`Error::Transient`].
    #[error("connect failed: {0}")]
    Connect(String),
    #[error("timeout expired")]
    TimedOut,
    #[error("error while parsing columns header from the response: {0}")]
    InvalidColumnsHeader(#[source] BoxedError),
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("{0}")]
    Other(BoxedError),
}

// Variant-by-variant where the name matches, `Other` otherwise.
// Upstream's enum is `#[non_exhaustive]`, so a variant added there later
// lands in `Other` until this mapping is extended.
impl From<clickhouse::error::Error> for Error {
    fn from(error: clickhouse::error::Error) -> Self {
        use clickhouse::error::Error as Up;
        match error {
            Up::InvalidParams(e) => Self::InvalidParams(e),
            Up::Network(e) => Self::Network(e),
            Up::Compression(e) => Self::Compression(e),
            Up::Decompression(e) => Self::Decompression(e),
            Up::RowNotFound => Self::RowNotFound,
            Up::SequenceMustHaveLength => Self::SequenceMustHaveLength,
            Up::DeserializeAnyNotSupported => Self::DeserializeAnyNotSupported,
            Up::NotEnoughData => Self::NotEnoughData,
            Up::InvalidUtf8Encoding(e) => Self::InvalidUtf8Encoding(e),
            Up::InvalidTagEncoding(n) => Self::InvalidTagEncoding(n),
            Up::VariantDiscriminatorIsOutOfBound(n) => Self::VariantDiscriminatorIsOutOfBound(n),
            Up::Custom(s) => Self::Custom(s),
            Up::BadResponse(s) => Self::BadResponse(s),
            Up::TimedOut => Self::TimedOut,
            Up::InvalidColumnsHeader(e) => Self::InvalidColumnsHeader(e),
            Up::SchemaMismatch(s) => Self::SchemaMismatch(s),
            Up::Unsupported(s) => Self::Unsupported(s),
            other => Self::Other(Box::new(other)),
        }
    }
}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl From<Error> for io::Error {
    fn from(error: Error) -> Self {
        io::Error::other(error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        // TODO: after MSRV 1.79 replace with `io::Error::downcast`.
        if error.get_ref().is_some_and(|r| r.is::<Error>()) {
            *error.into_inner().unwrap().downcast::<Error>().unwrap()
        } else {
            Self::Other(error.into())
        }
    }
}

impl Error {
    /// Conservative retriability classification.
    ///
    /// Returns `true` for errors that *might* succeed on retry --
    /// transport-level (`Network`, `TimedOut`, `Transient`, `Connect`)
    /// and a known set of transient server-side codes (timeouts,
    /// simultaneous-query limits, parts-count overruns, Keeper hiccups).
    ///
    /// Returns `false` for everything else, including unknown
    /// server codes. The classification is intentionally
    /// conservative -- callers wanting more aggressive retry should
    /// match on the underlying variant. The full mapping lives in
    /// [`is_retriable_code`][Self::is_retriable_code]; PRs welcome
    /// to extend it as production experience shows what's actually
    /// transient.
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        match self {
            // `Transient` and `Connect` are only ever built for a
            // condition a fresh attempt may not hit, so they classify
            // here rather than on their message text.
            Self::Network(_) | Self::TimedOut | Self::Transient(_) | Self::Connect(_) => true,
            Self::ServerException { code, .. } => Self::is_retriable_code(*code),
            _ => false,
        }
    }

    /// ClickHouse error codes considered transient for retry. See
    /// `src/Common/ErrorCodes.cpp` upstream for the canonical list.
    /// Conservative -- false negatives are expected; false positives
    /// should be rare. Source-of-truth comments in this table use
    /// the upstream macro name. The table is production-experience
    /// driven; PRs extending it with new operationally-transient
    /// codes are welcome.
    #[must_use]
    pub fn is_retriable_code(code: i32) -> bool {
        matches!(
            code,
            // TIMEOUT_EXCEEDED
            159 |
            // TOO_SLOW (merge queue lag; usually transient)
            160 |
            // TOO_MANY_SIMULTANEOUS_QUERIES
            202 |
            // SOCKET_TIMEOUT
            209 |
            // NETWORK_ERROR
            210 |
            // ABORTED (operator KILL or shutdown -- replayable on
            // another node)
            236 |
            // MEMORY_LIMIT_EXCEEDED (often transient under brief
            // memory pressure)
            241 |
            // TOO_MANY_PARTS (transient under heavy ingest)
            252 |
            // ALL_CONNECTION_TRIES_FAILED
            279 |
            // LIMIT_EXCEEDED (transient quota)
            290 |
            // UNKNOWN_STATUS_OF_INSERT (the "did my INSERT land?"
            // ambiguity code -- documented mitigation is retry with
            // an insert_deduplication_token, which the
            // batch_isolation token variant ships)
            319 |
            // RECEIVED_ERROR_TOO_MANY_REQUESTS (server proxied a 429
            // from a downstream resource -- S3, Kafka, HTTP storage)
            364 |
            // PART_IS_TEMPORARILY_LOCKED (mutate/merge contention)
            384 |
            // CANNOT_SCHEDULE_TASK (task scheduler full)
            439 |
            // DEADLOCK_AVOIDED (server-side avoidance; succeeds on retry)
            473 |
            // UNKNOWN_STATUS_OF_TRANSACTION (transaction analogue of
            // UNKNOWN_STATUS_OF_INSERT -- same "did it land?" ambiguity)
            659 |
            // TOO_MANY_UNAVAILABLE_SHARDS (transient cluster condition)
            904 |
            // KEEPER_EXCEPTION (replicated-metadata hiccup)
            999
        )
    }

    /// Record this `Error` against the current `tracing::Span` with the
    /// supplied context message.
    pub fn record_in_current_span(&self, msg: &str) {
        tracing::debug!(error=%self, "{msg}");
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use std::io;

    #[test]
    fn roundtrip_io_error() {
        let orig = Error::NotEnoughData;

        // Error -> io::Error
        let orig_str = orig.to_string();
        let io = io::Error::from(orig);
        assert_eq!(io.kind(), io::ErrorKind::Other);
        assert_eq!(io.to_string(), orig_str);

        // io::Error -> Error
        let orig = Error::from(io);
        assert!(matches!(orig, Error::NotEnoughData));
    }

    #[test]
    fn error_traits() {
        fn assert_traits<T: std::error::Error + Send + Sync>() {}

        assert_traits::<Error>();
    }

    #[test]
    fn is_retriable_classifies_known_transient_codes() {
        let make = |code: i32| Error::ServerException {
            code,
            name: None,
            message: String::new(),
            stack_trace: None,
        };

        // Sampled from the retriable table -- known transient codes.
        for &code in &[
            159i32, 160, 202, 209, 210, 236, 241, 252, 279, 290, 319, 364, 384, 439, 473, 659, 904,
            999,
        ] {
            assert!(make(code).is_retriable(), "code {code} should be retriable");
        }

        // Sampled known-not-retriable codes (auth, schema, parse).
        for &code in &[36i32, 60, 117, 192, 195, 469] {
            assert!(
                !make(code).is_retriable(),
                "code {code} should NOT be retriable"
            );
        }
    }

    #[test]
    fn is_retriable_for_transport_errors() {
        assert!(Error::TimedOut.is_retriable());
        // Network variant needs a BoxedError construction; use a
        // simple io::Error round-trip to build one.
        let net_err: Error = io::Error::new(io::ErrorKind::ConnectionReset, "reset").into();
        // io::Error -> Error::Custom path; not a Network variant.
        // Just verify Custom is NOT retriable to lock in the
        // conservative classification.
        assert!(!net_err.is_retriable());
    }

    #[test]
    fn is_retriable_code_defaults_unknown_to_not_retriable() {
        // Conservative default: anything outside the documented
        // retriable set is NOT retriable. New ClickHouse versions
        // can ship new codes; until our table is updated, callers
        // see them as terminal -- safer than masking real failures
        // under a retry loop.
        for &code in &[0i32, 1, 12345, 99999, i32::MAX] {
            assert!(
                !Error::is_retriable_code(code),
                "unknown code {code} should default to NOT retriable"
            );
        }
    }
}
