//! Bounded-backoff retry for provably-idempotent TCP operations.
//!
//! - [`RetryPolicy`] -- caller knob (attempt count plus the backoff
//!   bounds), surfaced as `TcpClient::with_retry`.
//! - [`run_with_retry`] -- the acquire+dispatch loop the
//!   [`crate::tcp::client_ext`] helpers wrap around repeatable operations
//!   (SELECT open, opt-in `ExecuteQuery`). **An in-flight INSERT is NEVER
//!   routed through here.**
//!
//! The retried region is `pool.get()` + the operation's *issue* only
//! (for a SELECT, opening the cursor): no row is consumed inside it, so a
//! transient connect/issue failure -- including a silently-dead pooled
//! connection -- replays safely; a mid-stream failure surfaces unchanged.
//! Endpoint failover lives in [`crate::tcp::pool`] (`create`
//! round-robins); this adds backoff passes on top.
//!
//! The schedule comes from `backon`'s [`ExponentialBuilder`], jittered:
//! every connection in a pool fails at the same moment when a server
//! restarts, so an unjittered schedule retries them all in lockstep.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use deadpool::managed::{Object, PoolError};

use crate::error::{Error, Result};
use crate::tcp::pool::{NativePool, TcpConnectionManager};

/// Bounded-backoff retry policy for idempotent TCP operations.
///
/// `None` (no policy) is a single acquire pass with no sleep -- endpoint
/// failover still happens inside `create`. A policy adds extra
/// acquire+dispatch passes with jittered bounded-exponential backoff
/// between them.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct RetryPolicy {
    /// Total attempts including the first (`1` = no retry; matches the
    /// `retry: None` semantics).
    pub max_attempts: u32,
    /// Backoff before the 2nd attempt; doubles each attempt up to
    /// [`Self::max_backoff`], with jitter applied on top.
    pub initial_backoff: Duration,
    /// Cap on a single backoff sleep, bounding the worst-case wait.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    /// Three attempts (two retries), 100ms initial backoff capped at 2s.
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(2),
        }
    }
}

impl RetryPolicy {
    /// The `backon` schedule this policy describes.
    ///
    /// `with_max_times` counts RETRIES, so it is one less than
    /// [`Self::max_attempts`]; a policy of 0 or 1 attempts yields no
    /// retries.
    fn schedule(self) -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(self.initial_backoff)
            .with_max_delay(self.max_backoff)
            .with_max_times(self.max_attempts.saturating_sub(1) as usize)
            .with_jitter()
    }
}

/// True when `e` is worth replaying on a fresh connection: either the
/// crate's conservative [`Error::is_retriable`] says so (Network /
/// TimedOut / `Transient` / `Connect` / known-transient server codes) or
/// it is a boxed `io::Error` of a transient connect kind.
///
/// Deliberately conservative: anything we cannot positively classify as
/// a transient transport/connect failure (a server Exception with a
/// non-transient code, a schema mismatch, a serde error, a `tcp:`
/// protocol-state message) returns `false` so it surfaces immediately
/// rather than being masked under a retry loop.
pub fn is_retriable_transport(e: &Error) -> bool {
    if e.is_retriable() {
        return true;
    }
    match e {
        // ECONNREFUSED / reset / abort / connect-timeout reach us as
        // `Error::Other(io::Error)`; retry only transient io kinds, not
        // every `Other`.
        Error::Other(boxed) => boxed
            .downcast_ref::<std::io::Error>()
            .is_some_and(is_transient_io_kind),
        _ => false,
    }
}

/// `true` for the `io::ErrorKind`s that represent a transient
/// connect/network failure worth replaying on a fresh connection
/// (another endpoint, or the same one a moment later). Conservative:
/// anything not positively transient (e.g. `PermissionDenied`,
/// `InvalidInput`) is excluded.
fn is_transient_io_kind(e: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    matches!(
        e.kind(),
        ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::NotConnected
            | ErrorKind::BrokenPipe
            | ErrorKind::TimedOut
            | ErrorKind::Interrupted
            | ErrorKind::UnexpectedEof
            | ErrorKind::AddrNotAvailable
            | ErrorKind::HostUnreachable
            | ErrorKind::NetworkUnreachable
            | ErrorKind::NetworkDown
    )
}

/// Map a deadpool [`PoolError`] into a crate [`Error`].
///
/// `Backend(Error)` already carries our typed error -- a connect,
/// resolve or handshake failure from `TcpConnectionManager::create` --
/// so it passes through unchanged and stays classifiable. Pool-internal
/// conditions (acquire timeout, closed pool, missing runtime, hook
/// failure) carry no crate error, so they become [`Error::Transient`].
#[must_use]
pub fn map_pool_error(e: PoolError<Error>) -> Error {
    match e {
        PoolError::Backend(err) => err,
        other => Error::Transient(format!("tcp pool: {other}")),
    }
}

/// Acquire a connection and run `op`, retrying transient
/// transport/connect failures per `retry`.
///
/// `retry == None` (or a policy with `max_attempts <= 1`) means a
/// single acquire pass with no sleep -- endpoint failover still happens
/// inside the pool manager's `create`. With a policy of N attempts, a
/// failed attempt whose error [`is_retriable_transport`] sleeps a
/// jittered backoff then re-acquires (which round-robins to a fresh
/// endpoint start) and re-runs `op`; a non-retriable error
/// short-circuits immediately. After the final attempt the last error
/// surfaces unchanged (typed).
///
/// `op` is `Fn` (callable once per attempt) and receives the freshly
/// acquired [`Object`], which derefs to
/// [`crate::tcp::connection_actor::ConnectionHandle`].
///
/// # Errors
///
/// The last attempt's error, unchanged: a pool-acquire failure mapped by
/// [`map_pool_error`], or whatever `op` returned.
pub async fn run_with_retry<T, F, Fut>(
    pool: &Arc<NativePool>,
    retry: Option<RetryPolicy>,
    op: F,
) -> Result<T>
where
    F: Fn(Object<TcpConnectionManager>) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    // Build the per-attempt fallible block: acquire (mapping a pool
    // error to a typed, retry-classifiable crate error) then run `op`.
    // A `pool.get()` failure is folded into the same `Result` so a
    // connect failure is classified by `is_retriable_transport`
    // identically to an `op` failure.
    let attempt_once = || async {
        let conn = pool.get().await.map_err(map_pool_error)?;
        op(conn).await
    };
    retry_with(retry, attempt_once).await
}

/// Drive `attempt_once` under `retry`, factored out so it is testable
/// without a live pool.
///
/// `None` and a policy of at most one attempt both collapse to a single
/// call with no sleep, which keeps `backon` out of the path entirely for
/// the default configuration.
async fn retry_with<T, A, Fut>(retry: Option<RetryPolicy>, attempt_once: A) -> Result<T>
where
    A: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    match retry {
        Some(policy) if policy.max_attempts > 1 => {
            attempt_once
                .retry(policy.schedule())
                .when(is_retriable_transport)
                .sleep(tokio::time::sleep)
                .await
        }
        _ => attempt_once().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::time::Duration;

    fn policy(max_attempts: u32, initial_ms: u64, max_ms: u64) -> RetryPolicy {
        RetryPolicy {
            max_attempts,
            initial_backoff: Duration::from_millis(initial_ms),
            max_backoff: Duration::from_millis(max_ms),
        }
    }

    fn server_error() -> Error {
        // Non-transient server code (UNKNOWN_TABLE = 60): NOT retriable.
        Error::ServerException {
            code: 60,
            name: None,
            message: "no such table".to_string(),
            stack_trace: None,
        }
    }

    fn transient_pool_error() -> Error {
        // Mirrors what `map_pool_error` stamps for a non-Backend pool
        // error -- recognised as a transient acquire failure.
        Error::Transient("tcp pool: Timeout occurred while creating a new object".to_string())
    }

    #[test]
    fn is_retriable_transport_classification() {
        assert!(is_retriable_transport(&Error::TimedOut));
        assert!(is_retriable_transport(&transient_pool_error()));
        assert!(is_retriable_transport(&Error::Connect(
            "tcp: cannot resolve \"bad:9000\"".to_string()
        )));
        // A retriable server code rides through is_retriable().
        assert!(is_retriable_transport(&Error::ServerException {
            code: 209, // SOCKET_TIMEOUT
            name: None,
            message: String::new(),
            stack_trace: None,
        }));
        // Resolver "no addresses" is transient (DNS blip / next endpoint).
        assert!(is_retriable_transport(&Error::Connect(
            "tcp: \"bad:9000\" resolved to no addresses".to_string()
        )));
        // Non-transient server error is NOT retriable.
        assert!(!is_retriable_transport(&server_error()));
        // An unrelated Custom string is NOT retriable.
        assert!(!is_retriable_transport(&Error::Custom(
            "some serde failure".to_string()
        )));
        // Protocol/state errors are Custom, never Transient/Connect, so
        // no `tcp:` message can be mistaken for a transient failure.
        for non_transient in [
            "tcp: actor busy in INSERT",
            "tcp: no INSERT session active",
            "tcp: INSERT block of 99 bytes exceeds the cap",
            "tcp: TLS requested but the `tls` feature is not enabled",
            "tcp pool: a message that only looks like a pool error",
        ] {
            assert!(
                !is_retriable_transport(&Error::Custom(non_transient.to_string())),
                "must NOT retry: {non_transient}"
            );
        }
    }

    #[test]
    fn is_retriable_transport_classifies_connect_io_kinds() {
        use std::io;
        // A refused connect round-trips io::Error -> Error::Other and
        // must be retriable (the common multi-host failover case).
        let refused: Error = io::Error::new(io::ErrorKind::ConnectionRefused, "refused").into();
        assert!(matches!(refused, Error::Other(_)));
        assert!(is_retriable_transport(&refused));

        let reset: Error = io::Error::new(io::ErrorKind::ConnectionReset, "reset").into();
        assert!(is_retriable_transport(&reset));

        // A non-transient io kind wrapped in Other stays terminal.
        let denied: Error = io::Error::new(io::ErrorKind::PermissionDenied, "denied").into();
        assert!(matches!(denied, Error::Other(_)));
        assert!(!is_retriable_transport(&denied));

        // A non-io Other is terminal.
        let other = Error::Other("plain string boxed".into());
        assert!(!is_retriable_transport(&other));
    }

    /// Upper bound only: the schedule is jittered, so a sleep is
    /// somewhere in `(0, candidate]` and the total cannot be asserted
    /// exactly. The attempt count is exact either way.
    #[tokio::test(start_paused = true)]
    async fn retry_then_succeed_stays_within_the_backoff_bounds() {
        let calls = Cell::new(0u32);
        let p = policy(3, 100, 2000);
        let start = tokio::time::Instant::now();
        let r: Result<u32> = retry_with(Some(p), || {
            let n = calls.get() + 1;
            calls.set(n);
            async move { if n < 3 { Err(Error::TimedOut) } else { Ok(n) } }
        })
        .await;
        assert_eq!(r.unwrap(), 3);
        assert_eq!(calls.get(), 3, "should have run exactly three attempts");
        // Unjittered candidates are 100ms + 200ms; allow 2x for jitter.
        assert!(
            start.elapsed() <= Duration::from_millis(600),
            "two backoffs should stay within twice the unjittered 300ms, took {:?}",
            start.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_exhaustion_surfaces_last_error() {
        let calls = Cell::new(0u32);
        let p = policy(3, 100, 2000);
        let r: Result<u32> = retry_with(Some(p), || {
            calls.set(calls.get() + 1);
            async { Err(Error::TimedOut) }
        })
        .await;
        assert!(matches!(r, Err(Error::TimedOut)));
        assert_eq!(calls.get(), 3, "should have exhausted all three attempts");
    }

    #[tokio::test(start_paused = true)]
    async fn non_retriable_short_circuits_without_sleep() {
        let calls = Cell::new(0u32);
        let p = policy(5, 100, 2000);
        let start = tokio::time::Instant::now();
        let r: Result<u32> = retry_with(Some(p), || {
            calls.set(calls.get() + 1);
            async { Err(server_error()) }
        })
        .await;
        assert!(matches!(r, Err(Error::ServerException { code: 60, .. })));
        assert_eq!(calls.get(), 1, "non-retriable must not retry");
        assert_eq!(start.elapsed(), Duration::ZERO, "must not sleep");
    }

    #[tokio::test(start_paused = true)]
    async fn none_policy_single_pass_no_sleep() {
        // None => exactly one attempt, no sleep, even on a retriable
        // error (failover still happens inside create()).
        let calls = Cell::new(0u32);
        let start = tokio::time::Instant::now();
        let r: Result<u32> = retry_with(None, || {
            calls.set(calls.get() + 1);
            async { Err(Error::TimedOut) }
        })
        .await;
        assert!(matches!(r, Err(Error::TimedOut)));
        assert_eq!(calls.get(), 1, "None must run exactly one pass");
        assert_eq!(start.elapsed(), Duration::ZERO, "None must not sleep");
    }

    /// A single-attempt policy behaves like `None`, so `backon` never
    /// enters the path for the no-retry configuration.
    #[tokio::test(start_paused = true)]
    async fn single_attempt_policy_matches_none() {
        let calls = Cell::new(0u32);
        let start = tokio::time::Instant::now();
        let r: Result<u32> = retry_with(Some(policy(1, 100, 2000)), || {
            calls.set(calls.get() + 1);
            async { Err(Error::TimedOut) }
        })
        .await;
        assert!(matches!(r, Err(Error::TimedOut)));
        assert_eq!(calls.get(), 1);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn none_policy_success_first_try() {
        let r: Result<u32> = retry_with(None, || async { Ok(7) }).await;
        assert_eq!(r.unwrap(), 7);
    }

    /// The whole point of the retry loop is that the second pass takes a
    /// FRESH connection: replaying on the same dead pooled handle would
    /// fail identically every time.
    #[tokio::test]
    async fn run_with_retry_reacquires_after_a_transient_failure() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::net::TcpListener;

        use crate::tcp::handshake::HandshakeConfig;
        use crate::tcp::mock::serve_one_handshake;
        use crate::tcp::pool::{ConnectKindConfig, PoolConfig, build_pool};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            // Two handshakes: one per acquire.
            for _ in 0..2 {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                serve_one_handshake(&mut sock).await;
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
        });

        let pool = Arc::new(
            build_pool(
                vec![addr],
                ConnectKindConfig::Plain,
                HandshakeConfig::default(),
                PoolConfig {
                    max_size: 1,
                    ..PoolConfig::default()
                },
                #[cfg(feature = "tls")]
                crate::tcp::pool::TcpTls::NotConfigured,
            )
            .expect("test pool builds"),
        );

        let attempts = AtomicUsize::new(0);
        let policy = RetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        };
        let result: Result<()> = run_with_retry(&pool, Some(policy), |conn| {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if n == 0 {
                    // Poison so recycle refuses this handle; the next
                    // pass must therefore open a new connection.
                    conn.poison();
                    return Err(Error::Transient("tcp pool: simulated".to_string()));
                }
                assert!(
                    conn.is_alive(),
                    "the retry must run on a freshly created connection"
                );
                Ok(())
            }
        })
        .await;

        result.expect("the second attempt succeeds on a fresh connection");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        let _ = server.await;
    }
}
