//! TCP-side dispatch helpers: acquire a connection from the pool,
//! run a query / streaming SELECT / INSERT session, and poison the
//! handle on failure so the pool's recycle path drops the broken
//! connection.
//!
//! These are the glue between the public [`crate::tcp::TcpClient`]
//! surface and the connection-actor primitives. Cancel-on-drop,
//! full-duplex Exception detection and Cancel-plus-drain on
//! receiver-drop all live inside [`crate::tcp::connection_actor`]; this
//! module only routes pooled handles into it.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::tcp::connection_actor::ConnectionHandle;
use crate::tcp::cursor::TcpRawCursor;
use crate::tcp::pool::NativePool;
use crate::tcp::retry::{RetryPolicy, map_pool_error, run_with_retry};

/// Default rows per Native block on the TCP insert path. Matches
/// ClickHouse server's `max_insert_block_size` default; keeps the
/// merge pipeline's batching cadence aligned and avoids the tiny-
/// block merge churn that smaller batches would trigger.
pub const DEFAULT_TCP_INSERT_BLOCK_ROWS: u64 = 1_000_000;

/// Acquire a connection and run a query that does not stream rows
/// (DDL, `SET`, `INSERT ... VALUES`, etc.). Poisons the handle on
/// error so the pool's recycle path drops it on return.
///
/// `query_id` is forwarded into `ClientInfo.initial_query_id` so the
/// server's `system.query_log.initial_query_id` matches what the
/// caller uses for tracing and for `KILL QUERY WHERE query_id = ?`.
///
/// # Retry safety
///
/// `ExecuteQuery` covers arbitrary statements, many of which are NOT
/// safe to replay (a non-idempotent `INSERT ... VALUES`, a mutation).
/// Auto-retry is therefore OPT-IN: it is enabled only when the caller
/// asserts idempotency. When `idempotent` is `false` this runs a single
/// acquire pass regardless of `retry` -- endpoint failover still happens
/// inside the pool manager's `create`, but no statement is replayed.
///
/// # Errors
///
/// A pool-acquire failure, [`Error::ServerException`] if the server
/// rejected the statement, or a transport error.
pub async fn execute_query_via_pool(
    pool: &Arc<NativePool>,
    query_id: &str,
    query: &str,
    settings: &[(String, String)],
    params: &[(String, String)],
    retry: Option<RetryPolicy>,
    idempotent: bool,
) -> Result<()> {
    // Only the idempotent path consults `retry`; a non-idempotent
    // statement gets exactly one pass (no replay) by passing `None`.
    let effective = if idempotent { retry } else { None };
    run_with_retry(pool, effective, |conn| async move {
        let result = conn
            .execute_query(
                query_id.to_string(),
                query.to_string(),
                settings.to_vec(),
                params.to_vec(),
            )
            .await;
        if let Err(ref e) = result {
            poison_unless_server_rejected(&conn, e);
        }
        result
    })
    .await
}

/// Acquire a connection and open a streaming SELECT.
///
/// Returns a [`TcpRawCursor`] that yields decoded blocks until the
/// server emits `EndOfStream`. The cursor holds the pool's
/// `Object<TcpConnectionManager>` for its whole life: releasing the slot
/// at dispatch time would let the next `pool.get()` hand out a connection
/// that is still streaming. Dropping the cursor drops both the receiver
/// -- which trips the actor's `results.closed()` watch and triggers
/// protocol Cancel + drain -- and the slot, which recycles the
/// connection.
///
/// # Retry safety
///
/// The retried region is the query *issue* only -- `pool.get()` plus
/// opening the cursor. No row is consumed inside it, so a transient
/// connect/issue failure (including a silently-dead pooled connection)
/// is replayed safely per `retry`. Once the cursor is returned, its
/// later `next_block()` failures happen AFTER the retried region and
/// surface to the caller WITHOUT auto-retry. A SELECT is always
/// retry-eligible (subject to the policy); the caller passes the
/// client's configured `retry`.
///
/// # Errors
///
/// A pool-acquire failure, or any error from issuing the query.
pub async fn execute_stream_via_pool(
    pool: &Arc<NativePool>,
    query_id: &str,
    query: &str,
    settings: &[(String, String)],
    params: &[(String, String)],
    retry: Option<RetryPolicy>,
) -> Result<TcpRawCursor> {
    // `op` is `Fn` (re-run per attempt), so clone the owned inputs
    // inside the closure rather than moving them out once.
    run_with_retry(pool, retry, |conn| {
        let query_id = query_id.to_string();
        let query = query.to_string();
        let settings = settings.to_vec();
        let params = params.to_vec();
        async move {
            match conn
                .execute_stream_cursor(query_id, query, settings, params)
                .await
            {
                Ok(mut cursor) => {
                    cursor.hold_pool_slot(conn);
                    Ok(cursor)
                }
                Err(e) => {
                    poison_unless_server_rejected(&conn, &e);
                    Err(e)
                }
            }
        }
    })
    .await
}

/// Poison `conn` unless the server merely rejected the statement.
///
/// A server Exception leaves the socket in a known-good state -- the
/// actor keeps the connection and the next caller can reuse it -- so
/// poisoning on one throws away a healthy connection per failed query.
fn poison_unless_server_rejected(conn: &ConnectionHandle, e: &Error) {
    if !matches!(e, Error::ServerException { .. }) {
        conn.poison();
    }
}

/// Pool-acquired INSERT session.
///
/// Holds the pooled [`deadpool::managed::Object`] for the whole session
/// so the actor keeps the socket exclusive across
/// `BeginInsert` -> N x `SendInsertBlock` -> `FinishInsert`. Dropping the
/// session without [`Self::finish`] or [`Self::abort`] returns the
/// connection to the pool still in `InsertActive`, and the next acquirer
/// gets an out-of-state error.
#[non_exhaustive]
pub struct TcpInsertSession {
    handle: deadpool::managed::Object<crate::tcp::pool::TcpConnectionManager>,
    /// Column metadata the server echoed in its schema block.
    /// Forwarded so callers can reconcile against their declared schema
    /// if needed.
    pub server_columns: Vec<(String, String)>,
    /// Negotiated server protocol revision from the handshake. The Native
    /// encoder uses THIS (not a hardcoded constant) so the per-column
    /// custom_serialization flag presence matches what the server expects
    /// for its revision -- a server below the custom-serialization revision
    /// must NOT receive the flag.
    pub server_revision: u64,
}

impl TcpInsertSession {
    /// Send one Native block. `column_bytes` is the pre-encoded
    /// payload from [`crate::native::encode_columns`]; the actor is
    /// purely a transport.
    ///
    /// # Errors
    ///
    /// [`Error::ServerException`] if the server rejected an earlier
    /// block (full-duplex detection), or an I/O error from the writer.
    pub async fn send_block(
        &self,
        column_bytes: Vec<u8>,
        num_columns: u64,
        num_rows: u64,
    ) -> Result<()> {
        let result = self
            .handle
            .send_insert_block(column_bytes, num_columns, num_rows)
            .await;
        if let Err(ref e) = result {
            poison_unless_server_rejected(&self.handle, e);
        }
        result
    }

    /// Terminate the INSERT session and return the connection to the
    /// pool. Drains response packets to EndOfStream.
    ///
    /// # Errors
    ///
    /// [`Error::ServerException`] if the server rejected the INSERT on
    /// commit, or an I/O or drain-timeout error.
    pub async fn finish(self) -> Result<()> {
        let result = self.handle.finish_insert().await;
        if let Err(ref e) = result {
            poison_unless_server_rejected(&self.handle, e);
        }
        result
    }

    /// Poison the connection and drop the session. The pool's recycle
    /// path will refuse the handle and `Manager::create` opens a new
    /// one for the next caller.
    pub fn abort(self) {
        self.handle.poison();
    }
}

/// Acquire a connection and open an INSERT session against `table`
/// using the supplied SQL.
///
/// `sql` is the full INSERT statement, typically
/// `"INSERT INTO <table> (...) FORMAT Native"`. The actor sends the
/// Query packet, drains protocol chatter until the server's schema
/// block, and replies with the `(name, type_name)` column pairs the
/// schema block carried.
///
/// # Errors
///
/// A pool-acquire failure, [`Error::ServerException`] if the server
/// rejected the INSERT before its schema block, or an I/O error.
pub async fn insert_native_via_pool(
    pool: &Arc<NativePool>,
    query_id: &str,
    sql: &str,
    settings: &[(String, String)],
) -> Result<TcpInsertSession> {
    let conn = pool.get().await.map_err(map_pool_error)?;
    let server_columns = match conn
        .begin_insert(
            query_id.to_string(),
            sql.to_string(),
            settings.to_vec(),
            Vec::new(),
        )
        .await
    {
        Ok(cols) => cols,
        Err(e) => {
            poison_unless_server_rejected(&conn, &e);
            return Err(e);
        }
    };
    // Capture the negotiated revision before `conn` is moved into the
    // session, so the encoder can match the server's framing exactly.
    let server_revision = conn.server_hello().revision;
    Ok(TcpInsertSession {
        handle: conn,
        server_columns,
        server_revision,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    use crate::native::io::ClickHouseWrite;
    use crate::tcp::handshake::HandshakeConfig;
    use crate::tcp::mock::{serve_one_handshake, write_schema_block, write_uint64_payload_block};
    use crate::tcp::pool::{ConnectKindConfig, PoolConfig, build_pool};
    use crate::tcp::protocol::ServerPacketId;
    use crate::tcp::retry::is_retriable_transport;

    /// Build a one-connection pool against `addr`.
    fn pool_of(addr: String, max_size: usize) -> Arc<NativePool> {
        Arc::new(
            build_pool(
                vec![addr],
                ConnectKindConfig::Plain,
                HandshakeConfig::default(),
                PoolConfig {
                    max_size,
                    ..PoolConfig::default()
                },
                #[cfg(feature = "tls")]
                crate::tcp::pool::TcpTls::NotConfigured,
            )
            .expect("test pool builds"),
        )
    }

    async fn write_exception(sock: &mut TcpStream, code: i32, message: &str) {
        sock.write_var_uint(ServerPacketId::Exception as u64)
            .await
            .unwrap();
        sock.write_i32_le(code).await.unwrap();
        sock.write_string(b"DB::Exception").await.unwrap();
        sock.write_string(message.as_bytes()).await.unwrap();
        sock.write_string(b"").await.unwrap();
        AsyncWriteExt::write_u8(sock, 0).await.unwrap();
        sock.flush().await.unwrap();
    }

    /// A rejected statement leaves the socket in a known-good state, so
    /// poisoning on it would throw away a healthy connection per failed
    /// query and force a reconnect the server never asked for.
    #[tokio::test]
    async fn execute_query_server_exception_does_not_poison_the_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            serve_one_handshake(&mut sock).await;
            write_exception(&mut sock, 60, "no such table").await;
            // Hold the socket so the connection stays usable.
            tokio::time::sleep(Duration::from_millis(300)).await;
            sock
        });

        let pool = pool_of(addr, 1);
        let err = execute_query_via_pool(&pool, "q", "SELECT 1", &[], &[], None, false)
            .await
            .expect_err("the server rejected the statement");
        assert!(matches!(err, Error::ServerException { code: 60, .. }));

        // The slot was returned, and recycle kept it: a second acquire
        // finds the same live connection rather than opening another.
        let conn = pool.get().await.expect("acquire after a rejection");
        assert!(
            conn.is_alive(),
            "a server rejection must not poison the connection"
        );

        drop(conn);
        let _sock = server.await.unwrap();
    }

    /// `map_pool_error` must not stringify a backend failure: the retry
    /// classifier reads the typed variant, and a refused connect that
    /// arrives as `Custom` would be treated as terminal.
    #[tokio::test]
    async fn insert_native_pool_error_stays_typed() {
        // A listener bound then dropped frees its port, so connects to
        // it are refused rather than hanging.
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let refusing = dead.local_addr().unwrap().to_string();
        drop(dead);

        let pool = pool_of(refusing, 1);
        // `TcpInsertSession` is not `Debug`, so match rather than
        // `expect_err`.
        let err = match insert_native_via_pool(&pool, "q", "INSERT INTO t FORMAT Native", &[]).await
        {
            Ok(_) => panic!("every endpoint refuses; the insert must not open"),
            Err(e) => e,
        };
        assert!(
            !matches!(err, Error::Custom(_)),
            "a backend failure must stay typed, got {err:?}"
        );
        assert!(
            is_retriable_transport(&err),
            "a refused connect must classify as retriable, got {err:?}"
        );
    }

    /// Releasing the slot at dispatch time would let the next `get()`
    /// hand out a connection that is still streaming, and the two
    /// callers would interleave on one socket.
    #[tokio::test]
    async fn streaming_select_holds_its_pool_slot_until_the_cursor_drops() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            serve_one_handshake(&mut sock).await;
            write_schema_block(&mut sock, &[("n", "UInt64")]).await;
            write_uint64_payload_block(&mut sock, &[1, 2, 3]).await;
            sock.write_var_uint(ServerPacketId::EndOfStream as u64)
                .await
                .unwrap();
            sock.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;
            sock
        });

        let pool = pool_of(addr, 1);
        let cursor = execute_stream_via_pool(&pool, "q", "SELECT number AS n", &[], &[], None)
            .await
            .expect("stream opens");

        // The only slot is taken while the cursor lives.
        let blocked = tokio::time::timeout(Duration::from_millis(150), pool.get()).await;
        assert!(
            blocked.is_err(),
            "the cursor must hold its pool slot for its whole life"
        );

        drop(cursor);

        // Dropping it returns the slot.
        let freed = tokio::time::timeout(Duration::from_secs(2), pool.get())
            .await
            .expect("the slot must free once the cursor drops");
        assert!(freed.is_ok(), "re-acquire after a cursor drop");

        let _sock = server.await.unwrap();
    }
}
