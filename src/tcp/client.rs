//! Standalone [`TcpClient`] over the ClickHouse native protocol.
//!
//! `TcpClient` owns its own endpoint list, credentials, settings, roles
//! and TLS trust rather than borrowing them from `clickhouse::Client`,
//! whose equivalent fields are private and have no public readers.
//!
//! # Surface limits
//!
//! - No JWT / access-token arm -- the TCP protocol has no JWT auth path.
//! - No shared HTTP transport -- a consumer wanting both keeps a
//!   `clickhouse::Client` alongside a `TcpClient`.
//! - Reads are column-typed, not row-typed: RowBinary bytes in,
//!   `DecodedBlock` out, values by column name via
//!   [`crate::native::FromColumn`].

use std::sync::Arc;
use std::time::Duration;

use crate::error::Result;
use crate::tcp::client_ext::{
    self, TcpInsertSession, execute_query_via_pool, execute_stream_via_pool, insert_native_via_pool,
};
use crate::tcp::cursor::TcpRawCursor;
use crate::tcp::handshake::HandshakeConfig;
use crate::tcp::pool::{ConnectKindConfig, NativePool, PoolConfig, TcpClientConfig, build_pool};
use crate::tcp::query::TcpQuery;
use crate::tcp::retry::RetryPolicy;

/// Setting names ClickHouse itself defines, as protocol-level string
/// literals.
mod settings {
    pub(super) const DATABASE: &str = "database";
    pub(super) const ROLE: &str = "role";
    /// Makes the server emit JSON columns as String in Native output; the
    /// block decoder has no reader for the path-based serialisation.
    pub(super) const JSON_AS_STRING: &str = "output_format_native_write_json_as_string";
}

/// A ClickHouse client that speaks ONLY the native TCP protocol
/// (port 9000).
///
/// Owns its endpoint list, credentials, per-session settings, role set,
/// retry policy and TLS trust, plus the deadpool connection pool built
/// from them. Cloning shares the pool.
///
/// Every `with_*` builder that feeds the handshake or the pool dials
/// rebuilds the pool.
#[derive(Clone)]
pub struct TcpClient {
    config: TcpClientConfig,
    /// Session settings sent in each Query packet (not handshake).
    settings: Vec<(String, String)>,
    /// `SET ROLE`-equivalent, emitted as repeated `role` settings.
    roles: Vec<String>,
    /// Send [`settings::JSON_AS_STRING`] with every query.
    json_as_string: bool,
    /// Explicit TLS trust. `None` means "not configured" -- the pool's
    /// TLS arm then resolves the default native+webpki anchors.
    #[cfg(feature = "tls")]
    tls: Option<crate::tls::TlsConfigSource>,
    pool: Arc<NativePool>,
}

impl TcpClient {
    /// Open a plain-TCP client against a single `host:port` endpoint.
    ///
    /// # Panics
    /// If the pool cannot be built. `Runtime::Tokio1` is always set, so
    /// the documented `NoRuntimeSpecified` failure is unreachable.
    pub fn new(addr: impl Into<String>) -> Self {
        let mut config = TcpClientConfig {
            endpoints: vec![addr.into()],
            ..TcpClientConfig::default()
        };
        // `TcpClientConfig::default()` derives `HandshakeConfig::default()`
        // via `Default`, which already carries default/default/empty.
        config.handshake = HandshakeConfig::default();
        let mut this = Self {
            config,
            settings: Vec::new(),
            roles: Vec::new(),
            json_as_string: true,
            #[cfg(feature = "tls")]
            tls: None,
            // Placeholder replaced immediately by `rebuild`; the pool is
            // not `Option` on the happy path so callers never see a
            // half-built client.
            pool: Arc::new(
                build_pool(
                    vec!["127.0.0.1:9000".to_string()],
                    ConnectKindConfig::Plain,
                    HandshakeConfig::default(),
                    PoolConfig::default(),
                    #[cfg(feature = "tls")]
                    crate::tcp::pool::TcpTls::NotConfigured,
                )
                .expect("tcp: placeholder pool build cannot fail (runtime is always set)"),
            ),
        };
        this.rebuild();
        this
    }

    /// Open a TLS-over-TCP client. `server_name` is the SNI / cert name.
    #[cfg(feature = "tls")]
    pub fn new_tls(addr: impl Into<String>, server_name: impl Into<String>) -> Self {
        let mut this = Self::new(addr);
        this.config.kind = ConnectKindConfig::Tls {
            server_name: server_name.into(),
        };
        this.rebuild();
        this
    }

    /// Replace the endpoint list. Round-robin failover across it happens
    /// inside the pool manager's `create`.
    ///
    /// # Panics
    /// If `addrs` yields no endpoints.
    pub fn with_addrs(mut self, addrs: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let endpoints: Vec<String> = addrs.into_iter().map(Into::into).collect();
        assert!(
            !endpoints.is_empty(),
            "with_addrs requires at least one endpoint"
        );
        self.config.endpoints = endpoints;
        self.rebuild();
        self
    }

    /// Handshake database, and the `database` session setting for
    /// queries and INSERTs.
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.config.handshake.database = database.into();
        self.rebuild();
        self
    }

    /// Handshake user.
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.config.handshake.user = user.into();
        self.rebuild();
        self
    }

    /// Handshake password.
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.config.handshake.password = password.into();
        self.rebuild();
        self
    }

    /// Quota key emitted in the post-Hello addendum.
    pub fn with_quota_key(mut self, quota_key: impl Into<String>) -> Self {
        self.config.handshake.quota_key = quota_key.into();
        self.rebuild();
        self
    }

    /// Add one per-session setting, sent in every Query packet.
    /// No pool rebuild: settings are dispatch-time, not handshake-time.
    pub fn with_setting(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.push((name.into(), value.into()));
        self
    }

    /// Ask the server to send JSON columns as String, which is the default.
    /// Turning it off yields the path-based JSON serialisation, which
    /// [`crate::native::DecodedBlock`] does not materialise.
    pub fn with_json_as_string(mut self, on: bool) -> Self {
        self.json_as_string = on;
        self
    }

    /// Replace the role set. Emitted as repeated `role` settings.
    pub fn with_roles(mut self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.roles = roles.into_iter().map(Into::into).collect();
        self
    }

    /// Bounded-backoff retry for idempotent operations. Dispatch-time,
    /// so no pool rebuild.
    pub fn with_retry(mut self, retry: impl Into<Option<RetryPolicy>>) -> Self {
        self.config.retry = retry.into();
        self
    }

    /// Maximum pooled connections.
    pub fn with_pool_size(mut self, n: usize) -> Self {
        self.config.pool.max_size = n;
        self.rebuild();
        self
    }

    /// How long a caller may wait for a free pool slot.
    pub fn with_pool_acquire_timeout(mut self, d: Option<Duration>) -> Self {
        self.config.pool.acquire_timeout = d;
        self.rebuild();
        self
    }

    /// How long `Manager::create` (connect + handshake) may take.
    pub fn with_pool_create_timeout(mut self, d: Option<Duration>) -> Self {
        self.config.pool.create_timeout = d;
        self.rebuild();
        self
    }

    /// Maximum age of a pooled connection before recycle drops it.
    pub fn with_pool_max_lifetime(mut self, d: Option<Duration>) -> Self {
        self.config.pool.max_lifetime = d;
        self.rebuild();
        self
    }

    /// Per-packet idle read timeout threaded into every spawned actor.
    pub fn with_read_timeout(mut self, d: Option<Duration>) -> Self {
        self.config.pool.read_timeout = d;
        self.rebuild();
        self
    }

    /// Supply an already-built rustls config (Go's `Options.TLS` analog).
    #[cfg(feature = "tls")]
    pub fn with_tls_config(mut self, cfg: Arc<rustls::ClientConfig>) -> Self {
        self.tls = Some(crate::tls::TlsConfigSource::Explicit(cfg));
        self.rebuild();
        self
    }

    /// Supply a declarative trust description, resolved at pool build.
    #[cfg(feature = "tls")]
    pub fn with_tls_trust(mut self, trust: crate::tls::TlsTrust) -> Self {
        self.tls = Some(crate::tls::TlsConfigSource::Trust(trust));
        self.rebuild();
        self
    }

    /// The underlying pool, exposed so callers can read deadpool's
    /// `status()` for metrics.
    pub fn pool(&self) -> &Arc<NativePool> {
        &self.pool
    }

    /// The configured retry policy, if any.
    pub fn retry(&self) -> Option<RetryPolicy> {
        self.config.retry
    }

    /// Per-query / per-INSERT settings: database, the JSON-as-String flag,
    /// plain settings, roles.
    pub fn insert_settings(&self) -> Vec<(String, String)> {
        let mut out = Vec::with_capacity(2 + self.settings.len() + self.roles.len());
        out.push((
            settings::DATABASE.to_string(),
            self.config.handshake.database.clone(),
        ));
        if self.json_as_string {
            out.push((settings::JSON_AS_STRING.to_string(), "1".to_string()));
        }
        out.extend(self.settings.iter().cloned());
        for role in &self.roles {
            out.push((settings::ROLE.to_string(), role.clone()));
        }
        out
    }

    /// Round-trip a Ping over a pooled connection.
    pub async fn ping(&self) -> Result<()> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(crate::tcp::retry::map_pool_error)?;
        conn.ping().await
    }

    /// Stage `sql` for [`TcpQuery::execute`] or
    /// [`TcpQuery::fetch_blocks`].
    pub fn query(&self, sql: &str) -> TcpQuery<'_> {
        TcpQuery::new(self, sql)
    }

    /// Run a statement that does not stream rows (DDL, `SET`,
    /// `INSERT ... VALUES`, a mutation). Auto-retry is opt-in via
    /// `idempotent` -- see [`execute_query_via_pool`].
    pub async fn execute_query(&self, query_id: &str, query: &str, idempotent: bool) -> Result<()> {
        execute_query_via_pool(
            &self.pool,
            query_id,
            query,
            &self.insert_settings(),
            self.config.retry,
            idempotent,
        )
        .await
    }

    /// Open a streaming SELECT, yielding decoded Native blocks.
    ///
    /// Row-typed `fetch::<T>()` needs upstream's `pub(crate)` RowBinary
    /// deserialiser and is not available yet; read columns by name off
    /// each [`crate::native::DecodedBlock`] instead.
    pub async fn execute_stream(&self, query_id: &str, query: &str) -> Result<TcpRawCursor> {
        execute_stream_via_pool(
            &self.pool,
            query_id,
            query,
            &self.insert_settings(),
            self.config.retry,
        )
        .await
    }

    /// Open an INSERT session. `sql` is the full statement, typically
    /// `INSERT INTO <table> (...) FORMAT Native`.
    ///
    /// Blocks are supplied as bytes already encoded by
    /// [`crate::native::encode_columns`].
    pub async fn insert_native(&self, query_id: &str, sql: &str) -> Result<TcpInsertSession> {
        insert_native_via_pool(&self.pool, query_id, sql, &self.insert_settings()).await
    }

    /// Default rows per Native block on the insert path.
    pub const DEFAULT_INSERT_BLOCK_ROWS: u64 = client_ext::DEFAULT_TCP_INSERT_BLOCK_ROWS;

    /// Rebuild the pool from the current config.
    fn rebuild(&mut self) {
        #[cfg(feature = "tls")]
        let tls = match &self.tls {
            None => crate::tcp::pool::TcpTls::NotConfigured,
            Some(src) => match crate::tls::build_client_config(src) {
                Ok(cfg) => crate::tcp::pool::TcpTls::Resolved(cfg),
                // Fail closed: a configured-but-unresolvable trust must
                // never fall back to broad default anchors.
                Err(_) => crate::tcp::pool::TcpTls::ConfiguredButFailed,
            },
        };
        self.pool = Arc::new(
            build_pool(
                self.config.endpoints.clone(),
                self.config.kind.clone(),
                self.config.handshake.clone(),
                self.config.pool,
                #[cfg(feature = "tls")]
                tls,
            )
            .expect("tcp: pool rebuild failed despite Runtime::Tokio1 being set (unreachable)"),
        );
    }
}
