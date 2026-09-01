// Project:   clickhouse-dfe
// File:      tests/tcp_params_live.rs
// Purpose:   Server-side query parameters over TCP, against a real cluster
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Bound `{name:Type}` parameters over the native protocol, against a
//! real cluster.
//!
//! Credentials come from the env file named by `CLICKHOUSE_DFE_ENV_FILE`;
//! nothing here prints a value read from it.
//!
//! ```text
//! env CLICKHOUSE_DFE_ENV_FILE=/path/to/.env \
//!     cargo test --all-features --test tcp_params_live -- --ignored
//! ```

#![cfg(all(feature = "tcp", feature = "tls"))]
// Helpers sit outside #[test], so clippy's in-test exemption misses them.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::OnceLock;

use clickhouse_dfe::{Error, TcpClient};

struct Cluster {
    addr: String,
    host: String,
    tls: bool,
    user: String,
    password: String,
    database: String,
}

fn required(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{key} must be set in the env file"))
}

/// `dotenvy` writes the process environment, so the load and every read of
/// it happen once, inside this initialiser.
fn cluster() -> &'static Cluster {
    static CLUSTER: OnceLock<Cluster> = OnceLock::new();
    CLUSTER.get_or_init(|| {
        let env_file = required("CLICKHOUSE_DFE_ENV_FILE");
        dotenvy::from_path(&env_file).expect("env file loads");
        Cluster {
            addr: format!(
                "{}:{}",
                required("CLICKHOUSE_HOST"),
                required("CLICKHOUSE_NATIVE_PORT")
            ),
            host: required("CLICKHOUSE_HOST"),
            tls: required("CLICKHOUSE_TLS") == "true",
            user: required("CLICKHOUSE_USER"),
            password: std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
            database: required("CLICKHOUSE_DATABASE"),
        }
    })
}

fn client() -> TcpClient {
    let c = cluster();
    let base = if c.tls {
        TcpClient::new_tls(c.addr.as_str(), c.host.as_str())
    } else {
        TcpClient::new(c.addr.as_str())
    };
    base.with_user(c.user.as_str())
        .with_password(c.password.as_str())
        .with_database(c.database.as_str())
}

/// The parameters section is what makes `{name:Type}` resolvable at all:
/// without it the server rejects the placeholder with `UNKNOWN_QUERY_PARAMETER`
/// (456).
#[tokio::test]
#[ignore = "needs a ClickHouse cluster -- see the module docs"]
async fn tcp_query_binds_server_side_parameters() {
    let client = client();
    let db = &cluster().database;

    let blocks = client
        .query(
            "SELECT count() AS n FROM system.columns \
             WHERE database = {database:String}",
        )
        .with_query_id("dfe_params_bound")
        .param("database", format!("'{db}'"))
        .fetch_blocks()
        .await
        .expect("a bound String parameter must resolve server-side");

    let counts: Vec<u64> = blocks
        .iter()
        .flat_map(|b| b.column_as::<u64>("n").expect("n reads as UInt64"))
        .collect();
    assert_eq!(counts.len(), 1, "one aggregate row");
    println!(
        "system.columns rows for the configured database: {}",
        counts[0]
    );
}

/// The parameters section carries a `ClickHouse` Field dump, so a number
/// travels quoted -- as a String field -- and the server casts it to the
/// type the placeholder declares.
#[tokio::test]
#[ignore = "needs a ClickHouse cluster -- see the module docs"]
async fn tcp_query_binds_a_numeric_parameter() {
    let blocks = client()
        .query("SELECT {n:UInt64} + 1 AS v")
        .with_query_id("dfe_params_numeric")
        .param("n", "'41'")
        .fetch_blocks()
        .await
        .expect("a bound UInt64 parameter must resolve server-side");

    let values: Vec<u64> = blocks
        .iter()
        .flat_map(|b| b.column_as::<u64>("v").expect("v reads as UInt64"))
        .collect();
    assert_eq!(values, vec![42]);
    println!("bound numeric parameter resolved to: {}", values[0]);
}

/// Referencing a parameter the client never bound is the failure the
/// parameters section exists to prevent, and it must surface typed.
#[tokio::test]
#[ignore = "needs a ClickHouse cluster -- see the module docs"]
async fn an_unbound_parameter_is_a_typed_server_error() {
    let err = client()
        .query("SELECT {missing:String} AS v")
        .with_query_id("dfe_params_unbound")
        .fetch_blocks()
        .await
        .expect_err("an unbound placeholder must be rejected");

    match err {
        Error::ServerException { code, .. } => {
            println!("unbound parameter surfaced as server error code {code}");
            assert_eq!(code, 456, "UNKNOWN_QUERY_PARAMETER");
        }
        other => panic!("expected a typed ServerException, got {other:?}"),
    }
}
