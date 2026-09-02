// Project:   clickhouse-dfe
// File:      tests/unified_live.rs
// Purpose:   UnifiedClient parity over HTTP and TCP against a real cluster
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The same statements over both transports, asserting identical answers.
//! Credentials come from `CLICKHOUSE_DFE_ENV_FILE`; nothing here prints one.
//! `env CLICKHOUSE_DFE_ENV_FILE=<path> cargo test --all-features --test unified_live -- --ignored`

#![cfg(all(
    feature = "tcp",
    feature = "tls",
    feature = "dynamic",
    feature = "unified"
))]
// Helpers sit outside #[test], so clippy's in-test exemption misses them.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::OnceLock;
use std::time::Duration;

use serde_json::{Map, Value, json};

use clickhouse_dfe::dynamic::DynamicSchemaCache;
use clickhouse_dfe::unified::{Columns, UnifiedClient};
use clickhouse_dfe::{Result, TcpClient};

/// So a run that dies mid-test leaves something obviously disposable behind.
const TABLE_PREFIX: &str = "clickhouse_dfe_s4_";

struct Cluster {
    host: String,
    native_port: String,
    http_port: String,
    tls: bool,
    user: String,
    password: String,
    database: String,
    name: String,
}

fn required(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{key} must be set in the env file"))
}

/// `dotenvy` writes the process environment, so load and read once, here.
fn cluster() -> &'static Cluster {
    static CLUSTER: OnceLock<Cluster> = OnceLock::new();
    CLUSTER.get_or_init(|| {
        let env_file = required("CLICKHOUSE_DFE_ENV_FILE");
        dotenvy::from_path(&env_file).expect("env file loads");
        Cluster {
            host: required("CLICKHOUSE_HOST"),
            native_port: required("CLICKHOUSE_NATIVE_PORT"),
            http_port: required("CLICKHOUSE_HTTP_PORT"),
            tls: required("CLICKHOUSE_TLS") == "true",
            user: required("CLICKHOUSE_USER"),
            password: std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
            database: required("CLICKHOUSE_DATABASE"),
            name: required("CLICKHOUSE_CLUSTER"),
        }
    })
}

fn tcp() -> UnifiedClient {
    let c = cluster();
    let addr = format!("{}:{}", c.host, c.native_port);
    let base = if c.tls {
        TcpClient::new_tls(addr, c.host.as_str())
    } else {
        TcpClient::new(addr)
    };
    UnifiedClient::Tcp(
        base.with_user(c.user.as_str())
            .with_password(c.password.as_str())
            .with_database(c.database.as_str())
            .with_setting("async_insert", "0")
            .with_setting("wait_for_async_insert", "1")
            .with_setting("insert_quorum", "auto")
            .with_setting("select_sequential_consistency", "1"),
    )
}

fn http() -> UnifiedClient {
    let c = cluster();
    let scheme = if c.tls { "https" } else { "http" };
    UnifiedClient::Http(
        clickhouse::Client::default()
            .with_url(format!("{scheme}://{}:{}", c.host, c.http_port))
            .with_user(c.user.as_str())
            .with_password(c.password.as_str())
            .with_database(c.database.as_str())
            .with_setting("async_insert", "0")
            .with_setting("wait_for_async_insert", "1")
            .with_setting("insert_quorum", "auto")
            .with_setting("select_sequential_consistency", "1"),
    )
}

async fn create_tables(client: &UnifiedClient, table: &str, columns: &str) -> Result<()> {
    let c = cluster();
    let (db, cl) = (&c.database, &c.name);
    client
        .execute(&format!(
            "CREATE TABLE {db}.{table}_local ON CLUSTER {cl} ({columns}) \
             ENGINE = ReplicatedMergeTree ORDER BY id"
        ))
        .await?;
    client
        .execute(&format!(
            "CREATE TABLE {db}.{table} ON CLUSTER {cl} AS {db}.{table}_local \
             ENGINE = Distributed({cl}, {db}, {table}_local, rand())"
        ))
        .await
}

async fn drop_tables(client: &UnifiedClient, table: &str) {
    let c = cluster();
    let (db, cl) = (&c.database, &c.name);
    for name in [table.to_string(), format!("{table}_local")] {
        let _ = client
            .execute(&format!(
                "DROP TABLE IF EXISTS {db}.{name} ON CLUSTER {cl} SYNC"
            ))
            .await;
    }
}

async fn on_both(sql: &str) -> Result<(Columns, Columns)> {
    let over_tcp = tcp().fetch_columns(sql).await?;
    let over_http = http().fetch_columns(sql).await?;
    Ok((over_tcp, over_http))
}

fn same_strings(pair: &(Columns, Columns), column: &str) -> Vec<String> {
    let over_tcp = pair.0.get::<String>(column).expect("tcp reads the column");
    let over_http = pair.1.get::<String>(column).expect("http reads the column");
    assert_eq!(over_tcp, over_http, "transports disagree on '{column}'");
    over_tcp
}

/// dfe-loader's five schema-shaped reads, both ways round, asserted equal.
#[tokio::test]
#[ignore = "needs a ClickHouse cluster -- see the module docs"]
async fn the_schema_queries_agree_across_transports() {
    let client = tcp();
    let table = format!("{TABLE_PREFIX}schema");
    let c = cluster();
    let db = &c.database;

    drop_tables(&client, &table).await;
    create_tables(&client, &table, "id UInt64, tag String, n Int64")
        .await
        .expect("cluster DDL succeeds");

    let outcome = run_schema_queries(db, &table).await;
    drop_tables(&client, &table).await;
    outcome.expect("every schema query succeeds on both transports");
}

async fn run_schema_queries(db: &str, table: &str) -> Result<()> {
    // String, UInt64 and UInt8 columns in one result.
    let local = format!("{table}_local");
    let columns = on_both(&format!(
        "SELECT name, type AS col_type, default_kind, position, is_in_primary_key \
         FROM system.columns WHERE database = '{db}' AND table = '{local}' ORDER BY position"
    ))
    .await?;
    assert_eq!(same_strings(&columns, "name"), ["id", "tag", "n"]);
    assert_eq!(
        same_strings(&columns, "col_type"),
        ["UInt64", "String", "Int64"]
    );
    same_strings(&columns, "default_kind");
    assert_eq!(
        columns.0.get::<u64>("position")?,
        columns.1.get::<u64>("position")?
    );
    assert_eq!(columns.0.get::<u64>("position")?, [1, 2, 3]);
    assert_eq!(
        columns.0.get::<u8>("is_in_primary_key")?,
        columns.1.get::<u8>("is_in_primary_key")?
    );

    let tables = on_both(&format!(
        "SELECT name, comment FROM system.tables \
         WHERE database = '{db}' AND name = '{local}'"
    ))
    .await?;
    assert_eq!(same_strings(&tables, "name"), [local.as_str()]);
    same_strings(&tables, "comment");

    // Narrowed so a concurrent run cannot move the list between the two calls.
    let listed = on_both(&format!(
        "SELECT name FROM system.tables \
         WHERE database = '{db}' AND name LIKE '{table}%' ORDER BY name"
    ))
    .await?;
    assert_eq!(same_strings(&listed, "name"), [table, &local]);

    let counted = on_both(&format!("SELECT count() AS _dfe_count FROM {db}.{table}")).await?;
    assert_eq!(counted.0.get::<u64>("_dfe_count")?, [0]);
    assert_eq!(
        counted.0.get::<u64>("_dfe_count")?,
        counted.1.get::<u64>("_dfe_count")?
    );

    let one = on_both("SELECT 1 AS one").await?;
    assert_eq!(one.0.get::<u8>("one")?, [1]);
    assert_eq!(one.0.get::<u8>("one")?, one.1.get::<u8>("one")?);
    tcp().ping().await?;
    http().ping().await
}

fn row(id: u64, tag: &str, n: i64) -> Map<String, Value> {
    let mut map = Map::new();
    map.insert("id".to_string(), json!(id));
    map.insert("tag".to_string(), json!(tag));
    map.insert("n".to_string(), json!(n));
    map
}

/// Five rows down each transport, read back through the Distributed table.
#[tokio::test]
#[ignore = "needs a ClickHouse cluster -- see the module docs"]
async fn dynamic_insert_round_trips_on_both_transports() {
    let client = tcp();
    let table = format!("{TABLE_PREFIX}insert");
    let c = cluster();
    let db = &c.database;

    drop_tables(&client, &table).await;
    create_tables(&client, &table, "id UInt64, tag String, n Int64")
        .await
        .expect("cluster DDL succeeds");

    let outcome = insert_and_read_back(db, &table).await;
    drop_tables(&client, &table).await;

    let (ids, tags, ns) = outcome.expect("both inserts and the read-back succeed");
    assert_eq!(ids, (0..5).chain(100..105).collect::<Vec<u64>>());
    assert_eq!(tags.len(), 10);
    assert!(tags[..5].iter().all(|t| t == "tcp"));
    assert!(tags[5..].iter().all(|t| t == "http"));
    assert_eq!(
        ns,
        ids.iter()
            .map(|id| -i64::try_from(*id).expect("test ids are small"))
            .collect::<Vec<i64>>()
    );
}

async fn insert_and_read_back(db: &str, table: &str) -> Result<(Vec<u64>, Vec<String>, Vec<i64>)> {
    let local = format!("{table}_local");

    // A cache each, so both schema paths actually run.
    for (client, tag, base) in [(tcp(), "tcp", 0u64), (http(), "http", 100)] {
        let cache = DynamicSchemaCache::new(Duration::from_secs(300));
        let mut insert = client.dynamic_insert(db, &local, cache).await?;
        for i in 0..5 {
            insert
                .write_map(&row(
                    base + i,
                    tag,
                    -i64::try_from(base + i).expect("test ids are small"),
                ))
                .await
                .map_err(|e| clickhouse_dfe::Error::Custom(e.to_string()))?;
        }
        insert
            .end()
            .await
            .map_err(|e| clickhouse_dfe::Error::Custom(e.to_string()))?;
    }

    // `select_sequential_consistency` does not hold one shard for another, so
    // a Distributed read straight after two per-replica inserts races
    // replication unless the replicas are levelled first.
    tcp()
        .execute(&format!(
            "SYSTEM SYNC REPLICA ON CLUSTER {} {db}.{local}",
            cluster().name
        ))
        .await?;

    let read = on_both(&format!("SELECT id, tag, n FROM {db}.{table} ORDER BY id")).await?;
    let ids = read.0.get::<u64>("id")?;
    assert_eq!(ids, read.1.get::<u64>("id")?, "transports disagree on 'id'");
    let ns = read.0.get::<i64>("n")?;
    assert_eq!(ns, read.1.get::<i64>("n")?, "transports disagree on 'n'");
    Ok((ids, same_strings(&read, "tag"), ns))
}

/// JSON reads the same on both transports. The HTTP arm gets there by asking
/// the server for the TCP wire shape (`client_protocol_version`) and decoding
/// it with this crate's codec, rather than upstream's Native reader, which
/// refuses the declared type outright.
#[tokio::test]
#[ignore = "needs a ClickHouse cluster -- see the module docs"]
async fn json_columns_read_the_same_on_both_transports() {
    const SQL: &str = r#"SELECT CAST('{"a":1}', 'JSON') AS doc"#;

    let over_tcp = tcp().fetch_columns(SQL).await.expect("tcp reads JSON");
    let over_http = http().fetch_columns(SQL).await.expect("http reads JSON");

    assert_eq!(over_tcp.get::<String>("doc").unwrap(), [r#"{"a":1}"#]);
    assert_eq!(
        over_http.get::<String>("doc").unwrap(),
        over_tcp.get::<String>("doc").unwrap(),
        "a JSON column must read identically on both transports"
    );
}

/// Variant and Dynamic take the same route as JSON: one document per row, and
/// the same document whichever transport carried it.
#[tokio::test]
#[ignore = "needs a ClickHouse cluster -- see the module docs"]
async fn semi_structured_columns_read_the_same_on_both_transports() {
    // CAST to Variant only accepts a type the Variant already lists, so the
    // literal is widened to UInt64 first rather than left as UInt8.
    const SQL: &str = "SELECT CAST(42::UInt64, 'Variant(UInt64, String)') AS v, \
                       CAST('hello', 'Dynamic') AS d";

    let over_tcp = tcp().fetch_columns(SQL).await.expect("tcp reads them");
    let over_http = http().fetch_columns(SQL).await.expect("http reads them");

    for column in ["v", "d"] {
        let tcp_values = over_tcp.get::<String>(column).unwrap();
        assert_eq!(tcp_values.len(), 1, "{column} must carry one row");
        assert_eq!(
            over_http.get::<String>(column).unwrap(),
            tcp_values,
            "{column} must read identically on both transports"
        );
    }
}
