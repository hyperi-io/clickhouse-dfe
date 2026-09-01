// Project:   clickhouse-dfe
// File:      tests/json_tcp_live.rs
// Purpose:   Live JSON-over-TCP round trip against a real ClickHouse cluster
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! JSON columns over the native protocol, against a real cluster.
//!
//! Credentials come from the env file named by `CLICKHOUSE_DFE_ENV_FILE`;
//! nothing here prints a value read from it.
//!
//! ```text
//! env CLICKHOUSE_DFE_ENV_FILE=/path/to/.env \
//!     cargo test --all-features --test json_tcp_live -- --ignored
//! ```

#![cfg(all(feature = "tcp", feature = "tls", feature = "dynamic"))]

use std::sync::OnceLock;

use serde_json::{Map, Value, json};

use clickhouse_dfe::dynamic::{ColumnDef, DynamicInsert, DynamicSchema};
use clickhouse_dfe::{Result, TcpClient};

/// Shared by every table here, so a run that dies mid-test leaves something
/// obviously disposable behind.
const TABLE_PREFIX: &str = "clickhouse_dfe_s3t2_";

struct Cluster {
    addr: String,
    host: String,
    tls: bool,
    user: String,
    password: String,
    database: String,
    cluster: String,
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
            cluster: required("CLICKHOUSE_CLUSTER"),
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
        // Synchronous insert, and a replica that has seen it before the
        // Distributed read fans out.
        .with_setting("async_insert", "0")
        .with_setting("wait_for_async_insert", "1")
        .with_setting("insert_quorum", "auto")
        .with_setting("select_sequential_consistency", "1")
}

async fn create_tables(client: &TcpClient, table: &str) -> Result<()> {
    let c = cluster();
    let (db, cl) = (&c.database, &c.cluster);
    client
        .query(&format!(
            "CREATE TABLE {db}.{table}_local ON CLUSTER {cl} \
             (id UInt64, tag String, doc JSON) \
             ENGINE = ReplicatedMergeTree ORDER BY id"
        ))
        .execute()
        .await?;
    client
        .query(&format!(
            "CREATE TABLE {db}.{table} ON CLUSTER {cl} AS {db}.{table}_local \
             ENGINE = Distributed({cl}, {db}, {table}_local, rand())"
        ))
        .execute()
        .await
}

async fn drop_tables(client: &TcpClient, table: &str) {
    let c = cluster();
    let (db, cl) = (&c.database, &c.cluster);
    for name in [table.to_string(), format!("{table}_local")] {
        let _ = client
            .query(&format!(
                "DROP TABLE IF EXISTS {db}.{name} ON CLUSTER {cl} SYNC"
            ))
            .execute()
            .await;
    }
}

fn schema(table: &str) -> DynamicSchema {
    DynamicSchema::from_columns(
        table,
        vec![
            ColumnDef::new("id", "UInt64"),
            ColumnDef::new("tag", "String"),
            ColumnDef::new("doc", "JSON"),
        ],
    )
}

fn row(id: u64, tag: &str, doc: &Value) -> Map<String, Value> {
    let mut map = Map::new();
    map.insert("id".to_string(), json!(id));
    map.insert("tag".to_string(), json!(tag));
    map.insert("doc".to_string(), doc.clone());
    map
}

async fn insert_rows(client: &TcpClient, table: &str, rows: &[(u64, String, Value)]) -> Result<()> {
    let c = cluster();
    let local = format!("{table}_local");
    let mut insert = DynamicInsert::tcp(
        client.clone(),
        &c.database,
        &local,
        schema(&format!("{}.{local}", c.database)),
    );
    for (id, tag, doc) in rows {
        insert
            .write_map(&row(*id, tag, doc))
            .await
            .map_err(|e| clickhouse_dfe::Error::Custom(e.to_string()))?;
    }
    insert
        .end()
        .await
        .map(|_| ())
        .map_err(|e| clickhouse_dfe::Error::Custom(e.to_string()))
}

/// `doc` comes back parsed, so a difference in JSON key order is not a
/// difference in the value.
async fn read_back(client: &TcpClient, table: &str) -> Result<Vec<(u64, String, Value)>> {
    let c = cluster();
    let db = &c.database;
    let blocks = client
        .query(&format!(
            "SELECT id, tag, doc FROM {db}.{table} ORDER BY id"
        ))
        .fetch_blocks()
        .await?;

    let mut out = Vec::new();
    for block in &blocks {
        let ids = block.column_as::<u64>("id")?;
        let tags = block.column_as::<String>("tag")?;
        let docs = block.column_as::<String>("doc")?;
        for ((id, tag), doc) in ids.into_iter().zip(tags).zip(docs) {
            let parsed = serde_json::from_str(&doc)
                .unwrap_or_else(|e| panic!("doc column is not JSON text: {e}"));
            out.push((id, tag, parsed));
        }
    }
    Ok(out)
}

async fn read_docs(client: &TcpClient, sql: &str) -> Result<Vec<String>> {
    let blocks = client.query(sql).fetch_blocks().await?;
    let mut out = Vec::new();
    for block in &blocks {
        out.extend(block.column_as::<String>("doc")?);
    }
    Ok(out)
}

/// Without `output_format_native_write_json_as_string` the server sends a
/// serialisation the block decoder cannot read, so the default is what makes
/// a JSON column readable at all.
#[tokio::test]
#[ignore = "needs a ClickHouse cluster -- see the module docs"]
async fn the_json_as_string_flag_is_what_makes_a_json_column_readable() {
    const SQL: &str = r#"SELECT CAST('{"a":1}', 'JSON') AS doc"#;

    let docs = read_docs(&client(), SQL)
        .await
        .expect("with the flag on, doc reads as String");
    assert_eq!(docs, vec![r#"{"a":1}"#.to_string()]);

    let without = read_docs(&client().with_json_as_string(false), SQL).await;
    assert!(
        without.is_err(),
        "with the flag off the document must not read back, got {without:?}"
    );
}

#[tokio::test]
#[ignore = "needs a ClickHouse cluster -- see the module docs"]
async fn reports_server_version() {
    let client = client();
    let blocks = client
        .query("SELECT version() AS v")
        .fetch_blocks()
        .await
        .expect("SELECT version() succeeds");
    let versions: Vec<String> = blocks
        .iter()
        .flat_map(|b| b.column_as::<String>("v").expect("v reads as String"))
        .collect();
    assert_eq!(versions.len(), 1);
    println!("clickhouse version: {}", versions[0]);
}

/// Ten rows with a JSON column and a String column go out over `FORMAT
/// Native` and come back as the same documents.
#[tokio::test]
#[ignore = "needs a ClickHouse cluster -- see the module docs"]
async fn json_rows_round_trip_over_tcp() {
    let client = client();
    let table = format!("{TABLE_PREFIX}round_trip");

    drop_tables(&client, &table).await;
    create_tables(&client, &table)
        .await
        .expect("cluster DDL succeeds");

    let expected: Vec<(u64, String, Value)> = (0u64..10)
        .map(|i| {
            let doc = json!({
                "kind": "login",
                "seq": i,
                "user": { "id": i * 7, "name": format!("u{i}") },
                "tags": ["alpha", "beta"],
            });
            (i, format!("tag{i}"), doc)
        })
        .collect();

    let written = insert_rows(&client, &table, &expected).await;
    let read = match written {
        Ok(()) => read_back(&client, &table).await,
        Err(e) => Err(e),
    };
    drop_tables(&client, &table).await;

    let read = read.expect("insert and read-back succeed");
    assert_eq!(read, expected);
}

/// A second batch introducing a path the table has never held exercises the
/// server's dynamic sub-column creation on the String -> JSON cast.
#[tokio::test]
#[ignore = "needs a ClickHouse cluster -- see the module docs"]
async fn a_new_json_path_lands_on_a_second_insert() {
    let client = client();
    let table = format!("{TABLE_PREFIX}new_path");

    drop_tables(&client, &table).await;
    create_tables(&client, &table)
        .await
        .expect("cluster DDL succeeds");

    let first: Vec<(u64, String, Value)> = (0u64..3)
        .map(|i| (i, "first".to_string(), json!({ "known": i })))
        .collect();
    let second = vec![(
        99u64,
        "second".to_string(),
        json!({ "known": 99, "brand_new_path": { "nested": "value" } }),
    )];

    let written = match insert_rows(&client, &table, &first).await {
        Ok(()) => insert_rows(&client, &table, &second).await,
        Err(e) => Err(e),
    };
    let read = match written {
        Ok(()) => read_back(&client, &table).await,
        Err(e) => Err(e),
    };
    drop_tables(&client, &table).await;

    let read = read.expect("both inserts and the read-back succeed");
    let mut expected = first;
    expected.extend(second);
    assert_eq!(read, expected);
}
