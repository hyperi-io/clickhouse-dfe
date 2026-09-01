// Project:   clickhouse-dfe
// File:      tests/wire_docker.rs
// Purpose:   Native wire round trip against a pinned ClickHouse in Docker
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Type-matrix proof against a real server, not a mock.
//!
//! One container per test, pinned to the version the devex cluster runs and
//! stopped before the test returns. The server builds every value, so a
//! failure here is this crate's decoder rather than its encoder; the encoder
//! is covered separately, and by one test that uses the server as its oracle.
//!
//! Run with `--test-threads=1`: one ClickHouse at a time on the host.
//!
//! ```text
//! cargo test --all-features --test wire_docker -- --include-ignored --test-threads=1
//! ```

#![cfg(all(feature = "tcp", feature = "unified", feature = "dynamic"))]
// Helpers sit outside #[test], so clippy's in-test exemption misses them.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use clickhouse::Client;
use clickhouse_dfe::native::DecodedBlock;
use clickhouse_dfe::{Columns, TcpClient, UnifiedClient};

/// The version the devex cluster runs. Never `latest`: the wire format moves.
const IMAGE_TAG: &str = "26.3.21.7";

/// Enough for the matrix, far below what an unbounded server would take.
const CONTAINER_MEMORY_BYTES: i64 = 2 * 1024 * 1024 * 1024;

struct Server {
    container: ContainerAsync<GenericImage>,
    native_port: u16,
    http_port: u16,
}

/// Start a server for ONE test, ready to answer on both ports.
///
/// Owned by the test and stopped when it returns. A `static` shared across
/// the binary would be faster, and was the first shape of this: a `static` is
/// never dropped at process exit, so every run left a server behind.
/// `testcontainers`' `watchdog` does not cover a normal exit either.
async fn server() -> Server {
    // No log wait: the image writes one line to stdout and its trace to a
    // file, so no message on that stream marks readiness. Readiness is the
    // ping loop below; testcontainers' own `http_wait` would cost a reqwest
    // dependency for one poll.
    let container = GenericImage::new("clickhouse/clickhouse-server", IMAGE_TAG)
        .with_exposed_port(9000.tcp())
        .with_exposed_port(8123.tcp())
        .with_wait_for(WaitFor::Nothing)
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .with_host_config_modifier(|host| host.memory = Some(CONTAINER_MEMORY_BYTES))
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("the pinned ClickHouse image starts");
    let native_port = container.get_host_port_ipv4(9000).await.unwrap();
    let http_port = container.get_host_port_ipv4(8123).await.unwrap();
    let server = Server {
        container,
        native_port,
        http_port,
    };
    await_ready(&server).await;
    server
}

impl Server {
    fn tcp(&self) -> UnifiedClient {
        UnifiedClient::Tcp(TcpClient::new(format!("127.0.0.1:{}", self.native_port)))
    }

    fn http(&self) -> UnifiedClient {
        UnifiedClient::Http(
            Client::default().with_url(format!("http://127.0.0.1:{}", self.http_port)),
        )
    }

    /// Stop the container. `Drop` also stops it, but an explicit call keeps
    /// the teardown on the test's own timeline rather than a background task.
    async fn stop(self) {
        let _ = self.container.rm().await;
    }
}

/// Poll until the server answers on both ports, so a test never races the
/// startup it did not wait for.
async fn await_ready(server: &Server) {
    let http = UnifiedClient::Http(
        Client::default().with_url(format!("http://127.0.0.1:{}", server.http_port)),
    );
    let tcp = UnifiedClient::Tcp(TcpClient::new(format!("127.0.0.1:{}", server.native_port)));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut last = String::from("never attempted");
    while tokio::time::Instant::now() < deadline {
        match (http.ping().await, tcp.ping().await) {
            (Ok(()), Ok(())) => return,
            (Err(e), _) | (_, Err(e)) => last = e.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("the server never answered on both ports; last error: {last}");
}

/// One matrix row: a column type and the SQL that produces its values. The
/// server builds the values, so nothing here depends on this crate's encoder.
struct Case {
    ch_type: &'static str,
    /// SQL expressions, one per row, each already of `ch_type`.
    values: &'static [&'static str],
    /// Set when the codec is known not to read this shape yet, naming the
    /// reason. The case is then asserted to FAIL, so fixing the codec fails
    /// this test until the marker is removed -- a marker cannot rot into a
    /// silently skipped type.
    known_broken: Option<&'static str>,
}

/// Shorthand for a case that must round-trip.
const fn case(ch_type: &'static str, values: &'static [&'static str]) -> Case {
    Case {
        ch_type,
        values,
        known_broken: None,
    }
}

/// `NativeWriter.cpp:93-94` serialises EVERY nested prefix for a column
/// before any of its data, while this codec reads and writes each prefix
/// inline where the child sits. The two orders coincide until an enclosing
/// type writes data of its own first: a `Tuple` writes nothing before its
/// fields and round-trips, an `Array` writes its offsets and pushes the
/// child's prefix out of place. Fixing it means a prefix phase and a data
/// phase in both directions, mirroring
/// `ISerialization::serializeBinaryBulkStatePrefix`.
///
/// `LowCardinality` is done -- its prefix is a fixed version word the reader
/// hoists. `Dynamic` and `Variant` carry variable-length prefixes the data
/// phase reads values out of, so they still go inline.
const PREFIX_PHASE: &str =
    "Dynamic/Variant prefixes are not split from data (NativeWriter.cpp:93-94)";

const CASES: &[Case] = &[
    case("UInt8", &["0", "255"]),
    case("UInt16", &["0", "65535"]),
    case("UInt32", &["0", "4294967295"]),
    case("UInt64", &["0", "18446744073709551615"]),
    case("UInt128", &["0", "340282366920938463463374607431768211455"]),
    case("UInt256", &["0", "12345678901234567890"]),
    case("Int8", &["-128", "127"]),
    case("Int16", &["-32768", "32767"]),
    case("Int32", &["-2147483648", "2147483647"]),
    case("Int64", &["-9223372036854775808", "9223372036854775807"]),
    case("Int128", &["-170141183460469231731687303715884105728", "0"]),
    case("Int256", &["-1", "1"]),
    case("Float32", &["-1.5", "3.25"]),
    case("Float64", &["-1.5", "3.25"]),
    case("BFloat16", &["1.5", "-2"]),
    case("Bool", &["true", "false"]),
    case("Decimal(9, 2)", &["'-1234567.89'", "'0.01'"]),
    case("Decimal(18, 4)", &["'-1.0001'", "'12345678901.2345'"]),
    case("Decimal(38, 10)", &["'-1.0000000001'", "'0'"]),
    case("Decimal(76, 20)", &["'-1.00000000000000000001'", "'0'"]),
    case("Date", &["'1970-01-01'", "'2149-06-06'"]),
    case("Date32", &["'1900-01-01'", "'2299-12-31'"]),
    case(
        "DateTime",
        &["'1970-01-01 00:00:00'", "'2106-02-07 06:28:15'"],
    ),
    case(
        "DateTime('Australia/Sydney')",
        &["'2026-09-02 08:00:00'", "'1970-01-01 10:00:00'"],
    ),
    case(
        "DateTime64(3, 'UTC')",
        &["'2026-09-02 08:00:00.123'", "'1970-01-01 00:00:00.000'"],
    ),
    case("DateTime64(9)", &["'2026-09-02 08:00:00.123456789'"]),
    case(
        "UUID",
        &["'00000000-0000-0000-0000-000000000000'", "generateUUIDv4()"],
    ),
    case("IPv4", &["'127.0.0.1'", "'255.255.255.255'"]),
    case("IPv6", &["'::1'", "'2001:db8::1'"]),
    case("Enum8('a' = 1, 'b' = -2)", &["'a'", "'b'"]),
    case("Enum16('a' = 1, 'b' = -300)", &["'a'", "'b'"]),
    // Unicode and an embedded NUL: ClickHouse Strings are bytes, not UTF-8.
    case("String", &["''", "'ünïcode'", "concat('a', char(0), 'b')"]),
    case("FixedString(4)", &["'ab'", "'abcd'"]),
    case("Nullable(UInt64)", &["NULL", "7"]),
    case("Nullable(String)", &["NULL", "'x'"]),
    case(
        "Nullable(DateTime64(3))",
        &["NULL", "'2026-09-02 08:00:00.500'"],
    ),
    case("LowCardinality(String)", &["'a'", "'b'", "'a'"]),
    case("LowCardinality(Nullable(String))", &["NULL", "'a'", "'a'"]),
    case("Array(Int32)", &["[]", "[-1, 0, 1]"]),
    case("Array(Nullable(UInt8))", &["[NULL, 1]", "[]"]),
    case("Array(Array(String))", &["[]", "[['a'], [], ['b', 'c']]"]),
    case("Array(LowCardinality(String))", &["[]", "['a', 'b', 'a']"]),
    case("Map(String, Int64)", &["map()", "map('a', 1, 'b', -2)"]),
    case("Map(String, Array(String))", &["map('a', ['x', 'y'])"]),
    case(
        "Map(String, LowCardinality(Nullable(String)))",
        &["map('a', NULL, 'b', 'v')"],
    ),
    case("Tuple(UInt8, String, Array(Int32))", &["(1, 'a', [1, 2])"]),
    case(
        "Array(Tuple(String, Map(String, UInt8)))",
        &["[('a', map('k', 1))]", "[]"],
    ),
    case("Point", &["(1.5, -2.5)"]),
    case("SimpleAggregateFunction(sum, UInt64)", &["1", "2"]),
    case("JSON", &[r#"'{"a":1,"b":{"c":"x"}}'"#, "'{}'"]),
    case("JSON", &[r#"'{"arr":[1,2,3]}'"#]),
    case(
        "Variant(UInt64, String)",
        &["'text'::String", "42::UInt64", "NULL"],
    ),
    case("Dynamic", &["'text'", "42::UInt64", "NULL"]),
    // Prefix-bearing types nested one level down. A Tuple writes no data of
    // its own before its fields, so its child's prefix lands first either
    // way and these pass; an Array's offsets come first, which is what
    // pushes the child's prefix out of place.
    case("Tuple(LowCardinality(String), UInt8)", &["('a', 1)"]),
    case("Array(JSON)", &[r#"['{"a":1}']"#, "[]"]),
    Case {
        ch_type: "Array(Dynamic)",
        values: &["['x', 42::UInt64]", "[]"],
        known_broken: Some(PREFIX_PHASE),
    },
    case("Array(Variant(UInt64, String))", &["['x'::String]", "[]"]),
];

/// The decoded shape, rendered. `DecodedColumn` is not `PartialEq`, and the
/// rendering compares the variant and its values together, which is the whole
/// claim. Every matrix case is a handful of rows, so the server sends one
/// block on either transport; a split would make the renderings differ for a
/// reason that is not a defect, so it is asserted rather than assumed.
fn rendered(columns: &Columns, name: &str) -> Result<String, String> {
    let blocks: Vec<&DecodedBlock> = columns.blocks().iter().filter(|b| b.num_rows > 0).collect();
    match blocks.as_slice() {
        [block] => match block.column(name) {
            Some(column) => Ok(format!("{column:?}")),
            None => Err(format!("block declares no column '{name}'")),
        },
        other => Err(format!("expected one payload block, got {}", other.len())),
    }
}

/// Every type in the matrix must decode to the same thing over TCP and over
/// HTTP. Both go through this crate's Native codec, so a divergence is a
/// transport-specific bug in the framing around it.
#[tokio::test]
#[ignore = "needs Docker -- see the module docs"]
async fn every_type_decodes_identically_on_both_transports() {
    let server = server().await;
    let (tcp, http) = (server.tcp(), server.http());
    tcp.execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();

    let mut failures = Vec::new();
    for (i, case) in CASES.iter().enumerate() {
        let outcome = run_case(&tcp, &http, case, i).await;
        match (outcome, case.known_broken) {
            // Behaved as declared: round-tripped, or failed and marked.
            (Ok(()), None) | (Err(_), Some(_)) => {}
            (Err(e), None) => failures.push(format!("{}: {e}", case.ch_type)),
            (Ok(()), Some(reason)) => failures.push(format!(
                "{} now round-trips -- drop its known_broken marker ({reason})",
                case.ch_type
            )),
        }
    }

    // Stop before asserting: a panic would skip the teardown.
    server.stop().await;
    assert!(
        failures.is_empty(),
        "{} cases failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Create, fill and read one case on both transports.
async fn run_case(
    tcp: &UnifiedClient,
    http: &UnifiedClient,
    case: &Case,
    index: usize,
) -> Result<(), String> {
    let table = format!("wire.t{index}");
    let create = format!(
        "CREATE OR REPLACE TABLE {table} (c {}) ENGINE = MergeTree ORDER BY tuple()",
        case.ch_type
    );
    tcp.execute(&create)
        .await
        .map_err(|e| format!("CREATE rejected: {e}"))?;

    let rows: Vec<String> = case.values.iter().map(|v| format!("({v})")).collect();
    let insert = format!("INSERT INTO {table} VALUES {}", rows.join(", "));
    tcp.execute(&insert)
        .await
        .map_err(|e| format!("INSERT rejected: {e}"))?;

    let sql = format!("SELECT c FROM {table}");
    let over_tcp = tcp
        .fetch_columns(&sql)
        .await
        .map_err(|e| format!("tcp SELECT failed: {e}"))?;
    let over_http = http
        .fetch_columns(&sql)
        .await
        .map_err(|e| format!("http SELECT failed: {e}"))?;

    let (t, h) = (rendered(&over_tcp, "c")?, rendered(&over_http, "c")?);
    if t == h {
        Ok(())
    } else {
        Err(format!("tcp {t} != http {h}"))
    }
}

/// The encoder faces the same prefix ordering as the decoder did. Rather than
/// assert which way round it is, write the same row twice -- once by the
/// server from SQL, once through this crate's encoder -- and require the two
/// tables to read back identically.
#[tokio::test]
#[ignore = "needs Docker -- see the module docs"]
async fn a_nested_low_cardinality_insert_matches_the_server_s_own() {
    use std::sync::Arc;

    use serde_json::{Map, json};

    use clickhouse_dfe::dynamic::{ColumnDef, DynamicInsert, DynamicSchema};

    const TYPE: &str = "Array(LowCardinality(String))";
    let server = server().await;
    let client = server.tcp();
    client
        .execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();

    for table in ["wire.lc_by_server", "wire.lc_by_encoder"] {
        client
            .execute(&format!(
                "CREATE OR REPLACE TABLE {table} (c {TYPE}) \
                 ENGINE = MergeTree ORDER BY tuple()"
            ))
            .await
            .unwrap();
    }
    client
        .execute("INSERT INTO wire.lc_by_server VALUES (['a', 'b', 'a'])")
        .await
        .unwrap();

    let schema = Arc::new(DynamicSchema::from_columns(
        "wire.lc_by_encoder",
        vec![ColumnDef::new("c", TYPE)],
    ));
    let mut insert = DynamicInsert::tcp(
        client.as_tcp().expect("the tcp arm").clone(),
        "wire",
        "lc_by_encoder",
        schema,
    );
    let mut row = Map::new();
    row.insert("c".to_string(), json!(["a", "b", "a"]));
    insert.write_map(&row).await.expect("the row encodes");
    insert.end().await.expect("the insert commits");

    let by_server = client
        .fetch_columns("SELECT c FROM wire.lc_by_server")
        .await
        .expect("read back what the server wrote");
    let by_encoder = client
        .fetch_columns("SELECT c FROM wire.lc_by_encoder")
        .await
        .expect("read back what this crate wrote");

    let (by_encoder, by_server) = (
        rendered(&by_encoder, "c").unwrap(),
        rendered(&by_server, "c").unwrap(),
    );
    server.stop().await;
    assert_eq!(
        by_encoder, by_server,
        "this crate's encoder must lay out {TYPE} the way the server does"
    );
}

/// A `MergeTree` column that is almost all defaults, merged into one part, is
/// stored with sparse serialisation. Real tables look like this.
///
/// This passes, but it does NOT yet prove the sparse form reached the wire:
/// the server may have materialised the column before sending, and this
/// decoder rejects a non-zero custom-serialization flag outright, so it would
/// have failed loudly if it had. Proving the wire form needs the flag
/// observed, not just the values -- see S5.T4 step 3.
#[tokio::test]
#[ignore = "needs Docker -- see the module docs"]
async fn a_sparse_column_reads_back_on_both_transports() {
    const TOTAL: &str = "SELECT sum(c) AS total FROM wire.sparse";
    // The column itself, where the sparse serialisation reaches the wire
    // rather than being collapsed by an aggregate.
    const RAW: &str = "SELECT c FROM wire.sparse ORDER BY n LIMIT 6000";

    let server = server().await;
    let client = server.tcp();
    client
        .execute("CREATE DATABASE IF NOT EXISTS wire")
        .await
        .unwrap();
    // The default ratio is 0.95; 1 non-default row in 10,000 is well under it.
    client
        .execute(
            "CREATE OR REPLACE TABLE wire.sparse (n UInt64, c UInt64) \
             ENGINE = MergeTree ORDER BY n \
             SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9",
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO wire.sparse \
             SELECT number, if(number = 5000, 42, 0) FROM system.numbers LIMIT 10000",
        )
        .await
        .unwrap();
    client
        .execute("OPTIMIZE TABLE wire.sparse FINAL")
        .await
        .unwrap();

    let over_tcp = client.fetch_columns(TOTAL).await.expect("tcp reads it");
    let over_http = server
        .http()
        .fetch_columns(TOTAL)
        .await
        .expect("http reads it");
    let values = client
        .fetch_columns(RAW)
        .await
        .expect("tcp reads the column")
        .get::<u64>("c")
        .unwrap();

    server.stop().await;
    assert_eq!(over_tcp.get::<u64>("total").unwrap(), [42]);
    assert_eq!(over_http.get::<u64>("total").unwrap(), [42]);
    assert_eq!(values.len(), 6000);
    assert_eq!(values[5000], 42, "the one non-default row");
    assert_eq!(values[0], 0);
}

/// A result set the server splits into several blocks must read back whole
/// and in order, on both transports.
#[tokio::test]
#[ignore = "needs Docker -- see the module docs"]
async fn a_multi_block_result_reads_back_whole_and_in_order() {
    const SQL: &str = "SELECT number AS n FROM system.numbers LIMIT 200000";

    let server = server().await;
    let over_tcp = server.tcp().fetch_columns(SQL).await.unwrap();
    let over_http = server.http().fetch_columns(SQL).await.unwrap();
    let ns = over_tcp.get::<u64>("n").unwrap();
    let http_ns = over_http.get::<u64>("n").unwrap();
    server.stop().await;

    assert_eq!(ns.len(), 200_000, "every row must arrive");
    assert_eq!(ns[0], 0);
    assert_eq!(ns[199_999], 199_999);
    assert_eq!(http_ns, ns, "transports disagree");
}

/// An exception raised part-way through a result set must surface as an
/// error, not as a short read that looks like success.
#[tokio::test]
#[ignore = "needs Docker -- see the module docs"]
async fn a_mid_stream_exception_surfaces_rather_than_truncating() {
    const SQL: &str = "SELECT throwIf(number = 50000) FROM system.numbers LIMIT 100000";

    let server = server().await;
    let err = server
        .tcp()
        .fetch_columns(SQL)
        .await
        .expect_err("the server throws part-way through");
    server.stop().await;
    assert!(
        format!("{err}").contains("Value passed to 'throwIf'"),
        "the server's own message must reach the caller: {err}"
    );
}

/// The connection is reusable after a failed query: the actor drains and
/// returns to idle rather than staying poisoned.
#[tokio::test]
#[ignore = "needs Docker -- see the module docs"]
async fn a_failed_query_leaves_the_connection_usable() {
    let server = server().await;
    let client = server.tcp();
    let _ = client.fetch_columns("SELECT * FROM no_such_table").await;

    let ok = client.fetch_columns("SELECT 1 AS n").await.unwrap();
    let n = ok.get::<u8>("n").unwrap();
    server.stop().await;
    assert_eq!(n, [1]);
}
