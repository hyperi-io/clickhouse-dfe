# clickhouse-dfe

<!-- BADGES:START -->
[![Build Status](https://github.com/hyperi-io/clickhouse-dfe/actions/workflows/ci.yml/badge.svg)](https://github.com/hyperi-io/clickhouse-dfe/actions)
[![Crates.io](https://img.shields.io/crates/v/clickhouse-dfe?logo=rust)](https://crates.io/crates/clickhouse-dfe)
[![docs.rs](https://img.shields.io/docsrs/clickhouse-dfe?logo=rust)](https://docs.rs/clickhouse-dfe)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/hyperi-io/clickhouse-dfe/blob/main/LICENSE)
<!-- BADGES:END -->

> The official ClickHouse Rust client speaks HTTP and nothing else. Plenty of
> deployments expose port 9000 and no HTTP at all. `JSON`, `Variant` and
> `Dynamic` columns do not read. Inserting needs a struct known at compile
> time, which a loader taking arbitrary shapes off a topic does not have.

Extensions for the official client, as a separate crate depending on the
published `clickhouse` release. Adds a native TCP transport, runtime-schema
inserts, and one client that dispatches over either.

```toml
[dependencies]
clickhouse = "0.15"
clickhouse-dfe = "0.1"
```

## Extension traits on the HTTP client

`ping`, `kill_query`, `with_query_id`, `with_session_id`, `with_role`, and
`ServerException::parse` for a typed error carrying the server's own code.

```rust,no_run
use clickhouse::Client;
use clickhouse_dfe::ClientExt;

# async fn example() -> clickhouse_dfe::Result<()> {
let client = Client::default()
    .with_url("http://localhost:8123")
    .with_query_id("nightly-rollup");

client.ping().await?;
# Ok(())
# }
```

## Native TCP

```toml
clickhouse-dfe = { version = "0.1", features = ["tcp"] }
```

Reads are column-typed, and `JSON`, `Variant` and `Dynamic` decode to their
per-row document text.

```rust,ignore
use clickhouse_dfe::TcpClient;

let client = TcpClient::new("localhost:9000").with_database("default");
let blocks = client
    .query("SELECT number FROM system.numbers LIMIT 10")
    .fetch_blocks()
    .await?;

for block in &blocks {
    for n in block.column_as::<u64>("number")? {
        println!("{n}");
    }
}
```

## Runtime-schema inserts

```toml
clickhouse-dfe = { version = "0.1", features = ["dynamic"] }
```

Rows arrive as `serde_json::Map`, column types come from `system.columns`.
Binary on the wire either way -- `FORMAT RowBinary` over HTTP, `FORMAT Native`
over TCP -- so the server does no JSON parsing.

## Features

| Feature | What it adds |
|---|---|
| `ext` | Extension traits on `clickhouse::Client` (default) |
| `lz4` | LZ4 for the HTTP transport, forwarded to upstream (default) |
| `zstd` | Zstd for the HTTP transport, forwarded to upstream |
| `tcp` | Native TCP transport: connection actor, pool, retry, wire codec |
| `tls` | rustls trust for the TCP transport (implies `tcp`) |
| `dynamic` | Runtime-schema inserts (implies `ext`) |
| `unified` | One client over both transports (implies `tcp`) |
| `full` | All of the above |

Default is `ext` plus `lz4`, neither of which adds a dependency beyond
upstream's. `tcp` brings deadpool, socket2 and backon.

## Documentation

| Read | When |
|---|---|
| [type-support](https://github.com/hyperi-io/clickhouse-dfe/blob/main/docs/TYPE-SUPPORT.md) | Checking whether a column type reads and writes |
| [insert-formats](https://github.com/hyperi-io/clickhouse-dfe/blob/main/docs/INSERT-FORMATS.md) | Asking why there is no JSONEachRow option |
| [unified-client](https://github.com/hyperi-io/clickhouse-dfe/blob/main/docs/UNIFIED-CLIENT.md) | Choosing a transport at runtime |
| [coverage](https://github.com/hyperi-io/clickhouse-dfe/blob/main/docs/COVERAGE.md) | Adding to a hot-path module |

## Upstream

Not a fork. The dependency on the published `clickhouse` release is the only
link -- no `git`, no `path`, no `[patch]`. Same edition, MSRV, rustfmt settings
and lint set, so
[ClickHouse/clickhouse-rs](https://github.com/ClickHouse/clickhouse-rs) can
cherry-pick from here without a reformat or a relicence.

## Licence

Apache-2.0. See
[LICENSE](https://github.com/hyperi-io/clickhouse-dfe/blob/main/LICENSE) and
[NOTICE](https://github.com/hyperi-io/clickhouse-dfe/blob/main/NOTICE).
