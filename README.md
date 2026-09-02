# clickhouse-dfe

HyperI extensions for the official ClickHouse Rust client. It depends on
`clickhouse` from crates.io and adds the pieces that client does not ship: a
native TCP transport, runtime-schema inserts, and one client that dispatches
over either transport.

Pre-release. The API is unstable and will change without a deprecation cycle
until 1.0.

## Status

The TCP transport is in -- connection actor, deadpool pool, retry, TLS trust and
the Native-format wire codec.

Reads over TCP are column-typed: `client.query(sql).fetch_blocks()`, then values
by column name off each block. Row-typed `fetch::<T>()` and `insert::<T>()` are
not in -- they need two small upstream re-exports of the RowBinary row serialiser
and deserialiser.

`JSON` columns travel over TCP as `String` in both directions: an insert declares
them as `String` and the server casts, and every query asks for
`output_format_native_write_json_as_string=1` so they come back as JSON text.
`TcpClient::with_json_as_string(false)` turns the read side off, at which point a
JSON column arrives in the path-based serialisation and does not decode.

## Layers

Every layer is a feature. Take what you need and pay for nothing else.

| Feature | What it adds |
|---|---|
| `tcp` | Native TCP transport -- connection actor, deadpool pool, retry, Native-format wire codec |
| `tls` | rustls trust for the TCP transport (implies `tcp`) |
| `lz4` | LZ4 compression for the HTTP transport, forwarded to upstream. The TCP handshake negotiates no compression |
| `zstd` | Zstd compression for the HTTP transport, forwarded to upstream. The TCP handshake negotiates no compression |
| `dynamic` | Runtime-schema insert from `serde_json::Map` rows -- `FORMAT RowBinary` over HTTP, `FORMAT Native` over TCP (implies `ext`) |
| `unified` | One client over HTTP `clickhouse::Client` and our `TcpClient` (implies `tcp`) |
| `ext` | Extension traits on `clickhouse::Client` -- ping, kill query, query id, session id, role, typed server exceptions |
| `full` | All of the above |

Default is `tcp` plus `lz4`. `lz4` affects the HTTP path only -- the TCP
handshake sends no compression, so a default build that uses only the native
transport pays nothing for it.

```toml
[dependencies]
clickhouse = "0.15"
clickhouse-dfe = "0.1"
```

## Tests

```bash
cargo test --all-features
```

That runs the unit tests and the wire suite, which starts a pinned ClickHouse
in Docker and round-trips every supported column type over both transports.
Without a container runtime the wire tests skip with a message, except under
`$CI`, where they fail rather than disappear. `cargo nextest run` serialises
them to one server at a time; plain `cargo test` does the same through a
semaphore.

The suites ending in `_live` are `#[ignore]`d because they need a real cluster.
Point them at one and opt in:

```bash
env CLICKHOUSE_DFE_ENV_FILE=/path/to/.env \
  cargo test --all-features -- --ignored
```

The env file supplies `CLICKHOUSE_HOST`, `CLICKHOUSE_NATIVE_PORT`,
`CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_TLS`, `CLICKHOUSE_USER`,
`CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE` and `CLICKHOUSE_CLUSTER`.

## Relationship to upstream

This is not a fork. It is a separate crate that depends on the published
`clickhouse` release, and that dependency is the only link -- no `git`, no
`path`, no `[patch]`.

Files here stay close to upstream conventions (same edition, same MSRV, same
rustfmt settings, same lint set) so that
[ClickHouse/clickhouse-rs](https://github.com/ClickHouse/clickhouse-rs) can
cherry-pick from this crate without a reformat or a relicensing step.

## Updating from upstream

Bump the `clickhouse` version in `Cargo.toml`, run `cargo update -p clickhouse`,
run the tests, and release. That is the whole procedure -- there is no fork to
rebase and no patch to reapply, because the dependency is a published crates.io
version. Renovate raises the bump PR on its own once a new release is a week
old; a bump that turns red is telling you which layer has drifted from
upstream's public API, and that layer is where the fix goes.

## Licence

Apache-2.0, which is one of the two options upstream offers, so code moves in
either direction unchanged. Files derived from upstream keep upstream's own
`MIT OR Apache-2.0` notice. See `NOTICE` for the attribution.
