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

## Layers

Every layer is a feature. Take what you need and pay for nothing else.

| Feature | What it adds |
|---|---|
| `tcp` | Native TCP transport -- connection actor, deadpool pool, retry, Native-format wire codec |
| `tls` | rustls trust for the TCP transport (implies `tcp`) |
| `lz4` | LZ4 block compression, on both transports |
| `zstd` | Zstd block compression, on both transports |
| `dynamic` | Runtime-schema RowBinary insert from `serde_json::Map` rows |
| `unified` | One client over HTTP `clickhouse::Client` and our `TcpClient` (implies `tcp`) |
| `ext` | Extension traits on `clickhouse::Client` -- ping, kill query, query id, session id, roles, typed server exceptions |
| `inserter` | Background-actor inserter layer |
| `full` | All of the above |

Default is `tcp` plus `lz4`, which is what a ClickHouse server negotiates out
of the box.

```toml
[dependencies]
clickhouse = "0.15"
clickhouse-dfe = "0.1"
```

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
