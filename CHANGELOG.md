# Changelog

Released sections are rendered by CI at the end of a release and committed
back. `[Unreleased]` is maintained by hand until then. Release notes also
appear on the GitHub Releases page, one per tag.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- TCP transport (connection actor, deadpool pool, retry, TLS trust) and the Native-format wire codec
- Column-typed reads over TCP: `TcpClient::query(sql).fetch_blocks()` plus `DecodedBlock::column_as` over the `FromColumn` trait
- Dynamic layer: runtime-schema inserts from `serde_json::Map` rows over HTTP (`FORMAT RowBinary`) and TCP (`FORMAT Native`)
- JSON columns travel as `String` in both directions, with `TcpClient::with_json_as_string` to opt out; opting out reads the V2 path-based serialisation instead, so both settings work
- The advertised protocol revision is 54473, so `JSON` and `Dynamic` arrive in their V2 serialisation rather than V1, and the server sends sparse columns at all
- Sparse columns read: a column whose defaults pass `ratio_of_defaults_for_sparse_serialization` arrives as offsets plus non-default values and is scattered back to the block's full length (scalars only, which is all the server sparse-serialises)
- `UnifiedClient::fetch_columns` decodes with this crate's Native codec on both transports, so `JSON`, `Variant` and `Dynamic` read over HTTP as well as TCP
- Unified client over both transports: `UnifiedClient`, `Columns::get::<T>` by column name, and `dynamic_insert` resolving its schema over whichever transport is configured
- `ClientExt` on `clickhouse::Client`: ping, kill query, query id, session id, role, plus `ServerException` parsed from a server error body with a retriable-code test
- Server-side query parameters over TCP: `TcpQuery::param` emits the Query packet's parameters section, so `{name:Type}` binds instead of being interpolated
- `column_as` reaches every scalar the decoder produces, and `Option<T>` works wherever `T` does
- A root-level JSON array written to a `JSON` column is wrapped as `{"_values": [...]}` (`dynamic::JSON_ARRAY_WRAPPER_KEY`), because ClickHouse's JSON parser accepts only an object at the root and rejects an array with code 117. An array nested inside an object is untouched

### Fixed

- The per-column custom-serialization header is two bytes, not one: a `has_custom_serialization` bool followed by a kind-stack byte. Reading only the first left the kind byte on the wire, and one byte of drift corrupted every following column, surfacing as an unknown packet id nowhere near the cause
- The Query packet carries the extra-roles string that every server from revision 54472 reads from all clients, not just interserver ones. Without it the server waits for a length-prefixed string that never arrives and the connection blocks
- `Manager::create` took the endpoint count modulo before checking it, so a pool built directly with no endpoints panicked inside an acquire
- Wire-declared lengths are read in bounded chunks rather than allocated up front, so a corrupt length costs one chunk instead of its full claim
- Four Native serialisation formats corrected against ClickHouse 26.3: the JSON v3 per-path version, JSON v2 shared data, the LowCardinality index-width threshold, and the Variant discriminator mode
- An uncapped column count could abort the process, a fixed-offset truncation of a server exception could panic mid-character before authentication, and a nested-exception chain could exhaust the stack
- Recycled connections leaked their socket and reader task; a streaming SELECT released its pool slot before the stream drained
- Retry backoff is jittered, so connections that fail together no longer retry in lockstep
- The pool's recycle decision is one function, so the tests exercise the production rule rather than a copy of it
- A nested `LowCardinality` now reads and writes its serialisation prefix where the server puts it -- ahead of the enclosing column's data, not inline. `Array(LowCardinality(String))` and `Map(String, LowCardinality(Nullable(String)))` could not be read, and inserting either returned server code 117
- `Enum8` and `Enum16` decode as `DecodedColumn::Int8` / `Int16`, matching the signed ordinals ClickHouse defines. A top-level enum column previously decoded unsigned, so `Enum8('b' = -2)` read back as `254`, while the same value inside a `Variant`, `Dynamic` or `JSON` cell rendered correctly as `-2`. Read such a column with `column_as::<i8>` / `::<i16>`

### Removed

- The unused Native block-compression and block-info modules. `lz4` and `zstd` forward to upstream for the HTTP path; the TCP handshake has always negotiated no compression
