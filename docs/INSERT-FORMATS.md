# Insert formats: binary by default, JSONEachRow never

Why every insert path in this crate puts binary on the wire. The code is in
`src/dynamic/insert.rs` and `src/native/encode.rs`; this is the argument.

## JSONEachRow is the easy option and the expensive one

It works, it is trivial to produce, and it hands the server a pile of work the
client had already done. For every row the server parses JSON text, infers
types and validates structure -- to arrive at a layout the client knew before
it sent anything.

ClickHouse's own format benchmark puts JSONEachRow at roughly 17% server CPU
overhead against about 5.5% for Native: three times the parse cost, and it
scales with ingest volume rather than flattening out. On a pipeline moving
10k-20k row batches continuously, that is the difference between one server and
two.

## What this crate sends

| Path | Format | Transport |
|---|---|---|
| `UnifiedClient::dynamic_insert` -> HTTP | RowBinary | HTTP |
| `UnifiedClient::dynamic_insert` -> TCP | Native, columnar | native TCP |
| `TcpClient::insert_native` | Native, columnar | native TCP |
| `clickhouse::Client::insert::<T>` | RowBinary | HTTP (upstream, unchanged) |

There is no JSONEachRow path. `DynamicInsert` takes the same
`Map<String, Value>` rows a JSON encoder would, and encodes them to RowBinary
or transposes them into Native columnar blocks -- the caller's code looks the
same either way, and the server stops parsing text.

Over native TCP the choice does not exist at all: the protocol accepts Native
format and nothing else, which is a server constraint rather than a gap here.

## What the crate does that the server cannot

Encoding client-side is also the only place some conversions can happen. The
coercions in `src/dynamic/encode.rs` -- epoch milliseconds against a
`DateTime64` of a declared precision, a string into an `Enum8` label, an
integer width checked rather than silently truncated -- have no JSONEachRow
equivalent, because by the time the server sees the text the type information
that would resolve them is gone.

## Sources

- [ClickHouse input format matchup](https://clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient)
- [Altinity ingestion performance](https://kb.altinity.com/altinity-kb-schema-design/ingestion-performance-and-formats/)
