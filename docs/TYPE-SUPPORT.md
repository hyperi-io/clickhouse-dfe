# Column type support

What the Native codec reads and writes. Every type listed as supported is
round-tripped against a real ClickHouse in `tests/wire_docker.rs`, on both
transports, with the server building the values.

## Scalars

`UInt8` through `UInt256`, `Int8` through `Int256`, `Float32`, `Float64`,
`BFloat16`, `Decimal32/64/128/256`, `Date`, `Date32`, `DateTime`,
`DateTime64`, `Time`, `Time64`, `UUID`, `IPv4`, `IPv6`, `Enum8`, `Enum16`,
`Point`, `Bool`.

Enum ordinals are signed: `Enum8` decodes as `Int8` and `Enum16` as `Int16`,
so a negative ordinal reads back as itself rather than as its unsigned byte.

## Strings

`String` (varuint length then bytes -- ClickHouse strings are bytes, not
guaranteed UTF-8) and `FixedString(N)` (N raw bytes on the wire).

## Composites

`Nullable(T)`, `LowCardinality(T)`, `Array(T)`, `Tuple(T1..Tn)`, `Map(K, V)`,
`SimpleAggregateFunction(f, T)`, nested arbitrarily.

Nested serialisation prefixes are written and read ahead of the enclosing
column's data, which is where `NativeWriter` puts them. Reading them inline
works until an enclosing type writes data of its own first, so
`Array(LowCardinality(String))` and friends need the phased form.

## Semi-structured

`Variant(T1..Tn)`, `Dynamic` and `JSON` all decode to their per-row document
text. `Object('json')` is the deprecated type and reads as a plain String.

A `JSON` column travels as `String` in both directions: an insert declares it
`String` and the server converts, and every query asks for
`output_format_native_write_json_as_string=1` so it comes back as text. Turn
that off and the path-based serialisation arrives, which this decoder rejects
with a message naming the version rather than misreading it.

Two limits worth knowing before sending data:

- A root-level JSON array is not a document ClickHouse will accept, so the
  dynamic encoder wraps one as `{"_values": [...]}`. An array nested inside an
  object is untouched.
- An integer too wide for any ClickHouse integer type is stored as a `Float64`
  and silently rounded -- about 17 significant digits survive, with no error.
  Send such fields as strings.

## Not implemented

`AggregateFunction`, `Nested` (decomposes as `Array(Tuple)`), `Ring`,
`Polygon`, `MultiPolygon`.

Sparse serialisation has a reader in `native/sparse.rs` that is not yet wired:
the advertised protocol revision sits below the sparse gate, so the server
never sends it. It is wired as part of raising that revision.
