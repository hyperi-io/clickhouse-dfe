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
`output_format_native_write_json_as_string=1` so it comes back as text.

Turning that setting off also works, and has since the advertised revision
reached 54473: the server then sends the V2 path-based serialisation, which the
decoder reads. The setting stays the default because the text form costs the
server nothing to produce, not because it is the only readable one. Both
directions are covered in `tests/wire_docker.rs` by
`json_reads_with_or_without_the_string_flag`.

Two limits worth knowing before sending data:

- A root-level JSON array is not a document ClickHouse will accept, so the
  dynamic encoder wraps one as `{"_values": [...]}`. An array nested inside an
  object is untouched.
- An integer too wide for any ClickHouse integer type is stored as a `Float64`
  and silently rounded -- about 17 significant digits survive, with no error.
  Send such fields as strings.

## Sparse columns

Wired and proved live. A column whose defaults pass
`ratio_of_defaults_for_sparse_serialization` arrives as an offset list plus
only the non-default values, and the decoder scatters it back to the block's
full length. Scalars only, which is all the server sparse-serialises; a
composite arriving that way is refused by name.

The header that selects it is TWO bytes, not one, and the second is easy to
miss: `NativeWriter` writes the `has_custom_serialization` bool, then, only
when it is set, a byte naming the kind stack (0 Default, 1 Sparse, 2 Detached,
3 Detached-over-Sparse, 4 Replicated, 5 a combination with its own varuint
count). Reading the flag alone and treating 1 as "sparse" leaves the kind byte
on the wire; one byte of drift then corrupts every following column and
surfaces as an unknown packet id nowhere near the cause. Only Default and
Sparse are decoded; the rest are refused by name.

## Not implemented

`AggregateFunction`, `Nested` (decomposes as `Array(Tuple)`), `Ring`,
`Polygon`, `MultiPolygon`.
