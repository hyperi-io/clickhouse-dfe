# dfe-clickhouse docs

Start at the [README](../README.md): it is the crate's front page, the feature
table and the quick start, and it is what renders on docs.rs.

Design rationale lives in code comments at the point of use, so a file
cherry-picked into another repository carries its reasoning with it. That is
deliberate -- this index stays thin rather than duplicating what the source
already says and would drift from.

Per-layer notes land here as each grows past what a comment can carry:

- [architecture.md](architecture.md) -- the cross-layer map: the three gaps the
  crate exists to close, the feature layers, and the invariants that span more
  than one module.
- [coverage.md](coverage.md) -- the 80% floor, the 90% hot-path target, how to
  measure, and where any tracked debt is recorded.
- [insert-formats.md](insert-formats.md) -- why every insert path sends binary,
  and why there is no JSONEachRow option.
- [type-support.md](type-support.md) -- which ClickHouse types the codec reads
  and writes.
- [unified-client.md](unified-client.md) -- one entry point over two transports.
