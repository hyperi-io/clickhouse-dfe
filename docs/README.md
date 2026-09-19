# clickhouse-dfe docs

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
- [COVERAGE.md](COVERAGE.md) -- the 80% floor, the 90% hot-path target, how to
  measure, and where any tracked debt is recorded.
- [INSERT-FORMATS.md](INSERT-FORMATS.md) -- why every insert path sends binary,
  and why there is no JSONEachRow option.
- [TYPE-SUPPORT.md](TYPE-SUPPORT.md) -- which ClickHouse types the codec reads
  and writes.
- [UNIFIED-CLIENT.md](UNIFIED-CLIENT.md) -- one entry point over two transports.
