# Coverage: the floor, the hot-path target, and the gap

The repo floor is 80% lines. Two modules are marked `HOT PATH` in their own
docs and held to 90% instead: `src/native/` (every row in or out of the server
goes through it) and `src/tcp/pool.rs` (every operation acquires from it).

Every module in `src/native/` meets it. `src/tcp/pool.rs` sits just under, and
that is tracked debt rather than an oversight -- this file is where it is
tracked, and the marker comments point here rather than asserting a number that
would go stale the next time someone adds a type.

## Measuring

CI does not gate on this. `coverage: false` in `.hyperi-ci.yaml` is deliberate:
hyperi-ci reads `test.min_coverage` for Python only, so turning it on would
gate nothing and cost the runner. Measure out of band:

```text
cargo llvm-cov --all-features --all-targets --summary-only
```

Use `--all-targets`. A `--lib`-only run reads several points lower on the
codec, because the integration suites cover paths the unit tests do not, and
comparing one scope against the other invents regressions that are not there.

## Where it stands, 2026-09-02

Line coverage, whole suite, at protocol revision 54473.

| Module | Lines | Against its target |
|---|---|---|
| `native/io.rs` | 92.6% | met |
| `native/decode.rs` | 92.3% | met |
| `native/encode.rs` | 91.7% | met |
| `native/columns.rs` | 91.7% | met |
| `native/sparse.rs` | 91.5% | met |
| `tcp/pool.rs` | 88.8% | -1.2 |
| Crate total | 91.8% | above the 90% goal |

All of `src/native/` came up in one pass, and the shape that did it is worth
copying. Each of the three laggards had its gap concentrated in one family of
per-type arms, so one table-driven test per family moved each of them 5 to 9
points:

- `columns.rs` 81.4 -> 91.7, covering `rowbinary_to_json`, which renders every
  `Variant`, `Dynamic` and `JSON` cell as text.
- `encode.rs` 85.7 -> 91.7, covering the `LowCardinality(Nullable(T))` INSERT
  path and the truncation checks in `rb_advance`.
- `decode.rs` 86.8 -> 92.3, covering the `expand_sparse` scatter arms and
  `empty_column`'s per-type shapes.

Write the expectations from the wire format, not from what the function
currently returns. The `LowCardinality(Nullable)` test asserts the exact
dictionary and index bytes derived from the spec, which is the difference
between a test that pins behaviour and one that pins a bug.

`tcp/pool.rs` is the last one short. Its gap is the recycle and health-check
paths, which want a connection that fails on demand rather than another table.

## What the numbers do not tell you

Coverage counts lines executed, not wire formats proved. The sparse
kind-stack bug sat in code the unit tests executed happily -- the fixtures
agreed with the decoder because both were written from the same wrong reading
of the spec. What caught it was `tests/wire_docker.rs`, where a real server
writes the bytes.

So a hot-path change wants both: the unit test for the branch, and a matrix row
in the Docker suite if it touches the wire. A number in the table above going
up is not on its own evidence that a format is right.
