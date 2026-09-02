# Coverage: the floor, the hot-path target, and the gap

The repo floor is 80% lines. Two modules are marked `HOT PATH` in their own
docs and held to 90% instead: `src/native/` (every row in or out of the server
goes through it) and `src/tcp/pool.rs` (every operation acquires from it).

`src/native/` mostly meets it; `src/tcp/pool.rs` sits just under. The remainder
is tracked debt, not an oversight, and this file is where it is tracked -- the
marker comments point here rather than asserting a number that would go stale
the next time someone adds a type.

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
| `native/sparse.rs` | 91.5% | met |
| `native/columns.rs` | 90.9% | met |
| `tcp/pool.rs` | 88.8% | -1.2 |
| `native/decode.rs` | 86.8% | -3.2 |
| `native/encode.rs` | 85.7% | -4.3 |
| Crate total | 90.5% | above the 90% goal |

`native/columns.rs` was the laggard at 81.4% and is now the one to copy: the
lift came almost entirely from one table-driven test over
`rowbinary_to_json`, the function that renders every `Variant`, `Dynamic` and
`JSON` cell as text. Roughly 260 uncovered lines, one coherent unit, and a
wrong arm in it is silently wrong data rather than a failed query.

`native/encode.rs` and `native/decode.rs` are the remaining gap. Both are
mostly per-type arms, so the same shape of test should work on them.

## What the numbers do not tell you

Coverage counts lines executed, not wire formats proved. The sparse
kind-stack bug sat in code the unit tests executed happily -- the fixtures
agreed with the decoder because both were written from the same wrong reading
of the spec. What caught it was `tests/wire_docker.rs`, where a real server
writes the bytes.

So a hot-path change wants both: the unit test for the branch, and a matrix row
in the Docker suite if it touches the wire. A number in the table above going
up is not on its own evidence that a format is right.
