# clickhouse-dfe docs

This is the docs index, and it is deliberately thin while the crate is being
built out. The working plan that drives the stages lives outside this repo at
`clickhouse-rs/.hyperi-ai/plans/2026-09-01-clickhouse-dfe.md`, and the design
rationale for each layer goes in code comments at the point of use so that a
file cherry-picked upstream carries its reasoning with it. A proper docs pass
lands once the TCP and dynamic layers are in place.
