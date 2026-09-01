// Project:   clickhouse-dfe
// File:      src/inserter.rs
// Purpose:   Background-actor inserter layer over upstream's inserter
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Background-actor inserter: multi-table batching, dedup tokens, batch
//! isolation and recovery. Stage S5 fills this in.
