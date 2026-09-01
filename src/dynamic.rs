// Project:   clickhouse-dfe
// File:      src/dynamic.rs
// Purpose:   Runtime-schema RowBinary insert from serde_json rows
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Runtime-schema insert: type parsing, schema discovery and RowBinary
//! encoding for `serde_json::Map` rows. Stage S3 fills this in.
