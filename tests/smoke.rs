// Project:   clickhouse-dfe
// File:      tests/smoke.rs
// Purpose:   Prove the crate links and the upstream client is reachable
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Smoke tests. Nothing here opens a connection, so the suite is hermetic and
//! needs no ClickHouse server.

// The prose here names a product, not a code item.
#![allow(clippy::doc_markdown)]

/// The crates.io dependency is the only link to upstream, so a build that
/// cannot reach `clickhouse::Client` has lost the thing this crate extends.
#[test]
fn upstream_client_is_reachable() {
    let client = clickhouse::Client::default().with_url("http://localhost:8123");
    drop(client);
}
