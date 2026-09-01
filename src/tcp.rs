// Project:   clickhouse-dfe
// File:      src/tcp.rs
// Purpose:   Native TCP transport: connection actor, pool, retry, TcpClient
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `TcpClient` over the ClickHouse native protocol -- connection actor,
//! deadpool pool and retry. Stage S2 fills this in.
