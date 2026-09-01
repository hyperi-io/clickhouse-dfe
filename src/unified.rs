// Project:   clickhouse-dfe
// File:      src/unified.rs
// Purpose:   One client dispatching over HTTP and TCP transports
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `UnifiedClient` over `Transport::{Http(clickhouse::Client), Tcp(TcpClient)}`
//! for runtime transport selection. Stage S4 fills this in.
