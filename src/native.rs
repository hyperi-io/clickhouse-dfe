// Project:   clickhouse-dfe
// File:      src/native.rs
// Purpose:   Native-format wire codec: blocks, columns, compression framing
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Native-format block codec, shared by `tcp` and `dynamic` and so ungated.
//! Stage S2 fills this in.
