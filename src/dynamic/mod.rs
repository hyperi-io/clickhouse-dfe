// Project:   clickhouse-dfe
// File:      src/dynamic/mod.rs
// Purpose:   Runtime-schema insert -- type parser, schema reflection, RowBinary encoder
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Insert rows whose shape is only known at run time.
//!
//! A [`DynamicSchema`] read from `system.columns` drives a per-column RowBinary
//! encoder over `serde_json::Map<String, Value>` rows, so a caller with no
//! compile-time row struct still puts binary on the wire instead of JSONEachRow.
//!
//! [`DynamicInsert`] ships those rows over HTTP (`FORMAT RowBinary` through
//! `clickhouse::Client`) or, with the `tcp` feature, over the native protocol
//! (`FORMAT Native` through `TcpClient`). Both sinks are fed the same
//! [`DynamicRow::encode`] bytes.
//!
//! The five modules below were ported from the DFE Loader and keep the
//! BUSL-1.1 headers they were written under.

pub mod encode;
pub mod error;
pub mod insert;
pub mod parsed_type;
pub mod schema;

pub use encode::{ColumnDef, DynamicRow};
pub use error::DynamicError;
pub use insert::DynamicInsert;
pub use parsed_type::{ParsedType, ParsedTypeExt, TypeTag};
pub use schema::{DynamicSchema, DynamicSchemaCache, fetch_dynamic_schema};
