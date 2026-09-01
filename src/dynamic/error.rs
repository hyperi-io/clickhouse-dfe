// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   clickhouse-dfe
// File:      src/dynamic/error.rs
// Purpose:   DynamicError for schema-driven inserts
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Error types for dynamic (schema-driven) inserts.

/// Errors specific to dynamic schema-driven inserts.
///
/// `#[non_exhaustive]`: a caller matching on this must carry a `_` arm. The
/// variants stay constructible so a consumer can build one in its own tests.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DynamicError {
    /// Column type string could not be parsed.
    #[error("unsupported type '{type_str}' for column '{column}'")]
    UnsupportedType {
        /// Column the type was declared on.
        column: String,
        /// The type string as `system.columns` reported it.
        type_str: String,
    },
    /// Value could not be encoded for the target column type.
    #[error("encoding error for column '{column}': {message}")]
    EncodingError {
        /// Column the value belonged to; empty for a whole-row failure.
        column: String,
        /// What the encoder could not do.
        message: String,
    },
    /// Schema mismatch detected -- server rejected the insert. Retriable once
    /// the cached schema is invalidated.
    #[error("schema mismatch for table '{table}': {message}")]
    SchemaMismatch {
        /// Fully qualified `database.table`.
        table: String,
        /// The server's rejection text.
        message: String,
    },
    /// Schema fetch from `system.columns` failed. Retriable: a cold cache
    /// after a restart hits this on a transport blip.
    #[error("failed to fetch schema for '{table}': {source}")]
    SchemaFetch {
        /// Fully qualified `database.table`.
        table: String,
        /// The query failure, kept typed so a caller can classify it.
        #[source]
        source: clickhouse::error::Error,
    },
    /// Table has no columns (or does not exist). Retriable against a cluster
    /// that is still replicating the DDL.
    #[error("table '{table}' has no columns or does not exist")]
    EmptySchema {
        /// Fully qualified `database.table`.
        table: String,
    },
}
