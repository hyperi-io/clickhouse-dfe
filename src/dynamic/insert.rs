// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse_ext/insert.rs
// Purpose:   DynamicInsert adapter over the clickhouse-rs RowBinary/Native sinks
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Single-table dynamic insert with schema-mismatch recovery.
//!
//! [`DynamicInsert`] encodes each `Map<String, Value>` against a
//! [`DynamicSchema`] and ships the rows through one of two sinks, both fed the
//! same [`DynamicRow::encode`] bytes:
//!
//! - HTTP -- `Client::insert_formatted_with(... FORMAT RowBinary)`, written
//!   row-wise. The schema is read from `system.columns` on the first write and
//!   cached.
//! - TCP (`tcp` feature) -- `TcpClient::insert_native(... FORMAT Native)`, with
//!   the rows transposed into columnar blocks by
//!   [`crate::native::encode_columns`]. The caller supplies the schema, because
//!   reading `system.columns` over TCP needs a typed cursor the crate does not
//!   have yet.
//!
//! On a schema-mismatch error the HTTP path invalidates the cached schema so
//! the next insert re-fetches.

use std::sync::Arc;

use serde_json::{Map, Value};

use clickhouse::Client;

use super::encode::{ColumnDef, DynamicRow};
use super::error::DynamicError;
use super::schema::{DynamicSchema, DynamicSchemaCache, fetch_dynamic_schema};

#[cfg(feature = "tcp")]
use crate::native::{ColumnSchema, encode_columns};
#[cfg(feature = "tcp")]
use crate::tcp::{TcpClient, TcpInsertSession};

/// The server setting that makes a RowBinary reader take a length-prefixed
/// string as a JSON value, which is how the dynamic encoder writes JSON
/// columns on the wire.
const JSON_AS_STRING_SETTING: &str = "input_format_binary_read_json_as_string";

/// Backtick-quote a SQL identifier using upstream's own escaper, so an
/// identifier is quoted the same way here and in `clickhouse::Client`.
fn escape_ident(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    let _ = clickhouse::_priv::sql_escape_identifier(name, &mut out);
    out
}

/// The client rows are written through, and where the schema comes from.
enum Backend {
    Http {
        client: Client,
        cache: Arc<DynamicSchemaCache>,
    },
    #[cfg(feature = "tcp")]
    Tcp(TcpClient),
}

/// The sink opened on the first write, once the column subset is fixed.
enum OpenSink {
    Http(clickhouse::insert_formatted::BufInsertFormatted),
    #[cfg(feature = "tcp")]
    Tcp {
        session: TcpInsertSession,
        /// Native block headers for the insert columns, parsed once.
        native: Vec<ColumnSchema>,
        /// RowBinary rows held for the next Native block.
        block: Vec<Vec<u8>>,
    },
}

/// The open INSERT: the column subset fixed by the first row, and the sink
/// carrying it.
struct Active {
    columns: Vec<ColumnDef>,
    sink: OpenSink,
}

/// Dynamic insert for a single table.
///
/// Encodes runtime-shaped `Map<String, Value>` rows to ClickHouse using a
/// schema fetched from `system.columns`. As simple to drive as JSONEachRow,
/// but binary on the wire so the server skips JSON parsing.
#[must_use = "a DynamicInsert must be finished with `.end().await` to commit the rows"]
pub struct DynamicInsert {
    backend: Backend,
    database: String,
    table: String,
    schema: Option<DynamicSchema>,
    active: Option<Active>,
    rows_written: u64,
}

impl DynamicInsert {
    /// Insert over HTTP. The schema is fetched from `system.columns` on the
    /// first write and cached in `schema_cache`.
    pub fn http(
        client: Client,
        database: &str,
        table: &str,
        schema_cache: Arc<DynamicSchemaCache>,
    ) -> Self {
        Self {
            backend: Backend::Http {
                client,
                cache: schema_cache,
            },
            database: database.to_string(),
            table: table.to_string(),
            schema: None,
            active: None,
            rows_written: 0,
        }
    }

    /// Insert over the native protocol against a caller-supplied schema.
    /// There is nothing to fetch or cache here: `system.columns` is only
    /// readable over HTTP until the typed TCP cursor lands.
    #[cfg(feature = "tcp")]
    pub fn tcp(client: TcpClient, database: &str, table: &str, schema: DynamicSchema) -> Self {
        Self {
            backend: Backend::Tcp(client),
            database: database.to_string(),
            table: table.to_string(),
            schema: Some(schema),
            active: None,
            rows_written: 0,
        }
    }

    fn full_table(&self) -> String {
        format!("{}.{}", self.database, self.table)
    }

    /// Load the schema (from cache, else `system.columns`) if not already held.
    async fn ensure_schema(&mut self) -> Result<(), DynamicError> {
        if self.schema.is_some() {
            return Ok(());
        }
        let full = self.full_table();
        let schema = match &self.backend {
            Backend::Http { client, cache } => match cache.get(&full) {
                Some(cached) => cached,
                None => {
                    let fetched = fetch_dynamic_schema(client, &self.database, &self.table).await?;
                    cache.insert(&full, fetched.clone());
                    fetched
                }
            },
            #[cfg(feature = "tcp")]
            Backend::Tcp(_) => return Ok(()),
        };
        self.schema = Some(schema);
        Ok(())
    }

    /// On the first row, fix the column subset and open the sink.
    ///
    /// `raw_names` are column names supplied via raw passthrough (e.g. `_json`)
    /// that must be in the INSERT even if absent from the row map.
    async fn ensure_active(
        &mut self,
        row: &Map<String, Value>,
        raw_names: &[&str],
    ) -> Result<(), DynamicError> {
        if self.active.is_some() {
            return Ok(());
        }
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| DynamicError::EncodingError {
                column: String::new(),
                message: "schema not available after fetch".to_string(),
            })?;

        let columns = select_columns(row, raw_names, schema);
        if columns.is_empty() {
            return Err(DynamicError::EncodingError {
                column: String::new(),
                message: "no columns to insert for this row".to_string(),
            });
        }
        let json_columns = schema.has_json_columns();

        let target = format!(
            "{}.{}",
            escape_ident(&self.database),
            escape_ident(&self.table)
        );
        let cols_sql = columns
            .iter()
            .map(|c| escape_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");

        let sink = match &self.backend {
            Backend::Http { client, .. } => {
                let sql = format!("INSERT INTO {target} ({cols_sql}) FORMAT RowBinary");
                let mut client = client.clone();
                if json_columns {
                    client = client.with_setting(JSON_AS_STRING_SETTING, "1");
                }
                OpenSink::Http(client.insert_formatted_with(sql).buffered())
            }
            #[cfg(feature = "tcp")]
            Backend::Tcp(client) => {
                let full = self.full_table();
                let sql = format!("INSERT INTO {target} ({cols_sql}) FORMAT Native");
                let headers: Vec<(String, String)> = columns
                    .iter()
                    .map(|c| (c.name.clone(), c.type_string.clone()))
                    .collect();
                let native =
                    ColumnSchema::from_headers(&headers).map_err(|e| classify_error(&full, &e))?;
                // Empty query id: the server allocates one.
                let session = client
                    .insert_native("", &sql)
                    .await
                    .map_err(|e| classify_error(&full, &e))?;
                OpenSink::Tcp {
                    session,
                    native,
                    block: Vec::new(),
                }
            }
        };

        self.active = Some(Active { columns, sink });
        Ok(())
    }

    /// Encode and buffer a row for insert. Fetches the schema and opens the
    /// sink on the first call.
    ///
    /// # Errors
    ///
    /// Returns [`DynamicError`] on schema fetch failure, an unsupported column
    /// type, an encoding failure, or a transport error.
    pub async fn write_map(&mut self, row: &Map<String, Value>) -> Result<(), DynamicError> {
        self.write(row, &[], None).await
    }

    /// Like [`write_map`][Self::write_map], but the named columns are written
    /// from pre-encoded raw bytes (e.g. the original payload for `_json`),
    /// avoiding a re-serialise. Only the first raw column is threaded through
    /// the encoder's zero-copy passthrough; any others fall back to the row map.
    ///
    /// # Errors
    ///
    /// As [`write_map`][Self::write_map].
    pub async fn write_map_with_raw(
        &mut self,
        row: &Map<String, Value>,
        raw_columns: &[(&str, &[u8])],
    ) -> Result<(), DynamicError> {
        let raw_names: Vec<&str> = raw_columns.iter().map(|(n, _)| *n).collect();
        self.write(row, &raw_names, raw_columns.first().copied())
            .await
    }

    async fn write(
        &mut self,
        row: &Map<String, Value>,
        raw_names: &[&str],
        raw: Option<(&str, &[u8])>,
    ) -> Result<(), DynamicError> {
        self.ensure_schema().await?;
        self.ensure_active(row, raw_names).await?;

        #[cfg(feature = "tcp")]
        let full = self.full_table();
        let Active { columns, sink } =
            self.active
                .as_mut()
                .ok_or_else(|| DynamicError::EncodingError {
                    column: String::new(),
                    message: "insert sink not initialised".to_string(),
                })?;

        let bytes = match raw {
            Some((json_col, raw_bytes)) => DynamicRow::with_raw(row, columns, raw_bytes, json_col),
            None => DynamicRow::new(row, columns),
        }
        .encode()?;

        match sink {
            OpenSink::Http(insert) => insert.write_buffered(&bytes),
            #[cfg(feature = "tcp")]
            OpenSink::Tcp {
                session,
                native,
                block,
            } => {
                block.push(bytes);
                if block.len() as u64 >= TcpClient::DEFAULT_INSERT_BLOCK_ROWS {
                    send_block(session, native, block, &full).await?;
                }
            }
        }
        self.rows_written += 1;
        Ok(())
    }

    /// Flush and finalise the INSERT, returning the number of rows written.
    ///
    /// On a schema-mismatch error the cached schema is invalidated so the next
    /// insert re-fetches from `system.columns`.
    ///
    /// # Errors
    ///
    /// Returns [`DynamicError`] if the server rejects the insert.
    pub async fn end(mut self) -> Result<u64, DynamicError> {
        let full = self.full_table();
        let rows_written = self.rows_written;
        let outcome = match self.active.take() {
            None => Ok(()),
            Some(active) => match active.sink {
                OpenSink::Http(mut insert) => {
                    insert.end().await.map_err(|e| classify_error(&full, &e))
                }
                #[cfg(feature = "tcp")]
                OpenSink::Tcp {
                    session,
                    native,
                    mut block,
                } => match send_block(&session, &native, &mut block, &full).await {
                    Ok(()) => session
                        .finish()
                        .await
                        .map_err(|e| classify_error(&full, &e)),
                    Err(e) => {
                        session.abort();
                        Err(e)
                    }
                },
            },
        };
        if let Err(err) = outcome {
            if matches!(err, DynamicError::SchemaMismatch { .. }) {
                self.invalidate_schema();
            }
            return Err(err);
        }
        Ok(rows_written)
    }

    /// Invalidate the cached schema, forcing a re-fetch on the next insert.
    /// A no-op on the TCP path, where the caller owns the schema.
    pub fn invalidate_schema(&mut self) {
        let full = self.full_table();
        match &self.backend {
            Backend::Http { cache, .. } => {
                cache.invalidate(&full);
                self.schema = None;
            }
            #[cfg(feature = "tcp")]
            Backend::Tcp(_) => {}
        }
    }

    /// Number of rows written so far.
    #[must_use]
    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    /// The resolved schema, if it has been loaded.
    #[must_use]
    pub fn schema(&self) -> Option<&DynamicSchema> {
        self.schema.as_ref()
    }
}

/// Transpose the buffered RowBinary rows into one Native block and send it.
#[cfg(feature = "tcp")]
async fn send_block(
    session: &TcpInsertSession,
    native: &[ColumnSchema],
    block: &mut Vec<Vec<u8>>,
    full_table: &str,
) -> Result<(), DynamicError> {
    if block.is_empty() {
        return Ok(());
    }
    let bytes = encode_columns(block, native, session.server_revision)
        .map_err(|e| classify_error(full_table, &e))?;
    let rows = block.len() as u64;
    block.clear();
    session
        .send_block(bytes, native.len() as u64, rows)
        .await
        .map_err(|e| classify_error(full_table, &e))
}

/// Choose the columns to include in the INSERT: every column present in the
/// row, every raw-passthrough column, and every required column (no
/// server-side default). Columns that have a default and are not supplied are
/// omitted so the server fills them (e.g. `_uuid`, `_timestamp_load`).
fn select_columns(
    row: &Map<String, Value>,
    raw_names: &[&str],
    schema: &DynamicSchema,
) -> Vec<ColumnDef> {
    schema
        .columns
        .iter()
        .filter(|c| {
            row.contains_key(&c.name) || raw_names.contains(&c.name.as_str()) || !c.has_default
        })
        .cloned()
        .collect()
}

/// Classify a transport error as a schema mismatch (cached schema is stale) or
/// a generic encoding/transport error. Schema mismatch covers explicit
/// column/type errors and the data-format errors that indicate schema drift.
fn classify_error(full_table: &str, e: &impl std::fmt::Display) -> DynamicError {
    let msg = e.to_string();
    let mismatch = msg.contains("UNKNOWN_IDENTIFIER")
        || msg.contains("NO_SUCH_COLUMN")
        || msg.contains("THERE_IS_NO_COLUMN")
        || msg.contains("TYPE_MISMATCH")
        || msg.contains("ILLEGAL_COLUMN")
        || msg.contains("CANNOT_PARSE")
        || msg.contains("cannot parse")
        || msg.contains("INCORRECT_DATA")
        || msg.contains("incorrect data")
        || msg.contains("Code: 117");
    if mismatch {
        DynamicError::SchemaMismatch {
            table: full_table.to_string(),
            message: msg,
        }
    } else {
        DynamicError::EncodingError {
            column: String::new(),
            message: msg,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema() -> DynamicSchema {
        DynamicSchema::from_columns(
            "db.t",
            vec![
                ColumnDef::with_default_kind("id", "UInt64", ""),
                ColumnDef::with_default_kind("name", "String", ""),
                ColumnDef::with_default_kind("_uuid", "UUID", "DEFAULT"),
                ColumnDef::with_default_kind("_json", "JSON", ""),
            ],
        )
    }

    #[test]
    fn select_columns_includes_present_and_required_omits_absent_default() {
        let s = schema();
        let row = json!({ "id": 1, "name": "a" });
        let obj = row.as_object().unwrap().clone();
        let cols = select_columns(&obj, &[], &s);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        // id, name present; _json required (no default); _uuid omitted (default, absent)
        assert_eq!(names, vec!["id", "name", "_json"]);
    }

    #[test]
    fn select_columns_includes_raw_passthrough_column() {
        let s = schema();
        let row = json!({ "id": 1 });
        let obj = row.as_object().unwrap().clone();
        let cols = select_columns(&obj, &["_json"], &s);
        assert!(cols.iter().any(|c| c.name == "_json"));
    }

    #[test]
    fn classify_recognises_schema_drift() {
        let e = clickhouse::error::Error::Custom("Code: 117. DB::Exception: incorrect data".into());
        assert!(matches!(
            classify_error("db.t", &e),
            DynamicError::SchemaMismatch { .. }
        ));

        let e = clickhouse::error::Error::Custom("network reset".into());
        assert!(matches!(
            classify_error("db.t", &e),
            DynamicError::EncodingError { .. }
        ));
    }

    #[test]
    fn escape_ident_quotes_and_escapes() {
        assert_eq!(escape_ident("plain"), "`plain`");
        assert_eq!(escape_ident("we`ird"), "`we\\`ird`");
    }

    /// Every type the encoder writes has to parse as a Native block header,
    /// or the TCP sink cannot declare the column.
    #[cfg(feature = "tcp")]
    #[test]
    fn native_headers_cover_every_encoder_type() {
        let ok = [
            ("a", "UInt64"),
            ("b", "Nullable(String)"),
            ("c", "LowCardinality(Nullable(String))"),
            ("d", "Array(Int64)"),
            ("e", "Map(String, UInt32)"),
            ("f", "DateTime64(3, 'UTC')"),
            ("g", "Decimal(18, 2)"),
            ("h", "Enum8('a' = 1)"),
            ("i", "FixedString(16)"),
            ("j", "IPv6"),
            ("k", "JSON"),
        ];
        for (name, ty) in ok {
            let headers = vec![(name.to_string(), ty.to_string())];
            assert!(
                ColumnSchema::from_headers(&headers).is_ok(),
                "{ty} must parse as a Native column header"
            );
        }

        // The encoder already writes JSON as a length-prefixed string, so the
        // block declares the column as String and the server casts it back.
        let json = vec![("data".to_string(), "JSON".to_string())];
        let native = ColumnSchema::from_headers(&json).unwrap();
        let block = encode_columns(&[vec![2u8, b'{', b'}']], &native, 0).unwrap();
        assert_eq!(block.as_slice(), b"\x04data\x06String\x02{}");
    }
}
