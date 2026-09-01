// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse_ext/encode.rs
// Purpose:   Dynamic Map<String, Value> to RowBinary encoder
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Dynamic RowBinary encoder.
//!
//! Turns a runtime-shaped `serde_json::Map<String, Value>` plus its resolved
//! ClickHouse column schema into RowBinary wire bytes. [`DynamicRow::encode`]
//! returns exactly what crosses the wire, so every column's encoding is
//! testable at the byte level with no server.
//!
//! # Coercions
//!
//! This encoder folds in the DFE coercions that ClickHouse's JSONEachRow path
//! cannot do server-side: epoch magnitude detection (ms/us/ns) and ISO-8601
//! 'T'-separator handling for date/time types, hyphen-less UUID hex, and
//! integer-to-dotted IPv4. All timestamps are treated as UTC; non-zero
//! timezone offsets on datetime strings are rejected rather than silently
//! dropped.

use std::borrow::Cow;
use std::net::{Ipv4Addr, Ipv6Addr};

use serde_json::{Map, Value};

use super::error::DynamicError;
use super::parsed_type::{ParsedType, TypeTag};

/// A single resolved column for a dynamic insert.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Column name, used to look up the value in the row map.
    pub name: String,
    /// Parsed ClickHouse type, drives wire encoding.
    pub ty: ParsedType,
    /// Canonical ClickHouse type string, declared in the Native block header
    /// so the server decodes the column as it was encoded.
    pub type_string: String,
    /// Default kind from system.columns: "", "DEFAULT", "MATERIALIZED",
    /// "ALIAS", "EPHEMERAL". Empty for columns without a server-side default.
    pub default_kind: String,
    /// True when the column has a server-side default and may be omitted from
    /// the INSERT column list (e.g. `_uuid`, `_timestamp_load`).
    pub has_default: bool,
}

impl ColumnDef {
    /// Build a column from a name and a ClickHouse type string. The column is
    /// treated as required (no server-side default).
    ///
    /// The type string is parsed once here; `type_string` keeps the original
    /// canonical form for the insert header.
    #[must_use]
    pub fn new(name: impl Into<String>, type_string: impl Into<String>) -> Self {
        let type_string = type_string.into();
        let ty = ParsedType::parse(&type_string);
        Self {
            name: name.into(),
            ty,
            type_string,
            default_kind: String::new(),
            has_default: false,
        }
    }

    /// Build a column carrying its `system.columns` default kind. `has_default`
    /// is set when `default_kind` is non-empty, marking the column omittable
    /// from the INSERT list.
    #[must_use]
    pub fn with_default_kind(
        name: impl Into<String>,
        type_string: impl Into<String>,
        default_kind: impl Into<String>,
    ) -> Self {
        let default_kind = default_kind.into();
        let has_default = !default_kind.is_empty();
        let mut column = Self::new(name, type_string);
        column.default_kind = default_kind;
        column.has_default = has_default;
        column
    }
}

/// Runtime-dynamic row: a borrowed JSON object plus the resolved column
/// schema it is encoded against, with optional raw passthrough for one named
/// JSON column.
pub struct DynamicRow<'a> {
    row: &'a Map<String, Value>,
    columns: &'a [ColumnDef],
    /// When set, `(column_name, raw_bytes)`: the named JSON column is written
    /// from `raw_bytes` as a length-prefixed string rather than from `row`.
    raw: Option<(&'a str, &'a [u8])>,
}

impl<'a> DynamicRow<'a> {
    /// Encode a row, taking each column's value from `row` by name.
    #[must_use]
    pub fn new(row: &'a Map<String, Value>, columns: &'a [ColumnDef]) -> Self {
        Self {
            row,
            columns,
            raw: None,
        }
    }

    /// Same as [`DynamicRow::new`], but the named JSON column (e.g. `_json`)
    /// is written from `raw` bytes -- a zero-copy passthrough of the original
    /// payload as a JSON string -- instead of from `row`.
    #[must_use]
    pub fn with_raw(
        row: &'a Map<String, Value>,
        columns: &'a [ColumnDef],
        raw: &'a [u8],
        json_col: &'a str,
    ) -> Self {
        Self {
            row,
            columns,
            raw: Some((json_col, raw)),
        }
    }

    /// Encode this row to RowBinary wire bytes.
    ///
    /// # Errors
    /// Returns [`DynamicError`] if any column value cannot be encoded for its
    /// target type.
    pub fn encode(&self) -> Result<Vec<u8>, DynamicError> {
        let mut buf = Vec::with_capacity(256);
        for col in self.columns {
            self.encode_column(col, &mut buf)?;
        }
        Ok(buf)
    }

    /// Encode a single column's value to RowBinary, appending to `buf`.
    ///
    /// Honours the raw `_json` passthrough for the named JSON column.
    fn encode_column(&self, col: &ColumnDef, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
        if let Some((name, bytes)) = self.raw
            && name == col.name
        {
            // Raw passthrough for a JSON/String column: Nullable not-null
            // marker if needed, then the bytes as a length-prefixed string.
            if col.ty.nullable {
                buf.push(0);
            }
            write_string(bytes, buf);
            return Ok(());
        }
        let value = self.row.get(&col.name).unwrap_or(&Value::Null);
        encode_value(value, &col.ty, &col.name, buf)
    }
}

// ---------------------------------------------------------------------------
// Per-value encoding
// ---------------------------------------------------------------------------

fn encode_value(
    value: &Value,
    pt: &ParsedType,
    col: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    // Nullable wrapper: one level only (ClickHouse forbids Nullable(Nullable)).
    if pt.nullable {
        if value.is_null() {
            buf.push(1);
            return Ok(());
        }
        buf.push(0);
    } else if value.is_null() {
        write_default(pt, buf);
        return Ok(());
    }

    encode_typed(value, pt, col, buf)
}

#[allow(clippy::too_many_lines)]
fn encode_typed(
    value: &Value,
    pt: &ParsedType,
    col: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    // A bare `Decimal(P, S)` carries tag Unknown; resolve it to a concrete
    // width by precision so it hits the right Decimal arm below.
    let tag = if pt.base == "Decimal" {
        decimal_tag_for_precision(pt.precision.unwrap_or(38))
    } else {
        pt.tag
    };

    match tag {
        TypeTag::String => {
            let s = value_to_str(value);
            write_string(s.as_bytes(), buf);
        }
        TypeTag::FixedString => {
            let s = value_to_str(value);
            let n = pt.fixed_size.unwrap_or(1);
            let bytes = s.as_bytes();
            // CH FixedString is a raw N-byte run: pad short values with NULs,
            // truncate long ones. No length prefix.
            if bytes.len() <= n {
                buf.extend_from_slice(bytes);
                buf.resize(buf.len() + (n - bytes.len()), 0);
            } else {
                buf.extend_from_slice(&bytes[..n]);
            }
        }
        TypeTag::Bool => {
            buf.push(u8::from(as_bool(value, col)?));
        }
        TypeTag::UInt8 => {
            buf.push(as_u64(value, col)? as u8);
        }
        TypeTag::UInt16 => {
            buf.extend_from_slice(&(as_u64(value, col)? as u16).to_le_bytes());
        }
        TypeTag::UInt32 => {
            buf.extend_from_slice(&(as_u64(value, col)? as u32).to_le_bytes());
        }
        TypeTag::UInt64 => {
            buf.extend_from_slice(&as_u64(value, col)?.to_le_bytes());
        }
        TypeTag::UInt128 => {
            buf.extend_from_slice(&as_u128(value, col)?.to_le_bytes());
        }
        TypeTag::UInt256 => {
            buf.extend_from_slice(&as_u256_le(value, col)?);
        }
        TypeTag::Int8 => {
            buf.extend_from_slice(&(as_i64(value, col)? as i8).to_le_bytes());
        }
        TypeTag::Int16 => {
            buf.extend_from_slice(&(as_i64(value, col)? as i16).to_le_bytes());
        }
        TypeTag::Enum8 => {
            let v = enum_discriminant(value, &pt.raw, col)?;
            let v = i8::try_from(v).map_err(|_| enc_err(col, "Enum8 value out of range"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Enum16 => {
            let v = enum_discriminant(value, &pt.raw, col)?;
            let v = i16::try_from(v).map_err(|_| enc_err(col, "Enum16 value out of range"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Int32 => {
            buf.extend_from_slice(&(as_i64(value, col)? as i32).to_le_bytes());
        }
        TypeTag::Int64 => {
            buf.extend_from_slice(&as_i64(value, col)?.to_le_bytes());
        }
        TypeTag::Int128 => {
            buf.extend_from_slice(&as_i128(value, col)?.to_le_bytes());
        }
        TypeTag::Int256 => {
            buf.extend_from_slice(&as_i256_le(value, col)?);
        }
        TypeTag::Float32 => {
            buf.extend_from_slice(&(as_f64(value, col)? as f32).to_le_bytes());
        }
        TypeTag::Float64 => {
            buf.extend_from_slice(&as_f64(value, col)?.to_le_bytes());
        }
        TypeTag::Date => {
            // Date is UInt16 days since 1970-01-01.
            let days = to_epoch_days(value, col)?;
            let days = u16::try_from(days)
                .map_err(|_| enc_err(col, "Date out of range for UInt16 days"))?;
            buf.extend_from_slice(&days.to_le_bytes());
        }
        TypeTag::Date32 => {
            // Date32 is Int32 days since 1970-01-01 (can be negative).
            let days = to_epoch_days(value, col)?;
            buf.extend_from_slice(&days.to_le_bytes());
        }
        TypeTag::DateTime => {
            // DateTime is UInt32 seconds since epoch.
            let secs = to_epoch_seconds(value, col)?;
            let secs = u32::try_from(secs)
                .map_err(|_| enc_err(col, "DateTime out of range for UInt32 seconds"))?;
            buf.extend_from_slice(&secs.to_le_bytes());
        }
        TypeTag::DateTime64 => {
            let precision = pt.precision.unwrap_or(3);
            let ticks = datetime64_to_ticks(value, precision, col)?;
            buf.extend_from_slice(&ticks.to_le_bytes());
        }
        TypeTag::Decimal32 => {
            let scale = pt.scale.unwrap_or(0);
            let backing = decimal_backing_i128(value, scale, col)?;
            let v = i32::try_from(backing).map_err(|_| enc_err(col, "Decimal32 overflow"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Decimal64 => {
            let scale = pt.scale.unwrap_or(0);
            let backing = decimal_backing_i128(value, scale, col)?;
            let v = i64::try_from(backing).map_err(|_| enc_err(col, "Decimal64 overflow"))?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        TypeTag::Decimal128 => {
            let scale = pt.scale.unwrap_or(0);
            let backing = decimal_backing_i128(value, scale, col)?;
            buf.extend_from_slice(&backing.to_le_bytes());
        }
        TypeTag::Decimal256 => {
            // Decimal256 backing is a 256-bit signed integer; the loader's
            // f64-scaled magnitudes fit in i128, so sign-extend to 32 bytes.
            let scale = pt.scale.unwrap_or(0);
            let backing = decimal_backing_i128(value, scale, col)?;
            buf.extend_from_slice(&i128_to_i256_le(backing));
        }
        TypeTag::UUID => encode_uuid(value, col, buf)?,
        TypeTag::IPv4 => encode_ipv4(value, col, buf)?,
        TypeTag::IPv6 => encode_ipv6(value, col, buf)?,
        TypeTag::Array => {
            let elem = pt
                .array_element
                .as_ref()
                .ok_or_else(|| enc_err(col, "Array without element type"))?;
            encode_array(value, elem, col, buf)?;
        }
        TypeTag::Map => {
            let (kt, vt) = pt
                .map_types
                .as_ref()
                .ok_or_else(|| enc_err(col, "Map without key/value types"))?;
            encode_map(value, kt, vt, col, buf)?;
        }
        TypeTag::JSON => {
            // JSON wire form is a length-prefixed String of the JSON text. A
            // Value::String is written verbatim (its content is the JSON);
            // other values are re-serialised to compact JSON text.
            // Missing values and empty strings become {}: the column's JSON
            // parser rejects the text `null` and empty input (code 117), and
            // JSON cannot be Nullable.
            let json_bytes = match value {
                Value::Null => Cow::Borrowed("{}".as_bytes()),
                Value::String(s) if s.is_empty() => Cow::Borrowed("{}".as_bytes()),
                Value::String(s) => Cow::Borrowed(s.as_bytes()),
                _ => Cow::Owned(value.to_string().into_bytes()),
            };
            write_string(&json_bytes, buf);
        }
        // Geo Point and any type the parser left as Unknown are not part of
        // the supported dynamic-insert surface. Tuple/Variant/Dynamic/Nested
        // were not handled by the source encoder either; reject explicitly so
        // a schema using them fails loudly instead of writing wrong bytes.
        TypeTag::Point | TypeTag::Tuple | TypeTag::Unknown => {
            return Err(DynamicError::UnsupportedType {
                column: col.to_string(),
                type_str: pt.raw.clone(),
            });
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn write_string(bytes: &[u8], buf: &mut Vec<u8>) {
    write_varint(bytes.len() as u64, buf);
    buf.extend_from_slice(bytes);
}

fn write_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

fn write_default(pt: &ParsedType, buf: &mut Vec<u8>) {
    // A bare `Decimal(P, S)` has no fixed_byte_size by base name; resolve it.
    if pt.base == "Decimal" {
        let size = match decimal_tag_for_precision(pt.precision.unwrap_or(38)) {
            TypeTag::Decimal32 => 4,
            TypeTag::Decimal64 => 8,
            TypeTag::Decimal256 => 32,
            _ => 16,
        };
        buf.resize(buf.len() + size, 0);
        return;
    }
    // JSON's no-value is the empty object: the column parser rejects empty
    // input (code 117) and JSON cannot be Nullable.
    if pt.tag == TypeTag::JSON {
        write_string(b"{}", buf);
        return;
    }
    if let Some(size) = pt.fixed_byte_size() {
        buf.resize(buf.len() + size, 0);
    } else {
        // Variable-length default: empty string / array / map.
        write_varint(0, buf);
    }
}

fn enc_err(col: &str, msg: &str) -> DynamicError {
    DynamicError::EncodingError {
        column: col.to_string(),
        message: msg.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Scalar coercions
// ---------------------------------------------------------------------------

/// Borrow the string directly when possible; allocate only for non-string
/// values that need stringification.
fn value_to_str(value: &Value) -> Cow<'_, str> {
    match value {
        Value::String(s) => Cow::Borrowed(s.as_str()),
        Value::Number(n) => Cow::Owned(n.to_string()),
        Value::Bool(b) => Cow::Borrowed(if *b { "true" } else { "false" }),
        Value::Null => Cow::Borrowed(""),
        other => Cow::Owned(other.to_string()),
    }
}

fn as_u64(value: &Value, col: &str) -> Result<u64, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().map(|v| v as u64))
            .or_else(|| n.as_f64().map(|v| v as u64))
            .ok_or_else(|| enc_err(col, "not a valid unsigned integer")),
        Value::Bool(b) => Ok(u64::from(*b)),
        Value::String(s) => s
            .trim()
            .parse::<u64>()
            .map_err(|_| enc_err(col, "string not parseable as u64")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_i64(value: &Value, col: &str) -> Result<i64, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().map(|v| v as i64))
            .or_else(|| n.as_f64().map(|v| v as i64))
            .ok_or_else(|| enc_err(col, "not a valid integer")),
        Value::Bool(b) => Ok(i64::from(*b)),
        Value::String(s) => s
            .trim()
            .parse::<i64>()
            .map_err(|_| enc_err(col, "string not parseable as i64")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_u128(value: &Value, col: &str) -> Result<u128, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_u64()
            .map(u128::from)
            .or_else(|| n.as_i64().map(|v| v as u128))
            .ok_or_else(|| enc_err(col, "not a valid u128")),
        Value::Bool(b) => Ok(u128::from(*b)),
        Value::String(s) => s
            .trim()
            .parse::<u128>()
            .map_err(|_| enc_err(col, "string not parseable as u128")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_i128(value: &Value, col: &str) -> Result<i128, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .map(i128::from)
            .or_else(|| n.as_u64().map(i128::from))
            .ok_or_else(|| enc_err(col, "not a valid i128")),
        Value::Bool(b) => Ok(i128::from(*b)),
        Value::String(s) => s
            .trim()
            .parse::<i128>()
            .map_err(|_| enc_err(col, "string not parseable as i128")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_f64(value: &Value, col: &str) -> Result<f64, DynamicError> {
    match value {
        Value::Number(n) => n.as_f64().ok_or_else(|| enc_err(col, "not a valid float")),
        Value::String(s) => s
            .trim()
            .parse::<f64>()
            .map_err(|_| enc_err(col, "string not parseable as f64")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_bool(value: &Value, col: &str) -> Result<bool, DynamicError> {
    match value {
        Value::Bool(b) => Ok(*b),
        Value::Number(n) => Ok(n.as_i64().unwrap_or(0) != 0),
        Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "yes" | "1" => Ok(true),
            "false" | "no" | "0" | "" => Ok(false),
            _ => Err(enc_err(col, "string not parseable as bool")),
        },
        Value::Null => Ok(false),
        _ => Err(enc_err(col, "expected bool")),
    }
}

/// Sign-extend a 128-bit signed backing value to a 32-byte little-endian
/// 256-bit integer (for `Int256` / `Decimal256`).
fn i128_to_i256_le(v: i128) -> [u8; 32] {
    let mut out = [if v < 0 { 0xFFu8 } else { 0x00u8 }; 32];
    out[..16].copy_from_slice(&v.to_le_bytes());
    out
}

/// Zero-extend a 128-bit unsigned value to a 32-byte little-endian 256-bit
/// integer (for `UInt256`).
fn u128_to_u256_le(v: u128) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[..16].copy_from_slice(&v.to_le_bytes());
    out
}

fn as_i256_le(value: &Value, col: &str) -> Result<[u8; 32], DynamicError> {
    Ok(i128_to_i256_le(as_i128(value, col)?))
}

fn as_u256_le(value: &Value, col: &str) -> Result<[u8; 32], DynamicError> {
    Ok(u128_to_u256_le(as_u128(value, col)?))
}

// ---------------------------------------------------------------------------
// Decimal
// ---------------------------------------------------------------------------

/// Resolve a bare `Decimal(P, S)` to its concrete backing width by precision.
/// ClickHouse uses Decimal32 for P<=9, Decimal64 for P<=18, Decimal128 for
/// P<=38, and Decimal256 above that.
fn decimal_tag_for_precision(precision: u8) -> TypeTag {
    match precision {
        0..=9 => TypeTag::Decimal32,
        10..=18 => TypeTag::Decimal64,
        19..=38 => TypeTag::Decimal128,
        _ => TypeTag::Decimal256,
    }
}

/// Compute the backing integer for a `Decimal(P, S)` by scaling the value by
/// `10^scale`. Mirrors the loader's f64-based scaling; the concrete width is
/// applied by the caller.
fn decimal_backing_i128(value: &Value, scale: u8, col: &str) -> Result<i128, DynamicError> {
    let factor = 10f64.powi(i32::from(scale));
    let f = match value {
        Value::Number(n) => n.as_f64().ok_or_else(|| enc_err(col, "invalid decimal"))?,
        Value::String(s) => s
            .trim()
            .parse::<f64>()
            .map_err(|_| enc_err(col, "string not parseable as decimal"))?,
        Value::Bool(b) => f64::from(u8::from(*b)),
        _ => return Err(enc_err(col, "expected decimal number")),
    };
    Ok((f * factor).round() as i128)
}

// ---------------------------------------------------------------------------
// Date / time
// ---------------------------------------------------------------------------

/// Scale a numeric epoch value down to whole seconds, detecting ms/us/ns by
/// magnitude. Matches the loader's coercer thresholds.
fn epoch_to_seconds(ts: i64) -> i64 {
    if ts > 1_000_000_000_000_000_000 {
        ts / 1_000_000_000
    } else if ts > 1_000_000_000_000_000 {
        ts / 1_000_000
    } else if ts > 1_000_000_000_000 {
        ts / 1_000
    } else {
        ts
    }
}

/// Resolve a JSON value to whole epoch seconds (UTC). Accepts a date/datetime
/// string, a numeric-string epoch, or a number (with magnitude detection).
fn to_epoch_seconds(value: &Value, col: &str) -> Result<i64, DynamicError> {
    match value {
        Value::Number(n) => {
            let raw = n
                .as_i64()
                .or_else(|| n.as_f64().map(|f| f as i64))
                .ok_or_else(|| enc_err(col, "invalid epoch number"))?;
            Ok(epoch_to_seconds(raw))
        }
        Value::String(s) => {
            let s = s.trim();
            if let Ok(raw) = s.parse::<i64>() {
                return Ok(epoch_to_seconds(raw));
            }
            let (secs, _) = parse_datetime_str(s).map_err(|m| enc_err(col, &m))?;
            Ok(secs)
        }
        _ => Err(enc_err(col, "expected datetime string or epoch number")),
    }
}

/// Resolve a JSON value to days since 1970-01-01 (UTC). Accepts a date string,
/// a numeric-string epoch, or a number (with magnitude detection).
fn to_epoch_days(value: &Value, col: &str) -> Result<i32, DynamicError> {
    // A bare "YYYY-MM-DD" has no time component; parse it directly.
    if let Value::String(s) = value {
        let s = s.trim();
        if s.len() == 10 && s.as_bytes().get(4) == Some(&b'-') {
            let year: i32 = s[0..4]
                .parse()
                .map_err(|_| enc_err(col, "invalid Date year"))?;
            let month: u32 = s[5..7]
                .parse()
                .map_err(|_| enc_err(col, "invalid Date month"))?;
            let day: u32 = s[8..10]
                .parse()
                .map_err(|_| enc_err(col, "invalid Date day"))?;
            if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
                return Err(enc_err(col, "invalid Date components"));
            }
            let days = civil_days_from_epoch(year, month, day);
            return i32::try_from(days).map_err(|_| enc_err(col, "Date out of range"));
        }
    }
    let secs = to_epoch_seconds(value, col)?;
    let days = secs.div_euclid(86_400);
    i32::try_from(days).map_err(|_| enc_err(col, "Date out of range"))
}

/// Convert a JSON value to `DateTime64` epoch ticks at the given precision.
fn datetime64_to_ticks(value: &Value, precision: u8, col: &str) -> Result<i64, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .ok_or_else(|| enc_err(col, "invalid DateTime64 number")),
        Value::String(s) => {
            let s = s.trim();
            // A numeric string is treated as an already-scaled tick count.
            if let Ok(n) = s.parse::<i64>() {
                return Ok(n);
            }
            let (secs, frac_nanos) = parse_datetime_str(s).map_err(|m| enc_err(col, &m))?;
            let multiplier = 10i64.pow(u32::from(precision));
            let base = secs
                .checked_mul(multiplier)
                .ok_or_else(|| enc_err(col, "DateTime64 epoch overflow"))?;
            let frac_scaled =
                i64::from(frac_nanos) / 10i64.pow(9u32.saturating_sub(u32::from(precision)));
            base.checked_add(frac_scaled)
                .ok_or_else(|| enc_err(col, "DateTime64 epoch overflow"))
        }
        _ => Err(enc_err(col, "expected number or datetime string")),
    }
}

/// Parse `YYYY-MM-DD[ T]HH:MM:SS[.frac][Z|+00:00]` into `(unix_seconds,
/// fractional_nanoseconds)`. UTC only; non-zero offsets are rejected.
fn parse_datetime_str(s: &str) -> Result<(i64, u32), String> {
    let bytes = s.as_bytes();
    let len = bytes.len();

    if len < 19 {
        return Err("invalid DateTime string".into());
    }
    let sep = bytes[10];
    if sep != b' ' && sep != b'T' {
        return Err("invalid DateTime string".into());
    }

    let mut pos = 19;

    let frac_nanos = if pos < len && bytes[pos] == b'.' {
        pos += 1;
        let frac_start = pos;
        while pos < len && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
        let frac_digits = pos - frac_start;
        if frac_digits == 0 {
            return Err("invalid DateTime string".into());
        }
        let clamped = frac_digits.min(9);
        let frac_slice = &s[frac_start..frac_start + clamped];
        let mut frac: u32 = frac_slice
            .parse()
            .map_err(|_| "invalid fractional seconds".to_string())?;
        if clamped < 9 {
            frac *= 10u32.pow(9 - clamped as u32);
        }
        frac
    } else {
        0u32
    };

    if pos < len {
        if bytes[pos] == b'Z' {
            pos += 1;
        } else if (bytes[pos] == b'+' || bytes[pos] == b'-') && pos + 6 <= len {
            let sign = bytes[pos];
            let tz_slice = &s[pos + 1..pos + 6];
            if tz_slice.len() == 5
                && tz_slice.as_bytes()[2] == b':'
                && tz_slice[..2].bytes().all(|b| b.is_ascii_digit())
                && tz_slice[3..].bytes().all(|b| b.is_ascii_digit())
            {
                if sign == b'-' || &tz_slice[..2] != "00" || &tz_slice[3..] != "00" {
                    return Err(format!(
                        "non-UTC timezone offset '{}{}' not supported; convert to UTC first",
                        sign as char, tz_slice
                    ));
                }
                pos += 6;
            }
        }
    }

    if pos != len {
        return Err("invalid DateTime string".into());
    }

    let year: i32 = s[0..4].parse().map_err(|_| "invalid year".to_string())?;
    let month: u32 = s[5..7].parse().map_err(|_| "invalid month".to_string())?;
    let day: u32 = s[8..10].parse().map_err(|_| "invalid day".to_string())?;
    let hour: u32 = s[11..13].parse().map_err(|_| "invalid hour".to_string())?;
    let min: u32 = s[14..16]
        .parse()
        .map_err(|_| "invalid minute".to_string())?;
    let sec: u32 = s[17..19]
        .parse()
        .map_err(|_| "invalid second".to_string())?;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || min > 59 || sec > 59 {
        return Err("invalid DateTime string".into());
    }

    let days = civil_days_from_epoch(year, month, day);
    let secs = days * 86_400 + i64::from(hour) * 3600 + i64::from(min) * 60 + i64::from(sec);
    Ok((secs, frac_nanos))
}

/// Days since 1970-01-01 for a civil date (Howard Hinnant's algorithm).
fn civil_days_from_epoch(year: i32, month: u32, day: u32) -> i64 {
    let y = i64::from(if month <= 2 { year - 1 } else { year });
    let m = i64::from(if month <= 2 { month + 9 } else { month - 3 });
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * m + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

// ---------------------------------------------------------------------------
// Enum
// ---------------------------------------------------------------------------

/// Resolve an enum value to its signed discriminant. A numeric value is used
/// directly; a string is mapped via the `Enum8(...)` / `Enum16(...)` member
/// list parsed from the type string.
fn enum_discriminant(value: &Value, type_str: &str, col: &str) -> Result<i64, DynamicError> {
    match value {
        Value::Number(_) | Value::Bool(_) => as_i64(value, col),
        Value::String(s) => {
            // A numeric string is treated as the discriminant directly.
            if let Ok(n) = s.trim().parse::<i64>() {
                return Ok(n);
            }
            enum_member_value(type_str, s)
                .ok_or_else(|| enc_err(col, "enum string not found in type definition"))
        }
        _ => Err(enc_err(col, "expected enum value")),
    }
}

/// Look up `name` in an `Enum8(...)`/`Enum16(...)` definition, returning its
/// integer discriminant. Members look like `'name' = 1` separated by commas.
fn enum_member_value(type_str: &str, name: &str) -> Option<i64> {
    let open = type_str.find('(')?;
    let close = type_str.rfind(')')?;
    if open >= close {
        return None;
    }
    let inner = &type_str[open + 1..close];
    for member in inner.split(',') {
        let (label, num) = member.split_once('=')?;
        let label = label.trim().trim_matches('\'').trim_matches('"');
        if label == name {
            return num.trim().parse::<i64>().ok();
        }
    }
    None
}

// ---------------------------------------------------------------------------
// UUID / IP
// ---------------------------------------------------------------------------

fn encode_uuid(value: &Value, col: &str, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    let s = value_to_str(value);
    // Accept hyphen-less hex or any punctuation by keeping only hex digits.
    let hex: String = s.chars().filter(char::is_ascii_hexdigit).collect();
    if hex.len() != 32 {
        return Err(enc_err(col, "invalid UUID length"));
    }
    // ClickHouse RowBinary UUID: two little-endian u64, high word first.
    let high = u64::from_str_radix(&hex[..16], 16).map_err(|_| enc_err(col, "invalid UUID hex"))?;
    let low = u64::from_str_radix(&hex[16..], 16).map_err(|_| enc_err(col, "invalid UUID hex"))?;
    buf.extend_from_slice(&high.to_le_bytes());
    buf.extend_from_slice(&low.to_le_bytes());
    Ok(())
}

fn encode_ipv4(value: &Value, col: &str, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    // Integer input is interpreted as the host-order address.
    if let Value::Number(n) = value
        && let Some(u) = n.as_u64()
    {
        let addr = Ipv4Addr::from(u as u32);
        buf.extend_from_slice(&u32::from(addr).to_le_bytes());
        return Ok(());
    }
    let s = value_to_str(value);
    let addr: Ipv4Addr = s.parse().map_err(|_| enc_err(col, "invalid IPv4"))?;
    // ClickHouse stores IPv4 as UInt32 little-endian.
    buf.extend_from_slice(&u32::from(addr).to_le_bytes());
    Ok(())
}

fn encode_ipv6(value: &Value, col: &str, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    let s = value_to_str(value);
    let addr: Ipv6Addr = s.parse().map_err(|_| enc_err(col, "invalid IPv6"))?;
    // 16 octets in network byte order, written verbatim.
    buf.extend_from_slice(&addr.octets());
    Ok(())
}

// ---------------------------------------------------------------------------
// Array / Map
// ---------------------------------------------------------------------------

fn encode_array(
    value: &Value,
    elem: &ParsedType,
    col: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    let arr = match value {
        Value::Array(a) => a,
        _ => return Err(enc_err(col, "expected array")),
    };
    write_varint(arr.len() as u64, buf);
    for item in arr {
        encode_value(item, elem, col, buf)?;
    }
    Ok(())
}

fn encode_map(
    value: &Value,
    _key: &ParsedType,
    val: &ParsedType,
    col: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    let obj = match value {
        Value::Object(m) => m,
        _ => return Err(enc_err(col, "expected object for Map")),
    };
    write_varint(obj.len() as u64, buf);
    // ClickHouse Map keys are String on this path; write the key text directly.
    for (k, v) in obj {
        write_string(k.as_bytes(), buf);
        encode_value(v, val, col, buf)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Encode a one-row map against the given columns and return wire bytes.
    fn enc(row: Value, cols: &[(&str, &str)]) -> Vec<u8> {
        let columns: Vec<ColumnDef> = cols.iter().map(|(n, t)| ColumnDef::new(*n, *t)).collect();
        let obj = row.as_object().unwrap().clone();
        DynamicRow::new(&obj, &columns).encode().unwrap()
    }

    fn enc_err_of(row: Value, cols: &[(&str, &str)]) -> DynamicError {
        let columns: Vec<ColumnDef> = cols.iter().map(|(n, t)| ColumnDef::new(*n, *t)).collect();
        let obj = row.as_object().unwrap().clone();
        DynamicRow::new(&obj, &columns).encode().unwrap_err()
    }

    // ---- String / FixedString ----

    #[test]
    fn string() {
        assert_eq!(
            enc(json!({"s": "hello"}), &[("s", "String")]),
            vec![5, b'h', b'e', b'l', b'l', b'o']
        );
    }

    #[test]
    fn string_from_number() {
        assert_eq!(
            enc(json!({"s": 42}), &[("s", "String")]),
            vec![2, b'4', b'2']
        );
    }

    #[test]
    fn fixed_string_pad() {
        assert_eq!(
            enc(json!({"f": "ab"}), &[("f", "FixedString(4)")]),
            vec![b'a', b'b', 0, 0]
        );
    }

    #[test]
    fn fixed_string_truncate() {
        assert_eq!(
            enc(json!({"f": "abcdef"}), &[("f", "FixedString(3)")]),
            vec![b'a', b'b', b'c']
        );
    }

    // ---- Integers ----

    #[test]
    fn unsigned_widths() {
        assert_eq!(enc(json!({"x": 7}), &[("x", "UInt8")]), vec![7]);
        assert_eq!(
            enc(json!({"x": 300}), &[("x", "UInt16")]),
            300u16.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": 42}), &[("x", "UInt32")]),
            42u32.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": 42}), &[("x", "UInt64")]),
            42u64.to_le_bytes()
        );
    }

    #[test]
    fn signed_widths() {
        assert_eq!(
            enc(json!({"x": -5}), &[("x", "Int8")]),
            (-5i8).to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": -300}), &[("x", "Int16")]),
            (-300i16).to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": -1}), &[("x", "Int32")]),
            (-1i32).to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": -100}), &[("x", "Int64")]),
            (-100i64).to_le_bytes()
        );
    }

    #[test]
    fn int128_uint128() {
        assert_eq!(
            enc(json!({"x": -100}), &[("x", "Int128")]),
            (-100i128).to_le_bytes()
        );
        assert_eq!(
            enc(json!({"x": 100}), &[("x", "UInt128")]),
            100u128.to_le_bytes()
        );
        // From numeric string (large value beyond i64).
        let v: i128 = 170141183460469231731687303715884105727;
        assert_eq!(
            enc(json!({"x": v.to_string()}), &[("x", "Int128")]),
            v.to_le_bytes()
        );
    }

    #[test]
    fn int256_uint256() {
        let mut expect_neg = [0xFFu8; 32];
        expect_neg[..16].copy_from_slice(&(-1i128).to_le_bytes());
        assert_eq!(enc(json!({"x": -1}), &[("x", "Int256")]), expect_neg);

        let mut expect_pos = [0u8; 32];
        expect_pos[..16].copy_from_slice(&5u128.to_le_bytes());
        assert_eq!(enc(json!({"x": 5}), &[("x", "UInt256")]), expect_pos);
    }

    // ---- Floats / Bool ----

    #[test]
    fn floats() {
        assert_eq!(
            enc(json!({"f": 2.5}), &[("f", "Float64")]),
            2.5f64.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"f": 1.5}), &[("f", "Float32")]),
            1.5f32.to_le_bytes()
        );
    }

    #[test]
    fn bool_native() {
        assert_eq!(enc(json!({"b": true}), &[("b", "Bool")]), vec![1]);
        assert_eq!(enc(json!({"b": false}), &[("b", "Bool")]), vec![0]);
    }

    #[test]
    fn bool_coercions() {
        for (v, want) in [
            (json!(1), 1u8),
            (json!(0), 0),
            (json!("true"), 1),
            (json!("false"), 0),
            (json!("yes"), 1),
            (json!("no"), 0),
            (json!("1"), 1),
            (json!("0"), 0),
        ] {
            assert_eq!(enc(json!({"b": v}), &[("b", "Bool")]), vec![want]);
        }
    }

    // ---- Date / DateTime ----

    #[test]
    fn date_from_string() {
        // 2024-12-25 = day 20082 since epoch.
        let days = civil_days_from_epoch(2024, 12, 25) as u16;
        assert_eq!(
            enc(json!({"d": "2024-12-25"}), &[("d", "Date")]),
            days.to_le_bytes()
        );
    }

    #[test]
    fn date_epoch_string() {
        assert_eq!(
            enc(json!({"d": "1970-01-01"}), &[("d", "Date")]),
            0u16.to_le_bytes()
        );
    }

    #[test]
    fn date32_from_string() {
        let days = civil_days_from_epoch(2024, 12, 25) as i32;
        assert_eq!(
            enc(json!({"d": "2024-12-25"}), &[("d", "Date32")]),
            days.to_le_bytes()
        );
    }

    #[test]
    fn datetime_from_string() {
        // 2024-12-25 10:30:00 UTC = 1735122600.
        assert_eq!(
            enc(json!({"ts": "2024-12-25 10:30:00"}), &[("ts", "DateTime")]),
            1_735_122_600u32.to_le_bytes()
        );
    }

    #[test]
    fn datetime_from_number_seconds() {
        assert_eq!(
            enc(json!({"ts": 1_735_122_600u64}), &[("ts", "DateTime")]),
            1_735_122_600u32.to_le_bytes()
        );
    }

    #[test]
    fn datetime_from_epoch_ms_magnitude() {
        // ms value gets scaled down to seconds.
        assert_eq!(
            enc(json!({"ts": 1_735_122_600_000i64}), &[("ts", "DateTime")]),
            1_735_122_600u32.to_le_bytes()
        );
    }

    // ---- DateTime64 ----

    #[test]
    fn datetime64_from_string_ms() {
        let expected: i64 = 1_775_545_380_095;
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07 07:03:00.095"}),
                &[("ts", "DateTime64(3)")]
            ),
            expected.to_le_bytes()
        );
    }

    #[test]
    fn datetime64_iso8601_t_separator() {
        let expected: i64 = 1_775_545_380_095;
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07T07:03:00.095Z"}),
                &[("ts", "DateTime64(3)")]
            ),
            expected.to_le_bytes()
        );
    }

    #[test]
    fn datetime64_from_number_passthrough() {
        let v: i64 = 1_775_545_380_095;
        assert_eq!(
            enc(json!({"ts": v}), &[("ts", "DateTime64(3)")]),
            v.to_le_bytes()
        );
    }

    #[test]
    fn datetime64_precision_6_and_9() {
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07 07:03:00.095123"}),
                &[("ts", "DateTime64(6)")]
            ),
            1_775_545_380_095_123i64.to_le_bytes()
        );
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07 07:03:00.095123456"}),
                &[("ts", "DateTime64(9)")]
            ),
            1_775_545_380_095_123_456i64.to_le_bytes()
        );
    }

    #[test]
    fn datetime64_rejects_nonzero_offset() {
        let e = enc_err_of(
            json!({"ts": "2026-04-07 07:03:00+05:30"}),
            &[("ts", "DateTime64(3)")],
        );
        assert!(format!("{e}").contains("non-UTC"), "got: {e}");
    }

    // ---- Decimal ----

    #[test]
    fn decimal64() {
        // 123.45 at scale 2 -> backing 12345.
        assert_eq!(
            enc(json!({"d": 123.45}), &[("d", "Decimal(18, 2)")]),
            12_345i64.to_le_bytes()
        );
    }

    #[test]
    fn decimal_concrete_widths() {
        assert_eq!(
            enc(json!({"d": 1.5}), &[("d", "Decimal32(2)")]),
            150i32.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"d": 1.5}), &[("d", "Decimal64(2)")]),
            150i64.to_le_bytes()
        );
        assert_eq!(
            enc(json!({"d": 1.5}), &[("d", "Decimal128(2)")]),
            150i128.to_le_bytes()
        );
        let mut expect256 = [0u8; 32];
        expect256[..16].copy_from_slice(&150i128.to_le_bytes());
        assert_eq!(enc(json!({"d": 1.5}), &[("d", "Decimal256(2)")]), expect256);
    }

    // ---- UUID / IP ----

    #[test]
    fn uuid_hyphenated() {
        let bytes = enc(
            json!({"id": "12345678-1234-5678-1234-567812345678"}),
            &[("id", "UUID")],
        );
        let high = 0x1234_5678_1234_5678u64;
        let low = 0x1234_5678_1234_5678u64;
        let mut expected = high.to_le_bytes().to_vec();
        expected.extend_from_slice(&low.to_le_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn uuid_hyphenless_matches_hyphenated() {
        let with = enc(
            json!({"id": "12345678-1234-5678-1234-567812345678"}),
            &[("id", "UUID")],
        );
        let without = enc(
            json!({"id": "12345678123456781234567812345678"}),
            &[("id", "UUID")],
        );
        assert_eq!(with, without);
    }

    #[test]
    fn ipv4_dotted() {
        let bytes = enc(json!({"ip": "1.2.3.4"}), &[("ip", "IPv4")]);
        let addr: Ipv4Addr = "1.2.3.4".parse().unwrap();
        assert_eq!(bytes, u32::from(addr).to_le_bytes());
    }

    #[test]
    fn ipv4_from_integer() {
        let addr: Ipv4Addr = "1.2.3.4".parse().unwrap();
        let int = u32::from(addr);
        let bytes = enc(json!({"ip": int}), &[("ip", "IPv4")]);
        assert_eq!(bytes, int.to_le_bytes());
    }

    #[test]
    fn ipv6() {
        let bytes = enc(json!({"ip": "::1"}), &[("ip", "IPv6")]);
        let addr: Ipv6Addr = "::1".parse().unwrap();
        assert_eq!(bytes, addr.octets());
    }

    // ---- Enum ----

    #[test]
    fn enum8_and_enum16_numeric() {
        // Enum values are written as the underlying int discriminant. The
        // numeric discriminant is taken from the value directly.
        assert_eq!(
            enc(json!({"e": 2}), &[("e", "Enum8('a'=1,'b'=2)")]),
            vec![2]
        );
        assert_eq!(
            enc(json!({"e": 200}), &[("e", "Enum16('x'=100,'y'=200)")]),
            200i16.to_le_bytes()
        );
    }

    #[test]
    fn enum8_string_mapping() {
        // String maps to its discriminant from the type definition.
        assert_eq!(
            enc(
                json!({"e": "high"}),
                &[("e", "Enum8('low'=1, 'medium'=2, 'high'=3)")]
            ),
            vec![3]
        );
        assert_eq!(
            enc(json!({"e": "y"}), &[("e", "Enum16('x'=100, 'y'=200)")]),
            200i16.to_le_bytes()
        );
    }

    #[test]
    fn enum_unknown_string_errors() {
        let e = enc_err_of(json!({"e": "nope"}), &[("e", "Enum8('a'=1)")]);
        assert!(
            matches!(e, DynamicError::EncodingError { .. }),
            "got: {e:?}"
        );
    }

    // ---- Nullable ----

    #[test]
    fn nullable_null() {
        assert_eq!(
            enc(json!({"n": null}), &[("n", "Nullable(String)")]),
            vec![1]
        );
    }

    #[test]
    fn nullable_non_null() {
        assert_eq!(
            enc(json!({"n": "hi"}), &[("n", "Nullable(String)")]),
            vec![0, 2, b'h', b'i']
        );
    }

    #[test]
    fn nullable_datetime64() {
        let mut expected = vec![0u8];
        expected.extend_from_slice(&1_775_545_380_095i64.to_le_bytes());
        assert_eq!(
            enc(
                json!({"ts": "2026-04-07 07:03:00.095"}),
                &[("ts", "Nullable(DateTime64(3))")]
            ),
            expected
        );
    }

    #[test]
    fn non_nullable_null_gets_default() {
        assert_eq!(enc(json!({}), &[("x", "UInt32")]), 0u32.to_le_bytes());
        // Variable-length default is an empty string.
        assert_eq!(enc(json!({}), &[("s", "String")]), vec![0]);
    }

    // ---- LowCardinality ----

    #[test]
    fn low_cardinality_encodes_inner() {
        // LowCardinality(String) on the INSERT path is just the inner String.
        assert_eq!(
            enc(json!({"c": "x"}), &[("c", "LowCardinality(String)")]),
            vec![1, b'x']
        );
    }

    #[test]
    fn low_cardinality_nullable() {
        assert_eq!(
            enc(
                json!({"c": null}),
                &[("c", "LowCardinality(Nullable(String))")]
            ),
            vec![1]
        );
        assert_eq!(
            enc(
                json!({"c": "x"}),
                &[("c", "LowCardinality(Nullable(String))")]
            ),
            vec![0, 1, b'x']
        );
    }

    // ---- Array / Map ----

    #[test]
    fn array_uint32() {
        let mut expected = vec![3u8];
        for n in [1u32, 2, 3] {
            expected.extend_from_slice(&n.to_le_bytes());
        }
        assert_eq!(
            enc(json!({"a": [1, 2, 3]}), &[("a", "Array(UInt32)")]),
            expected
        );
    }

    #[test]
    fn array_nested() {
        // Array(Array(UInt8)): outer len 2, each inner len + bytes.
        let bytes = enc(json!({"a": [[1, 2], [3]]}), &[("a", "Array(Array(UInt8))")]);
        assert_eq!(bytes, vec![2, /*inner0*/ 2, 1, 2, /*inner1*/ 1, 3]);
    }

    #[test]
    fn array_nullable_elements() {
        // Array(Nullable(UInt8)): len 2, then per element null-marker + value.
        let bytes = enc(json!({"a": [5, null]}), &[("a", "Array(Nullable(UInt8))")]);
        assert_eq!(bytes, vec![2, 0, 5, 1]);
    }

    #[test]
    fn map_string_uint32() {
        let bytes = enc(json!({"m": {"a": 1}}), &[("m", "Map(String, UInt32)")]);
        let mut expected = vec![1u8]; // count
        expected.extend_from_slice(&[1, b'a']); // key "a"
        expected.extend_from_slice(&1u32.to_le_bytes()); // value
        assert_eq!(bytes, expected);
    }

    // ---- JSON ----

    #[test]
    fn json_from_object() {
        let bytes = enc(
            json!({"data": {"key": "value", "num": 42}}),
            &[("data", "JSON")],
        );
        let json_str = r#"{"key":"value","num":42}"#;
        let mut expected = Vec::new();
        write_varint(json_str.len() as u64, &mut expected);
        expected.extend_from_slice(json_str.as_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn json_from_string_verbatim() {
        let payload = r#"{"event":"login","user":"alice"}"#;
        let bytes = enc(json!({"data": payload}), &[("data", "JSON")]);
        let mut expected = Vec::new();
        write_varint(payload.len() as u64, &mut expected);
        expected.extend_from_slice(payload.as_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn json_missing_value_becomes_empty_object() {
        assert_eq!(enc(json!({}), &[("data", "JSON")]), vec![2, b'{', b'}']);
    }

    #[test]
    fn json_explicit_null_becomes_empty_object() {
        assert_eq!(
            enc(json!({"data": null}), &[("data", "JSON")]),
            vec![2, b'{', b'}']
        );
    }

    #[test]
    fn json_empty_string_becomes_empty_object() {
        assert_eq!(
            enc(json!({"data": ""}), &[("data", "JSON")]),
            vec![2, b'{', b'}']
        );
    }

    #[test]
    fn nullable_json_null_and_non_null() {
        assert_eq!(enc(json!({"t": null}), &[("t", "Nullable(JSON)")]), vec![1]);
        let bytes = enc(json!({"t": {"env": "prod"}}), &[("t", "Nullable(JSON)")]);
        let json_str = r#"{"env":"prod"}"#;
        let mut expected = vec![0u8];
        write_varint(json_str.len() as u64, &mut expected);
        expected.extend_from_slice(json_str.as_bytes());
        assert_eq!(bytes, expected);
    }

    // ---- Raw passthrough ----

    #[test]
    fn raw_passthrough_for_json_column() {
        let columns = vec![
            ColumnDef::new("id", "UInt32"),
            ColumnDef::new("_json", "Nullable(JSON)"),
        ];
        let row = json!({"id": 1}).as_object().unwrap().clone();
        let raw = br#"{"event":"login","user":"alice"}"#;
        let bytes = DynamicRow::with_raw(&row, &columns, raw, "_json")
            .encode()
            .unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&1u32.to_le_bytes());
        expected.push(0); // not null
        write_varint(raw.len() as u64, &mut expected);
        expected.extend_from_slice(raw);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn raw_passthrough_matches_value_path() {
        let columns = vec![ColumnDef::new("data", "JSON")];
        let payload = br#"{"key":"value","num":42}"#;

        let row_value = json!({"data": std::str::from_utf8(payload).unwrap()})
            .as_object()
            .unwrap()
            .clone();
        let via_value = DynamicRow::new(&row_value, &columns).encode().unwrap();

        let row_empty = json!({}).as_object().unwrap().clone();
        let via_raw = DynamicRow::with_raw(&row_empty, &columns, payload, "data")
            .encode()
            .unwrap();

        assert_eq!(via_value, via_raw);
    }

    // ---- Multi-column ----

    #[test]
    fn multi_column_order() {
        let bytes = enc(
            json!({"id": 42, "name": "test"}),
            &[("id", "UInt32"), ("name", "String")],
        );
        let mut expected = Vec::new();
        expected.extend_from_slice(&42u32.to_le_bytes());
        expected.extend_from_slice(&[4, b't', b'e', b's', b't']);
        assert_eq!(bytes, expected);
    }

    // ---- Unsupported types ----

    #[test]
    fn tuple_is_unsupported() {
        let e = enc_err_of(json!({"t": [1, 2]}), &[("t", "Tuple(UInt8, UInt8)")]);
        assert!(
            matches!(e, DynamicError::UnsupportedType { .. }),
            "got: {e:?}"
        );
    }

    #[test]
    fn variant_is_unsupported() {
        let e = enc_err_of(json!({"v": 1}), &[("v", "Variant(UInt8, String)")]);
        assert!(
            matches!(e, DynamicError::UnsupportedType { .. }),
            "got: {e:?}"
        );
    }

    #[test]
    fn point_is_unsupported() {
        let e = enc_err_of(json!({"p": [1.0, 2.0]}), &[("p", "Point")]);
        assert!(
            matches!(e, DynamicError::UnsupportedType { .. }),
            "got: {e:?}"
        );
    }

    // ---- varint ----

    #[test]
    fn varint_boundaries() {
        let mut buf = Vec::new();
        write_varint(0, &mut buf);
        assert_eq!(buf, vec![0]);
        buf.clear();
        write_varint(127, &mut buf);
        assert_eq!(buf, vec![127]);
        buf.clear();
        write_varint(128, &mut buf);
        assert_eq!(buf, vec![0x80, 0x01]);
        buf.clear();
        write_varint(300, &mut buf);
        assert_eq!(buf, vec![0xAC, 0x02]);
    }

    // ---- civil days ----

    #[test]
    fn civil_days_known() {
        assert_eq!(civil_days_from_epoch(1970, 1, 1), 0);
        assert_eq!(civil_days_from_epoch(2026, 4, 7), 20_550);
        assert_eq!(civil_days_from_epoch(1969, 12, 31), -1);
    }
}
