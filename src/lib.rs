// Project:   clickhouse-dfe
// File:      src/lib.rs
// Purpose:   Crate root -- feature-gated layers over the official client
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
// The crate docs are the README, whose prose names products, not code items;
// modules re-enable the lint where the names really are Rust or wire types.
#![allow(clippy::doc_markdown)]

pub mod native;

// Crate-internal, and the TCP connection actor is its only consumer, so
// it compiles with that transport rather than unconditionally.
#[cfg(feature = "tcp")]
mod worker;

pub mod error;

// `TcpClient::pool` still expects on a deadpool build that cannot fail.
#[cfg(feature = "tcp")]
#[cfg_attr(docsrs, doc(cfg(feature = "tcp")))]
#[allow(clippy::expect_used)]
pub mod tcp;

#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
pub mod tls;

#[cfg(feature = "dynamic")]
#[cfg_attr(docsrs, doc(cfg(feature = "dynamic")))]
pub mod dynamic;

#[cfg(feature = "unified")]
#[cfg_attr(docsrs, doc(cfg(feature = "unified")))]
pub mod unified;

#[cfg(feature = "ext")]
#[cfg_attr(docsrs, doc(cfg(feature = "ext")))]
pub mod ext;

pub use error::{Error, Result};

#[cfg(feature = "tcp")]
pub use tcp::TcpClient;

#[cfg(feature = "unified")]
pub use unified::{Columns, Transport, UnifiedClient};

#[cfg(feature = "ext")]
pub use ext::{ClientExt, ServerException};
