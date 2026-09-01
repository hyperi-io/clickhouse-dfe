// Project:   clickhouse-dfe
// File:      src/lib.rs
// Purpose:   Crate root -- feature-gated layers over the official client
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod error;
pub mod native;
pub mod worker;

#[cfg(feature = "tcp")]
pub mod tcp;

#[cfg(feature = "tls")]
pub mod tls;

#[cfg(feature = "dynamic")]
pub mod dynamic;

#[cfg(feature = "unified")]
pub mod unified;

#[cfg(feature = "ext")]
pub mod ext;

#[cfg(feature = "inserter")]
pub mod inserter;

pub use error::{Error, Result};

#[cfg(feature = "tcp")]
pub use tcp::TcpClient;
