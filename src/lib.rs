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

// The documentation and pedantic lint set in Cargo.toml is enforced in
// `native`; a module listed here carries the allow until it is clean, and
// removing its entry is how it opts in.
//
// `unwrap_used` and `expect_used` are deliberately absent: a panic in library
// code is a defect in any module, and `clippy.toml` exempts test code. A module
// still carrying one declares that allow beside its own `mod`.
//
// The docs.rs feature badge is emitted here, so a feature-gated module cannot
// be added without being labelled.
macro_rules! ratcheted {
    ($(
        $(#[cfg(feature = $feature:literal)])?
        $(#[allow($($allow:meta),*)])?
        $vis:vis mod $name:ident;
    )*) => {
        $(
            $(#[cfg(feature = $feature)])?
            $(#[cfg_attr(docsrs, doc(cfg(feature = $feature)))])?
            $(#[allow($($allow),*)])?
            #[allow(
                missing_docs,
                clippy::pedantic,
                clippy::missing_errors_doc,
                clippy::missing_panics_doc
            )]
            $vis mod $name;
        )*
    };
}

pub mod native;

// Crate-internal, and the TCP connection actor is its only consumer, so
// it compiles with that transport rather than unconditionally.
#[cfg(feature = "tcp")]
mod worker;

ratcheted! {
    pub mod error;

    // `TcpClient::pool` still expects on a deadpool build that cannot fail.
    #[cfg(feature = "tcp")]
    #[allow(clippy::expect_used)]
    pub mod tcp;

    #[cfg(feature = "tls")]
    pub mod tls;

    #[cfg(feature = "dynamic")]
    pub mod dynamic;

    #[cfg(feature = "unified")]
    pub mod unified;

    #[cfg(feature = "ext")]
    pub mod ext;
}

pub use error::{Error, Result};

#[cfg(feature = "tcp")]
pub use tcp::TcpClient;

#[cfg(feature = "unified")]
pub use unified::{Columns, Transport, UnifiedClient};

#[cfg(feature = "ext")]
pub use ext::{ClientExt, ServerException};
