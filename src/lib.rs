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

// The pedantic and rustdoc lint set in Cargo.toml is enforced in `native`;
// every other module carries the allow until its own remediation wave clears
// it. Removing an entry here is how a module opts in.
macro_rules! ratcheted {
    ($($(#[$attr:meta])* $vis:vis mod $name:ident;)*) => {
        $(
            $(#[$attr])*
            #[allow(
                missing_docs,
                clippy::pedantic,
                clippy::unwrap_used,
                clippy::expect_used,
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
}

pub use error::{Error, Result};

#[cfg(feature = "tcp")]
pub use tcp::TcpClient;
