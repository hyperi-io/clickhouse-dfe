// Project:   clickhouse-dfe
// File:      tests/smoke.rs
// Purpose:   Prove the crate's own entry points construct from outside it
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Smoke tests. Nothing here opens a connection, so the suite is hermetic and
//! needs no ClickHouse server.
//!
//! These run against the crate's PUBLIC surface, so a re-export that stops
//! being reachable fails here rather than in a consumer's build.

// The prose here names a product, not a code item.
#![allow(clippy::doc_markdown)]

/// Every entry point a consumer starts from must be constructible by name from
/// the crate root, and must report the state it was built with.
#[test]
fn the_crate_constructs_its_own_entry_points() {
    #[cfg(feature = "tcp")]
    {
        let tcp = clickhouse_dfe::TcpClient::new("127.0.0.1:9000").with_pool_size(3);
        assert_eq!(tcp.pool().status().max_size, 3);
    }

    #[cfg(feature = "unified")]
    {
        use clickhouse_dfe::{Transport, UnifiedClient};

        let http = UnifiedClient::Http(clickhouse::Client::default().with_url("http://h:8123"));
        assert_eq!(http.transport(), Transport::Http);
        assert!(http.as_http().is_some(), "the HTTP arm exposes its client");
        assert!(http.as_tcp().is_none());

        let tcp = UnifiedClient::Tcp(clickhouse_dfe::TcpClient::new("127.0.0.1:9000"));
        assert_eq!(tcp.transport(), Transport::Tcp);
        assert!(tcp.as_tcp().is_some(), "the TCP arm exposes its client");
        assert!(tcp.as_http().is_none());
    }

    #[cfg(feature = "dynamic")]
    {
        use clickhouse_dfe::dynamic::{ColumnDef, DynamicSchema, DynamicSchemaCache, TypeTag};
        use std::sync::Arc;
        use std::time::Duration;

        let schema = Arc::new(DynamicSchema::from_columns(
            "db.t",
            vec![
                ColumnDef::new("id", "UInt64"),
                ColumnDef::with_default_kind("seen", "DateTime64(3)", "DEFAULT"),
            ],
        ));
        assert_eq!(schema.len(), 2);
        assert_eq!(schema.required_columns().count(), 1);
        assert_eq!(
            schema.column("id").expect("declared above").ty.tag,
            TypeTag::UInt64
        );

        let cache = DynamicSchemaCache::new(Duration::from_secs(60));
        assert!(cache.get("db.t").is_none());
        cache.insert("db.t", Arc::clone(&schema));
        assert_eq!(cache.get("db.t").expect("just inserted").len(), 2);
    }

    #[cfg(feature = "ext")]
    {
        use clickhouse_dfe::ServerException;

        let parsed = ServerException::parse(&clickhouse::error::Error::BadResponse(
            "Code: 60. DB::Exception: Table db.t does not exist. (UNKNOWN_TABLE)".into(),
        ))
        .expect("a well-formed exception body parses");
        assert_eq!(parsed.code, 60);
        assert!(!parsed.is_retriable(), "UNKNOWN_TABLE is terminal");
    }
}

/// The public types cross task boundaries in a consumer's runtime, so losing
/// `Send`/`Sync` on one of them is a breaking change worth catching here. An
/// open insert is `Send` only -- it owns a connection and is driven from one
/// task at a time.
#[test]
fn public_types_are_send_and_sync() {
    use static_assertions::assert_impl_all;

    assert_impl_all!(clickhouse_dfe::Error: Send, Sync);

    #[cfg(feature = "unified")]
    {
        assert_impl_all!(clickhouse_dfe::UnifiedClient: Send, Sync, Clone);
        assert_impl_all!(clickhouse_dfe::Columns: Send, Sync, Clone);
    }

    #[cfg(feature = "dynamic")]
    {
        use clickhouse_dfe::dynamic::{
            DynamicError, DynamicInsert, DynamicSchema, DynamicSchemaCache,
        };
        assert_impl_all!(DynamicSchema: Send, Sync, Clone);
        assert_impl_all!(DynamicSchemaCache: Send, Sync);
        assert_impl_all!(DynamicError: Send, Sync);
        assert_impl_all!(DynamicInsert: Send);
    }

    #[cfg(feature = "ext")]
    assert_impl_all!(clickhouse_dfe::ServerException: Send, Sync, Clone);
}
