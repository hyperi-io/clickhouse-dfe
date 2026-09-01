// Project:   clickhouse-dfe
// File:      tests/tcp_public_surface.rs
// Purpose:   Drive the public TCP surface offline -- builder plus handle bounds
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Public TCP surface, exercised offline -- nothing here opens a socket.

#![cfg(feature = "tcp")]

use std::time::Duration;

use clickhouse_dfe::TcpClient;
use clickhouse_dfe::tcp::connection_actor::ConnectionHandle;
use clickhouse_dfe::tcp::{RetryPolicy, TcpInsertSession, TcpRawCursor};

/// Every builder knob has to be readable back before a connection exists.
#[test]
fn tcp_client_builds_and_configures_offline() {
    let client = TcpClient::new("127.0.0.1:9000")
        .with_addrs(["127.0.0.1:9000", "127.0.0.1:9001"])
        .with_database("dfe")
        .with_user("default")
        .with_password("secret")
        .with_setting("max_execution_time", "30")
        .with_roles(["reader"])
        .with_retry(RetryPolicy::default())
        .with_pool_size(4)
        .with_pool_acquire_timeout(Some(Duration::from_secs(5)))
        .with_read_timeout(Some(Duration::from_secs(60)));

    assert_eq!(client.pool().status().max_size, 4);
    assert!(client.retry().is_some());

    let settings = client.insert_settings();
    assert_eq!(settings[0], ("database".to_string(), "dfe".to_string()));
    assert!(settings.contains(&("max_execution_time".to_string(), "30".to_string())));
    assert!(settings.contains(&("role".to_string(), "reader".to_string())));
}

/// The thread bounds are part of the public contract, not an implementation
/// detail.
#[test]
fn handles_carry_the_thread_bounds_callers_rely_on() {
    static_assertions::assert_impl_all!(TcpClient: Send, Sync, Clone);
    static_assertions::assert_impl_all!(ConnectionHandle: Send, Sync);
    static_assertions::assert_impl_all!(TcpRawCursor: Send);
    static_assertions::assert_impl_all!(TcpInsertSession: Send);
}
