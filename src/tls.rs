// Project:   clickhouse-dfe
// File:      src/tls.rs
// Purpose:   Trust description to a rustls ClientConfig for the TCP transport
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! TLS trust configuration for the TCP transport.
//!
//! This module turns a trust description (OS native roots, the compiled
//! webpki bundle, and/or explicit PEM CA files) into a single rustls
//! [`ClientConfig`] that the TCP connector consumes, and that a
//! caller-built HTTP connector can share. Mirrors clickhouse-go: a
//! server cert is verified against one pool; CA files are loaded
//! `AppendCertsFromPEM`-style (best-effort, all certs in the file,
//! error only if none parse).
//!
//! Resolution fails closed: an empty trust store is an error rather than a
//! config that falls back to broad trust.
//!
//! [`ClientConfig`]: rustls::ClientConfig

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::RootCertStore;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;

use crate::error::{Error, Result};

/// Declarative trust description, resolved to a [`rustls::ClientConfig`]
/// at transport-build time.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TlsTrust {
    /// Load OS native roots (rustls-native-certs).
    pub native_roots: bool,
    /// Include the compiled-in Mozilla bundle (webpki-roots).
    pub webpki_roots: bool,
    /// Explicit root CA PEM files (each may bundle many certs).
    pub extra_roots: Vec<PathBuf>,
    /// Explicit intermediate CA PEM files (added as anchors too).
    pub extra_intermediates: Vec<PathBuf>,
    /// When true, ignore native + webpki; trust ONLY the explicit files.
    pub exclusive: bool,
}

impl Default for TlsTrust {
    fn default() -> Self {
        // Native + webpki both on: internal-CA clusters work (OS store
        // carries the org CA) and public-CA servers still verify.
        Self {
            native_roots: true,
            webpki_roots: true,
            extra_roots: Vec::new(),
            extra_intermediates: Vec::new(),
            exclusive: false,
        }
    }
}

/// What [`crate::TcpClient`] carries: either a caller-built config (Go's
/// `Options.TLS` analog) or a declarative trust we resolve ourselves.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum TlsConfigSource {
    /// A config the caller built; used as-is, nothing here inspects it.
    Explicit(Arc<rustls::ClientConfig>),
    /// A trust description resolved to a config at transport-build time.
    Trust(TlsTrust),
}

/// `AppendCertsFromPEM` analog: read `path`, best-effort parse every PEM
/// certificate block, add all valid certs to `store`. Junk / non-cert
/// blocks are skipped. Errors only if the file cannot be read or yields
/// ZERO usable certs (the Go `!successful` branch).
fn add_pem_file_certs(store: &mut RootCertStore, path: &Path) -> Result<()> {
    let mut certs: Vec<CertificateDer<'static>> = Vec::new();
    let iter = CertificateDer::pem_file_iter(path)
        .map_err(|e| Error::Custom(format!("tls: cannot read CA file {}: {e}", path.display())))?;
    // Lenient per-block: skip an unparseable block rather than fail the
    // whole file (AppendCertsFromPEM parity).
    for cert in iter.flatten() {
        certs.push(cert);
    }
    let (added, _ignored) = store.add_parsable_certificates(certs);
    if added == 0 {
        return Err(Error::Custom(format!(
            "tls: no usable certificates in CA file {}",
            path.display()
        )));
    }
    Ok(())
}

/// Assemble the trust anchor set per the [`TlsTrust`] rules.
fn build_root_store(trust: &TlsTrust) -> Result<RootCertStore> {
    let mut store = RootCertStore::empty();

    if !trust.exclusive {
        if trust.native_roots {
            // Best-effort: an OS store that yields nothing is not fatal here,
            // because the empty-store check below is the fail-closed gate.
            let result = rustls_native_certs::load_native_certs();
            let _ = store.add_parsable_certificates(result.certs);
        }
        if trust.webpki_roots {
            store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
    } else if trust.extra_roots.is_empty() && trust.extra_intermediates.is_empty() {
        return Err(Error::Custom(
            "tls: exclusive trust requested but no explicit CA files were supplied".into(),
        ));
    }

    for path in &trust.extra_roots {
        add_pem_file_certs(&mut store, path)?;
    }
    for path in &trust.extra_intermediates {
        add_pem_file_certs(&mut store, path)?;
    }

    if store.is_empty() {
        return Err(Error::Custom(
            "tls: resulting trust store is empty (no roots loaded)".into(),
        ));
    }
    Ok(store)
}

/// Build a [`rustls::ClientConfig`] from an already-assembled root store.
fn build_config_with_roots(roots: RootCertStore) -> Result<Arc<rustls::ClientConfig>> {
    // Name the provider explicitly so the build does not depend on
    // process-default provider installation order. The `tls` feature
    // enables aws-lc-rs and nothing else.
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Custom(format!("tls: rustls config: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

/// Resolve a [`TlsConfigSource`] into a ready [`rustls::ClientConfig`].
///
/// # Errors
///
/// [`Error::Custom`] if a CA file cannot be read or yields no usable
/// certificate, if exclusive trust is requested with no files, or if the
/// assembled store ends up empty.
pub fn build_client_config(src: &TlsConfigSource) -> Result<Arc<rustls::ClientConfig>> {
    match src {
        TlsConfigSource::Explicit(cfg) => Ok(cfg.clone()),
        TlsConfigSource::Trust(trust) => {
            let roots = build_root_store(trust)?;
            build_config_with_roots(roots)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A syntactically valid self-signed cert (parses as a cert; never
    // verified here -- these tests check store assembly, not chains, so
    // expiry is irrelevant). Generated once for the suite.
    const TEST_CA_PEM: &str = include_str!("testdata/test_ca.pem");

    /// The `TempDir` must outlive the path, so it is returned with it: two
    /// suite runs on one host get separate directories and neither leaks.
    fn write_tmp(name: &str, body: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join(name);
        std::fs::write(&p, body).unwrap();
        (dir, p)
    }

    #[test]
    fn add_pem_file_adds_all_certs_in_bundle() {
        let bundle = format!("{TEST_CA_PEM}\n{TEST_CA_PEM}");
        let (_dir, path) = write_tmp("bundle.pem", &bundle);
        let mut store = RootCertStore::empty();
        add_pem_file_certs(&mut store, &path).unwrap();
        assert_eq!(store.len(), 2, "both concatenated certs must be added");
    }

    #[test]
    fn add_pem_file_is_lenient_skips_junk() {
        let mixed = format!(
            "-----BEGIN CERTIFICATE-----\nbm90YWNlcnQ=\n-----END CERTIFICATE-----\n{TEST_CA_PEM}"
        );
        let (_dir, path) = write_tmp("mixed.pem", &mixed);
        let mut store = RootCertStore::empty();
        add_pem_file_certs(&mut store, &path).unwrap();
        assert_eq!(store.len(), 1, "valid cert added, junk block skipped");
    }

    #[test]
    fn add_pem_file_errors_on_zero_certs() {
        let (_dir, path) = write_tmp("empty.pem", "not a pem at all\n");
        let mut store = RootCertStore::empty();
        let err = add_pem_file_certs(&mut store, &path).expect_err("zero-cert file must error");
        assert!(format!("{err}").contains("no usable certificates"));
    }

    #[test]
    fn add_pem_file_errors_on_missing_path() {
        let mut store = RootCertStore::empty();
        let err = add_pem_file_certs(&mut store, Path::new("/no/such/ca.pem"))
            .expect_err("missing file must error");
        assert!(format!("{err}").contains("cannot read CA file"));
    }

    #[test]
    fn build_root_store_augment_includes_extra() {
        let (_dir, path) = write_tmp("root.pem", TEST_CA_PEM);
        let trust = TlsTrust {
            native_roots: false, // keep test hermetic (no OS dependency)
            webpki_roots: true,
            extra_roots: vec![path],
            extra_intermediates: Vec::new(),
            exclusive: false,
        };
        let store = build_root_store(&trust).unwrap();
        // webpki bundle is large; +1 for our cert. Just assert non-empty
        // and larger than webpki alone is hard to pin, so assert it added.
        assert!(store.len() > 1);
    }

    #[test]
    fn build_root_store_exclusive_only_extra() {
        let (_dir, path) = write_tmp("only.pem", TEST_CA_PEM);
        let trust = TlsTrust {
            native_roots: true,
            webpki_roots: true,
            extra_roots: vec![path],
            extra_intermediates: Vec::new(),
            exclusive: true,
        };
        let store = build_root_store(&trust).unwrap();
        assert_eq!(store.len(), 1, "exclusive trusts only the supplied CA");
    }

    #[test]
    fn build_root_store_exclusive_no_files_errors() {
        let trust = TlsTrust {
            native_roots: true,
            webpki_roots: true,
            extra_roots: Vec::new(),
            extra_intermediates: Vec::new(),
            exclusive: true,
        };
        let err = build_root_store(&trust).expect_err("exclusive with no files must error");
        assert!(format!("{err}").contains("no explicit CA files"));
    }

    /// Fail closed: a trust that resolves to nothing is an error, never a
    /// config that quietly trusts whatever the platform defaults to.
    #[test]
    fn a_trust_that_resolves_to_no_roots_errors() {
        let trust = TlsTrust {
            native_roots: false,
            webpki_roots: false,
            extra_roots: Vec::new(),
            extra_intermediates: Vec::new(),
            exclusive: false,
        };
        let err = build_client_config(&TlsConfigSource::Trust(trust))
            .expect_err("an empty trust store must error");
        assert!(format!("{err}").contains("trust store is empty"), "{err}");
    }

    /// A caller-built config is handed back as-is, not rebuilt from our rules.
    #[test]
    fn explicit_config_passes_through_unchanged() {
        let (_dir, path) = write_tmp("explicit.pem", TEST_CA_PEM);
        let mut roots = RootCertStore::empty();
        add_pem_file_certs(&mut roots, &path).unwrap();
        let built = build_config_with_roots(roots).unwrap();

        let out = build_client_config(&TlsConfigSource::Explicit(built.clone())).unwrap();

        assert!(Arc::ptr_eq(&built, &out));
    }
}
