//! TLS certificate utilities for Blockyard mutual TLS.
//!
//! Provides helpers to generate a self-signed CA and node certificates using
//! `rcgen`, and to load PEM-encoded certificates / keys for `rustls`
//! configuration.

use std::path::Path;
use std::sync::Arc;

use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, KeyUsagePurpose};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tracing::info;

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Certificate generation
// ---------------------------------------------------------------------------

/// A generated certificate together with its private key, both in PEM form.
#[derive(Debug, Clone)]
pub struct GeneratedCert {
    /// PEM-encoded X.509 certificate.
    pub cert_pem: String,
    /// PEM-encoded private key (PKCS#8).
    pub key_pem: String,
}

/// Generate a self-signed Certificate Authority (CA) certificate.
///
/// The returned [`GeneratedCert`] contains the PEM-encoded CA certificate and
/// private key which can be written to disk or used directly with
/// [`generate_node_cert`].
pub fn generate_ca() -> Result<GeneratedCert> {
    let key_pair =
        KeyPair::generate().map_err(|e| Error::Config(format!("CA key generation failed: {e}")))?;

    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Blockyard CA");

    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| Error::Config(format!("CA self-sign failed: {e}")))?;

    info!("generated self-signed CA certificate");

    Ok(GeneratedCert {
        cert_pem: cert.pem(),
        key_pem: key_pair.serialize_pem(),
    })
}

/// Generate a node certificate signed by the given CA.
///
/// `ca_pem` and `ca_key_pem` are the PEM strings previously produced by
/// [`generate_ca`]. `node_name` is embedded as the certificate Common Name and
/// as a DNS Subject Alternative Name.
pub fn generate_node_cert(
    _ca_pem: &str,
    ca_key_pem: &str,
    node_name: &str,
) -> Result<GeneratedCert> {
    // Reconstruct the CA key pair so we can sign with it.
    let ca_key = KeyPair::from_pem(ca_key_pem)
        .map_err(|e| Error::Config(format!("failed to parse CA key: {e}")))?;

    // Build CA params (needed to create the issuer reference).
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Blockyard CA");

    // Self-sign the CA params to get a Certificate we can use as issuer.
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .map_err(|e| Error::Config(format!("failed to rebuild CA cert: {e}")))?;

    // Build the node certificate parameters.
    let node_key = KeyPair::generate()
        .map_err(|e| Error::Config(format!("node key generation failed: {e}")))?;

    let mut node_params = CertificateParams::new(vec![node_name.to_string()])
        .map_err(|e| Error::Config(format!("invalid node SAN: {e}")))?;
    node_params.is_ca = IsCa::NoCa;
    node_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    node_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, node_name);

    // Sign the node certificate with the CA cert + key.
    let node_cert = node_params
        .signed_by(&node_key, &ca_cert, &ca_key)
        .map_err(|e| Error::Config(format!("node cert signing failed: {e}")))?;

    info!(node = %node_name, "generated node certificate signed by CA");

    Ok(GeneratedCert {
        cert_pem: node_cert.pem(),
        key_pem: node_key.serialize_pem(),
    })
}

// ---------------------------------------------------------------------------
// PEM loading helpers
// ---------------------------------------------------------------------------

/// Load PEM-encoded certificates from a file, returning DER-encoded certs.
pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let pem_data = std::fs::read(path)
        .map_err(|e| Error::Config(format!("failed to read cert file {}: {e}", path.display())))?;

    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut &pem_data[..])
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                Error::Config(format!("failed to parse certs from {}: {e}", path.display()))
            })?;

    if certs.is_empty() {
        return Err(Error::Config(format!(
            "no certificates found in {}",
            path.display()
        )));
    }

    Ok(certs)
}

/// Load a PEM-encoded private key from a file, returning the DER-encoded key.
pub fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let pem_data = std::fs::read(path)
        .map_err(|e| Error::Config(format!("failed to read key file {}: {e}", path.display())))?;

    rustls_pemfile::private_key(&mut &pem_data[..])
        .map_err(|e| Error::Config(format!("failed to parse key from {}: {e}", path.display())))?
        .ok_or_else(|| {
            Error::Config(format!("no private key found in {}", path.display()))
        })
}

// ---------------------------------------------------------------------------
// rustls configuration builders
// ---------------------------------------------------------------------------

/// Build a [`rustls::ServerConfig`] for mutual TLS.
///
/// * `cert_path` – PEM file containing the server certificate chain.
/// * `key_path`  – PEM file containing the server private key.
/// * `ca_path`   – PEM file containing the trusted CA certificate(s) used to
///   verify client certificates.
pub fn build_server_config(
    cert_path: &Path,
    key_path: &Path,
    ca_path: &Path,
) -> Result<rustls::ServerConfig> {
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    let ca_certs = load_certs(ca_path)?;
    let mut root_store = rustls::RootCertStore::empty();
    for ca in &ca_certs {
        root_store.add(ca.clone()).map_err(|e| {
            Error::Config(format!("failed to add CA cert to root store: {e}"))
        })?;
    }

    let client_verifier =
        rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| Error::Config(format!("failed to build client verifier: {e}")))?;

    let config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certs, key)
        .map_err(|e| Error::Config(format!("server TLS config error: {e}")))?;

    Ok(config)
}

/// Build a [`rustls::ClientConfig`] for mutual TLS.
///
/// * `cert_path` – PEM file containing the client certificate chain.
/// * `key_path`  – PEM file containing the client private key.
/// * `ca_path`   – PEM file containing the trusted CA certificate(s) used to
///   verify the server.
pub fn build_client_config(
    cert_path: &Path,
    key_path: &Path,
    ca_path: &Path,
) -> Result<rustls::ClientConfig> {
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    let ca_certs = load_certs(ca_path)?;
    let mut root_store = rustls::RootCertStore::empty();
    for ca in &ca_certs {
        root_store.add(ca.clone()).map_err(|e| {
            Error::Config(format!("failed to add CA cert to root store: {e}"))
        })?;
    }

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(certs, key)
        .map_err(|e| Error::Config(format!("client TLS config error: {e}")))?;

    Ok(config)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_generate_ca() {
        let ca = generate_ca().expect("CA generation should succeed");
        assert!(ca.cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(ca.key_pem.contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn test_generate_node_cert() {
        let ca = generate_ca().unwrap();
        let node =
            generate_node_cert(&ca.cert_pem, &ca.key_pem, "node-1").expect("node cert should work");
        assert!(node.cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(node.key_pem.contains("BEGIN PRIVATE KEY"));
        // The node cert must be different from the CA cert.
        assert_ne!(node.cert_pem, ca.cert_pem);
    }

    #[test]
    fn test_round_trip_load_certs_and_key() {
        let ca = generate_ca().unwrap();
        let node = generate_node_cert(&ca.cert_pem, &ca.key_pem, "round-trip-node").unwrap();

        let dir = tempdir();

        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");

        std::fs::write(&cert_path, &node.cert_pem).unwrap();
        std::fs::write(&key_path, &node.key_pem).unwrap();

        let certs = load_certs(&cert_path).expect("should load certs");
        assert_eq!(certs.len(), 1);

        let _key = load_key(&key_path).expect("should load key");
    }

    #[test]
    fn test_round_trip_server_client_config() {
        let ca = generate_ca().unwrap();
        let server_cert = generate_node_cert(&ca.cert_pem, &ca.key_pem, "server-node").unwrap();
        let client_cert = generate_node_cert(&ca.cert_pem, &ca.key_pem, "client-node").unwrap();

        let dir = tempdir();

        let ca_path = dir.join("ca.pem");
        let srv_cert_path = dir.join("server.pem");
        let srv_key_path = dir.join("server-key.pem");
        let cli_cert_path = dir.join("client.pem");
        let cli_key_path = dir.join("client-key.pem");

        std::fs::write(&ca_path, &ca.cert_pem).unwrap();
        std::fs::write(&srv_cert_path, &server_cert.cert_pem).unwrap();
        std::fs::write(&srv_key_path, &server_cert.key_pem).unwrap();
        std::fs::write(&cli_cert_path, &client_cert.cert_pem).unwrap();
        std::fs::write(&cli_key_path, &client_cert.key_pem).unwrap();

        let _server_cfg =
            build_server_config(&srv_cert_path, &srv_key_path, &ca_path).expect("server config");
        let _client_cfg =
            build_client_config(&cli_cert_path, &cli_key_path, &ca_path).expect("client config");
    }

    #[test]
    fn test_load_certs_missing_file() {
        let r = load_certs(Path::new("/nonexistent/cert.pem"));
        assert!(r.is_err());
    }

    #[test]
    fn test_load_key_missing_file() {
        let r = load_key(Path::new("/nonexistent/key.pem"));
        assert!(r.is_err());
    }

    #[test]
    fn test_load_certs_empty_pem() {
        let dir = tempdir();
        let path = dir.join("empty.pem");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"not a real PEM\n").unwrap();
        let r = load_certs(&path);
        assert!(r.is_err());
    }

    /// Quick-and-dirty temp directory helper for tests.
    fn tempdir() -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!("blockyard-tls-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
