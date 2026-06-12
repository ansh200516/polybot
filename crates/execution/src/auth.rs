//! Polymarket CLOB auth: L1 (EIP-712 ClobAuth → create/derive API key) and
//! L2 (HMAC headers on every trading request). RECON-M5-pinned recipes.
//!
//! Recipes are pinned against `py-clob-client 0.34.6` and proven byte-identical
//! by the reference vectors in `tests/fixtures/auth_vectors.json`. A mismatch
//! with docs/RECON-M5.md is a stop-and-fix, not a local edit.

use alloy_primitives::{U256, hex};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolStruct, eip712_domain, sol};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::secrets::ApiCreds;

/// RECON-pinned attestation message (RECON-M5.md / py_clob_client `MSG_TO_SIGN`).
const CLOB_AUTH_MESSAGE: &str = "This message attests that I control the given wallet";

sol! {
    /// Field NAMES and ORDER are the EIP-712 typestring — never reorder.
    struct ClobAuth {
        address address;
        string timestamp;
        uint256 nonce;
        string message;
    }
}

#[derive(Debug)]
pub enum AuthError {
    /// The local signer failed to produce a signature.
    Sign(String),
    /// The L2 secret was not valid base64url, or could not key the HMAC.
    BadSecret(String),
    /// An auth HTTP request failed, or the venue returned an unusable response.
    Http(String),
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::Sign(e) => write!(f, "auth signing error: {e}"),
            AuthError::BadSecret(e) => write!(f, "invalid L2 secret: {e}"),
            AuthError::Http(e) => write!(f, "auth http error: {e}"),
        }
    }
}

impl std::error::Error for AuthError {}

/// L1: sign the ClobAuth attestation. Returns the 65-byte `r || s || v`
/// signature as a `0x`-prefixed hex string.
///
/// The ClobAuth domain has NO `verifyingContract` (RECON-M5.md); the
/// `eip712_domain!` macro encodes the absent field as `None`, matching
/// `py_clob_client.signing.eip712`.
pub fn l1_signature(
    signer: &PrivateKeySigner,
    timestamp: &str,
    nonce: u64,
) -> Result<String, AuthError> {
    let auth = ClobAuth {
        address: signer.address(),
        timestamp: timestamp.to_string(),
        nonce: U256::from(nonce),
        message: CLOB_AUTH_MESSAGE.to_string(),
    };
    let domain = eip712_domain! {
        name: "ClobAuthDomain",
        version: "1",
        chain_id: crate::sign::CHAIN_ID,
    };
    let hash = auth.eip712_signing_hash(&domain);
    let sig = signer
        .sign_hash_sync(&hash)
        .map_err(|e| AuthError::Sign(e.to_string()))?;
    Ok(format!("0x{}", hex::encode(sig.as_bytes())))
}

/// L2: `base64url-with-padding(HMAC-SHA256(base64url-decode(secret),
/// ts + METHOD + path + body))`. For GET requests `path` must EXCLUDE query
/// params (RECON-M5.md: the HMAC signs the request path only). `body` must be
/// the EXACT serialized string sent on the wire — canonical double-quoted
/// JSON (the reference client normalizes single quotes; we never emit them).
pub fn l2_signature(
    secret_b64url: &str,
    timestamp: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Result<String, AuthError> {
    let key = URL_SAFE
        .decode(secret_b64url)
        .map_err(|e| AuthError::BadSecret(e.to_string()))?;
    let mut mac =
        Hmac::<Sha256>::new_from_slice(&key).map_err(|e| AuthError::BadSecret(e.to_string()))?;
    mac.update(timestamp.as_bytes());
    mac.update(method.as_bytes());
    mac.update(path.as_bytes());
    if let Some(b) = body {
        mac.update(b.as_bytes());
    }
    Ok(URL_SAFE.encode(mac.finalize().into_bytes()))
}

/// The five L2 headers for a trading request, in the order py-clob-client
/// sends them. `eoa_address` is the signing EOA (POLY_ADDRESS).
pub fn l2_headers(
    creds: &ApiCreds,
    eoa_address: &str,
    timestamp: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Result<Vec<(&'static str, String)>, AuthError> {
    let signature = l2_signature(creds.secret.expose(), timestamp, method, path, body)?;
    Ok(vec![
        ("POLY_ADDRESS", eoa_address.to_string()),
        ("POLY_SIGNATURE", signature),
        ("POLY_TIMESTAMP", timestamp.to_string()),
        ("POLY_API_KEY", creds.key.clone()),
        ("POLY_PASSPHRASE", creds.passphrase.expose().to_string()),
    ])
}

/// Pull a string field from the venue's JSON response, trying `primary` first
/// and then any `fallback` name (the API-key field is `apiKey` in
/// py-clob-client's `CreateApiKeyResponse`; `api_key` is cheap insurance).
fn json_str<'a>(
    v: &'a serde_json::Value,
    primary: &str,
    fallback: Option<&str>,
) -> Option<&'a str> {
    v.get(primary)
        .or_else(|| fallback.and_then(|f| v.get(f)))
        .and_then(serde_json::Value::as_str)
}

/// Derive (`GET /auth/derive-api-key`) then create (`POST /auth/api-key`) the
/// API key with L1 headers; the first success wins. `server_time_s` is the
/// venue clock (seconds) used for the L1 timestamp.
///
/// Polymarket returns the same deterministic key for a given wallet from both
/// endpoints; derive is the idempotent read, create is the first-time write.
/// Trying derive first avoids a spurious "already exists" on re-runs while
/// still bootstrapping a fresh wallet via create.
pub async fn derive_or_create_api_key(
    http: &reqwest::Client,
    base: &str,
    signer: &PrivateKeySigner,
    server_time_s: u64,
) -> Result<ApiCreds, AuthError> {
    let timestamp = server_time_s.to_string();
    let signature = l1_signature(signer, &timestamp, 0)?;
    // alloy's `Address` Display is EIP-55 checksummed — matches what
    // py-clob-client sends for POLY_ADDRESS.
    let address = signer.address().to_string();

    let mut last_err = String::from("no auth endpoints attempted");
    for (method, path) in [("GET", "/auth/derive-api-key"), ("POST", "/auth/api-key")] {
        let url = format!("{base}{path}");
        let req = match method {
            "POST" => http.post(&url),
            _ => http.get(&url),
        }
        .header("POLY_ADDRESS", &address)
        .header("POLY_SIGNATURE", &signature)
        .header("POLY_TIMESTAMP", &timestamp)
        .header("POLY_NONCE", "0");

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                last_err = format!("{method} {path}: request failed: {e}");
                continue;
            }
        };
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            last_err = format!("{method} {path}: HTTP {status}: {text}");
            continue;
        }
        let json: serde_json::Value = match serde_json::from_str(&text) {
            Ok(j) => j,
            Err(e) => {
                last_err = format!("{method} {path}: HTTP {status}: invalid JSON: {e}: {text}");
                continue;
            }
        };
        match (
            json_str(&json, "apiKey", Some("api_key")),
            json_str(&json, "secret", None),
            json_str(&json, "passphrase", None),
        ) {
            (Some(key), Some(secret), Some(passphrase)) => {
                return Ok(ApiCreds {
                    key: key.to_string(),
                    secret: crate::secrets::Secret::new(secret.to_string()),
                    passphrase: crate::secrets::Secret::new(passphrase.to_string()),
                });
            }
            _ => {
                last_err = format!(
                    "{method} {path}: HTTP {status}: response missing apiKey/secret/passphrase: {text}"
                );
            }
        }
    }
    Err(AuthError::Http(format!(
        "derive and create both failed: {last_err}"
    )))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use alloy_signer_local::PrivateKeySigner;

    #[test]
    fn l1_signature_matches_reference_vector() {
        let raw = include_str!("../tests/fixtures/auth_vectors.json");
        let v: serde_json::Value = serde_json::from_str(raw).unwrap();
        let l1 = &v["l1"];
        let signer: PrivateKeySigner = l1["private_key"].as_str().unwrap().parse().unwrap();
        let sig = l1_signature(&signer, l1["timestamp"].as_str().unwrap(), l1["nonce"].as_u64().unwrap()).unwrap();
        assert_eq!(sig, l1["signature"].as_str().unwrap());
    }

    #[test]
    fn l2_hmac_matches_reference_vectors() {
        let raw = include_str!("../tests/fixtures/auth_vectors.json");
        let v: serde_json::Value = serde_json::from_str(raw).unwrap();
        let arr = v["l2"].as_array().unwrap();
        assert_eq!(arr.len(), 2, "POST-with-body and GET-without-body vectors");
        for vec in arr {
            let sig = l2_signature(
                vec["secret"].as_str().unwrap(),
                vec["timestamp"].as_str().unwrap(),
                vec["method"].as_str().unwrap(),
                vec["path"].as_str().unwrap(),
                vec["body"].as_str(), // None when JSON null
            )
            .unwrap();
            assert_eq!(sig, vec["signature"].as_str().unwrap(), "method {}", vec["method"]);
        }
    }

    #[test]
    fn l2_headers_carry_all_five() {
        let creds = crate::secrets::ApiCreds {
            key: "k".into(),
            secret: crate::secrets::Secret::new("QQ==".into()),
            passphrase: crate::secrets::Secret::new("p".into()),
        };
        let h = l2_headers(&creds, "0xabc", "123", "GET", "/data/orders", None).unwrap();
        let names: Vec<&str> = h.iter().map(|(n, _)| *n).collect();
        assert_eq!(
            names,
            vec!["POLY_ADDRESS", "POLY_SIGNATURE", "POLY_TIMESTAMP", "POLY_API_KEY", "POLY_PASSPHRASE"]
        );
        assert_eq!(h[0].1, "0xabc");
        assert_eq!(h[3].1, "k");
        assert_eq!(h[4].1, "p");
    }
}
