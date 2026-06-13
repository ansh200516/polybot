//! Polymarket CLOB auth: L1 (EIP-712 ClobAuth → create/derive API key) and
//! L2 (HMAC headers on every trading request). RECON-M5-pinned recipes.
//!
//! Recipes are pinned against `py-clob-client 0.34.6` and proven byte-identical
//! by the reference vectors in `tests/fixtures/auth_vectors.json`. A mismatch
//! with docs/RECON-M5.md is a stop-and-fix, not a local edit.

use alloy_primitives::{Address, U256, hex};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{Eip712Domain, SolStruct, eip712_domain, sol};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tracing::info;

use crate::secrets::ApiCreds;

/// RECON-pinned attestation message (RECON-M5.md / py_clob_client `MSG_TO_SIGN`).
const CLOB_AUTH_MESSAGE: &str = "This message attests that I control the given wallet";

/// The ClobAuth EIP-712 type string (`contentsType` for the ERC-7739 wrap). The
/// SAME string the `sol! ClobAuth` derives; pinned so the deposit-wallet L1 wrap
/// is byte-exact (asserted equal to alloy's derived type string in tests).
const CLOB_AUTH_CONTENTS_TYPE: &str =
    "ClobAuth(address address,string timestamp,uint256 nonce,string message)";

sol! {
    /// Field NAMES and ORDER are the EIP-712 typestring — never reorder.
    struct ClobAuth {
        address address;
        string timestamp;
        uint256 nonce;
        string message;
    }
}

/// The ClobAuth app EIP-712 domain: name "ClobAuthDomain", version "1", chainId,
/// and NO `verifyingContract` (RECON-M5.md / py-clob-client-v2 signing/eip712.py).
fn clob_auth_domain(chain_id: u64) -> Eip712Domain {
    eip712_domain! {
        name: "ClobAuthDomain",
        version: "1",
        chain_id: chain_id,
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

/// Build the ClobAuth struct for the given attestation `address` (the address
/// the API key BINDS to: the EOA for a plain/proxy account, the deposit wallet
/// for a POLY_1271 account — RECON-M5-V2-1271 "Auth binding").
fn clob_auth(address: Address, timestamp: &str, nonce: u64) -> ClobAuth {
    ClobAuth {
        address,
        timestamp: timestamp.to_string(),
        nonce: U256::from(nonce),
        message: CLOB_AUTH_MESSAGE.to_string(),
    }
}

/// L1 (plain EOA / POLY_PROXY): sign the ClobAuth attestation with the EOA's own
/// address in the `address` field. Returns the 65-byte `r || s || v` signature
/// as a `0x`-prefixed hex string. Binds the API key to the **EOA**.
///
/// The ClobAuth domain has NO `verifyingContract` (RECON-M5.md); the
/// `eip712_domain!` macro encodes the absent field as `None`, matching
/// `py_clob_client.signing.eip712`.
pub fn l1_signature(
    signer: &PrivateKeySigner,
    timestamp: &str,
    nonce: u64,
) -> Result<String, AuthError> {
    let auth = clob_auth(signer.address(), timestamp, nonce);
    let hash = auth.eip712_signing_hash(&clob_auth_domain(crate::sign::CHAIN_ID));
    let sig = signer
        .sign_hash_sync(&hash)
        .map_err(|e| AuthError::Sign(e.to_string()))?;
    Ok(format!("0x{}", hex::encode(sig.as_bytes())))
}

/// L1 (POLY_1271 deposit wallet): sign the ClobAuth attestation so the API key
/// BINDS to the **deposit wallet**. The ClobAuth `address` field is the deposit
/// wallet; the signature is an ERC-7739 `TypedDataSign`-wrapped signature (the
/// EOA signs on behalf of the deposit wallet; the CLOB validates it via the
/// deposit wallet's ERC-1271 `isValidSignature`). RECON-M5-V2-1271 "Auth
/// binding" (py-clob-client-v2 #70 / clob-client-v2 #65 fix sketches).
///
/// Reuses the SAME ERC-7739 nesting proven byte-exact for orders
/// (`sign::erc7739_wrap`): app domain = ClobAuthDomain (NOT the exchange
/// domain), contents = the ClobAuth struct, wallet domain =
/// DepositWallet/1/chainId/deposit_wallet/salt0.
pub fn l1_signature_1271(
    signer: &PrivateKeySigner,
    timestamp: &str,
    nonce: u64,
    deposit_wallet: Address,
) -> Result<String, AuthError> {
    let chain_id = crate::sign::CHAIN_ID;
    let auth = clob_auth(deposit_wallet, timestamp, nonce);
    let app_domain_separator = clob_auth_domain(chain_id).hash_struct();
    let contents_hash = auth.eip712_hash_struct();
    crate::sign::erc7739_wrap(
        signer,
        app_domain_separator,
        contents_hash,
        CLOB_AUTH_CONTENTS_TYPE,
        &crate::sign::wallet_domain(chain_id, deposit_wallet),
    )
    .map_err(|e| AuthError::Sign(e.to_string()))
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

/// Re-serialise a JSON object with `secret` and `passphrase` REDACTED, every
/// other field shown verbatim — so the create/derive response can be logged
/// (INFO) to reveal any bound `address`/`profileAddress` the venue returns,
/// without leaking the L2 credential material. Non-objects are returned as-is
/// (they carry no secret fields).
fn redacted_json(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(map) => {
            let mut out = map.clone();
            for k in ["secret", "passphrase"] {
                if out.contains_key(k) {
                    out.insert(k.to_string(), serde_json::Value::String("<redacted>".into()));
                }
            }
            serde_json::Value::Object(out)
        }
        other => other.clone(),
    }
}

/// Derive (`GET /auth/derive-api-key`) then create (`POST /auth/api-key`) the
/// API key with L1 headers; the first success wins. `server_time_s` is the
/// venue clock (seconds) used for the L1 timestamp.
///
/// `deposit_wallet`:
/// - `None` → plain EOA / POLY_PROXY account: ClobAuth.address = EOA, plain
///   ECDSA L1 signature, POLY_ADDRESS = EOA → the API key binds to the EOA.
/// - `Some(w)` → POLY_1271 deposit wallet: ClobAuth.address = `w`, ERC-7739
///   wrapped L1 signature, POLY_ADDRESS = `w` → the API key binds to the
///   **deposit wallet** (so order.signer = deposit wallet matches the bound
///   key). RECON-M5-V2-1271 "Auth binding".
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
    deposit_wallet: Option<Address>,
) -> Result<ApiCreds, AuthError> {
    let timestamp = server_time_s.to_string();
    // The address the API key binds to == the ClobAuth.address == POLY_ADDRESS.
    // alloy's `Address` Display is EIP-55 checksummed — matches py-clob-client.
    let (signature, bind_address, sig_kind) = match deposit_wallet {
        Some(w) => (l1_signature_1271(signer, &timestamp, 0, w)?, w, "1271-wrapped"),
        None => (l1_signature(signer, &timestamp, 0)?, signer.address(), "plain-eoa"),
    };
    let address = bind_address.to_string();
    // Diagnostic (no secrets): which address the key is being bound to and how.
    // RECON-M5-V2-1271 "Auth binding" — the live venue requires order.signer ==
    // this bound address. EOA is shown for comparison with the deposit wallet.
    info!(
        bind_address = %address,
        eoa = %signer.address(),
        l1_signature = sig_kind,
        "deriving API key (ClobAuth.address / POLY_ADDRESS = bind_address)"
    );

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
        // Diagnostic (no secrets): the FULL response with secret/passphrase
        // redacted. Reveals any bound `address`/`profileAddress` the venue
        // returns — the decisive evidence for which address the key is bound to.
        info!(
            method,
            path,
            response = %redacted_json(&json),
            "API key endpoint response (secret/passphrase redacted)"
        );
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

    /// The pinned `CLOB_AUTH_CONTENTS_TYPE` (used to build the ERC-7739 typehash
    /// AND emitted as the wire `contentsType` for the deposit-wallet L1 wrap)
    /// MUST equal the type string alloy derives from the `sol! ClobAuth` struct.
    #[test]
    fn clob_auth_contents_type_matches_sol_derived() {
        assert_eq!(
            CLOB_AUTH_CONTENTS_TYPE,
            ClobAuth::eip712_encode_type(),
            "pinned ClobAuth contentsType drifted from the sol! ClobAuth type string"
        );
    }

    /// Deposit-wallet L1 (RECON-M5-V2-1271 "Auth binding"): the signature is an
    /// ERC-7739 `TypedDataSign` wrap, NOT a plain 65-byte ECDSA. No Polymarket
    /// published vector exists (the SDK bug means the official clients never
    /// produce one), so this is a STRUCTURAL self-check pinned to the same Solady
    /// scheme `sign::erc7739_wrap` proves byte-exact for orders:
    ///  - it differs from the plain-EOA L1 signature (binds a different address),
    ///  - the wire embeds the ClobAuthDomain separator + the ClobAuth contents
    ///    hash (over ClobAuth.address = deposit wallet),
    ///  - the leading 65 bytes recover to the EOA over the nested digest.
    #[test]
    fn l1_signature_1271_is_erc7739_wrapped_and_binds_deposit_wallet() {
        use alloy_primitives::{Address, Signature, keccak256};
        let signer: PrivateKeySigner = "ad".repeat(32).parse().unwrap();
        let deposit_wallet: Address = format!("0x{}", "22".repeat(20)).parse().unwrap();
        let ts = "1750000000";

        let wrapped = l1_signature_1271(&signer, ts, 0, deposit_wallet).unwrap();
        let plain = l1_signature(&signer, ts, 0).unwrap();
        assert_ne!(wrapped, plain, "1271 wrap must differ from the plain-EOA L1 sig");

        let bytes = hex::decode(wrapped.trim_start_matches("0x")).unwrap();
        // innerSig(65) ‖ appDomainSep(32) ‖ contentsHash(32) ‖ contentsType ‖ len(2)
        let ct = CLOB_AUTH_CONTENTS_TYPE.as_bytes();
        assert_eq!(bytes.len(), 65 + 32 + 32 + ct.len() + 2, "ERC-7739 wire layout");

        // Embedded appDomainSeparator = OUR ClobAuthDomain separator.
        let app_sep = clob_auth_domain(crate::sign::CHAIN_ID).hash_struct();
        assert_eq!(&bytes[65..97], app_sep.as_slice(), "embedded ClobAuthDomain separator");

        // Embedded contentsHash = ClobAuth struct hash with address = deposit wallet.
        let auth = clob_auth(deposit_wallet, ts, 0);
        assert_eq!(&bytes[97..129], auth.eip712_hash_struct().as_slice(), "embedded contentsHash binds deposit wallet");

        // Trailer: contentsType bytes + uint16_be(len).
        assert_eq!(&bytes[129..129 + ct.len()], ct, "wire contentsType");
        let len = u16::from_be_bytes([bytes[bytes.len() - 2], bytes[bytes.len() - 1]]);
        assert_eq!(usize::from(len), ct.len(), "contentsType length trailer");

        // Inner 65 bytes recover to the EOA over the nested 7739 digest.
        let mut buf = [0u8; 7 * 32];
        let preimage = format!(
            "TypedDataSign(ClobAuth contents,string name,string version,uint256 chainId,address verifyingContract,bytes32 salt){CLOB_AUTH_CONTENTS_TYPE}"
        );
        buf[0..32].copy_from_slice(keccak256(preimage.as_bytes()).as_slice());
        buf[32..64].copy_from_slice(auth.eip712_hash_struct().as_slice());
        buf[64..96].copy_from_slice(keccak256(b"DepositWallet").as_slice());
        buf[96..128].copy_from_slice(keccak256(b"1").as_slice());
        buf[128..160].copy_from_slice(&U256::from(crate::sign::CHAIN_ID).to_be_bytes::<32>());
        buf[172..192].copy_from_slice(deposit_wallet.as_slice());
        let hash_struct = keccak256(buf);
        let mut digest = [0u8; 66];
        digest[0] = 0x19;
        digest[1] = 0x01;
        digest[2..34].copy_from_slice(app_sep.as_slice());
        digest[34..66].copy_from_slice(hash_struct.as_slice());
        let nested = keccak256(digest);
        let sig = Signature::from_raw(&bytes[0..65]).unwrap();
        assert_eq!(
            sig.recover_address_from_prehash(&nested).unwrap(),
            signer.address(),
            "inner sig must recover to the EOA over the nested 7739 digest"
        );
    }

    #[test]
    fn redacted_json_hides_secret_and_passphrase_only() {
        let v = serde_json::json!({
            "apiKey": "k-123",
            "secret": "SUPERSECRET",
            "passphrase": "PHRASE",
            "address": "0xDEADBEEF",
            "profileAddress": "0xFEED",
        });
        let r = redacted_json(&v);
        assert_eq!(r["secret"], "<redacted>");
        assert_eq!(r["passphrase"], "<redacted>");
        assert_eq!(r["apiKey"], "k-123", "non-secret fields preserved");
        assert_eq!(r["address"], "0xDEADBEEF", "bound address must be visible");
        assert_eq!(r["profileAddress"], "0xFEED");
        // A non-object value passes through unchanged.
        assert_eq!(redacted_json(&serde_json::json!("x")), serde_json::json!("x"));
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
