//! Env-only secret loading for live trading (spec 2026-06-13 §Config & env).
//!
//! Secrets never appear in config files, git, logs, or SQLite. `Secret`'s
//! Debug impl is redacted; there is intentionally NO Display impl.

/// An owned secret string. Debug prints `Secret(<redacted>)`; no Display.
///
/// `Clone` is derived so that two consumers can each own the same resolved
/// credential without re-deriving it (Task 4.5: arb and the market maker each
/// build a `LiveVenue` for the SAME account, and each venue owns its creds by
/// value). Cloning duplicates the in-memory owned `String` only — the redacted
/// `Debug` and the absent `Display` still guarantee the value is never logged.
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    pub fn new(s: String) -> Self {
        Secret(s)
    }
    /// The raw value. Callers must never log it.
    pub fn expose(&self) -> &str {
        &self.0
    }
    /// Hex private key with any `0x` prefix stripped.
    pub fn expose_key_hex(&self) -> String {
        self.0.strip_prefix("0x").unwrap_or(&self.0).to_string()
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Secret(<redacted>)")
    }
}

/// Pre-derived L2 API credentials (all three or none).
///
/// `Clone` (Task 4.5): arb and the market maker each construct a `LiveVenue`
/// (which owns its `ApiCreds` by value) for the SAME account, so the resolved
/// creds are cloned once rather than derived twice. The cloned `Secret` fields
/// stay redacted in `Debug`.
#[derive(Debug, Clone)]
pub struct ApiCreds {
    pub key: String,
    pub secret: Secret,
    pub passphrase: Secret,
}

/// Polymarket Relayer API-key credentials for the M6 deposit-wallet relayer
/// (current scheme — the "Relayer API keys" settings page). Two STATIC headers,
/// no HMAC/secret: `RELAYER_API_KEY` (the key) + `RELAYER_API_KEY_ADDRESS` (the
/// owner/signer address the key is bound to). Sent verbatim on every relayer
/// request; the wallet ACTION is authorized by the EIP-712 Batch signature, so
/// these only authenticate the submitter to the relayer service.
#[derive(Debug, Clone)]
pub struct RelayerCreds {
    pub api_key: String,
    pub api_key_address: String,
}

/// Everything live mode reads from the environment.
#[derive(Debug)]
pub struct LiveSecrets {
    pub private_key: Secret,
    /// Magic/email accounts: the Polymarket proxy wallet (maker). Optional —
    /// when absent the binary refuses to start live (RECON-M5: no reliable
    /// unauthenticated lookup; the operator copies it from the profile page).
    pub proxy_address: Option<String>,
    /// V2 deposit-wallet address (the order `maker`, signatureType 3 / POLY_1271).
    /// New API accounts must trade via the deposit wallet (RECON-M5-V2-1271).
    /// Optional here; the binary requires it in live mode and copies it from the
    /// Polymarket UI (no reliable unauthenticated derivation — clone type needs
    /// an on-chain probe).
    pub deposit_wallet: Option<String>,
    /// When present, API-key derivation is skipped.
    pub api: Option<ApiCreds>,
    /// Polymarket Relayer API-key credentials for the M6 deposit-wallet relayer
    /// (`RELAYER_API_KEY` + `RELAYER_API_KEY_ADDRESS`, the current "Relayer API
    /// keys" settings scheme). Both or none. Absent → the relayer is not
    /// configured and live merge/redeem stays the hold-to-resolution no-op
    /// (spec 2026-06-25 §7).
    pub relayer: Option<RelayerCreds>,
    /// Polygon JSON-RPC URL (`RPC_URL`), used to read the deposit-wallet batch
    /// nonce (M6-5). Absent → relayer not configured.
    pub rpc_url: Option<String>,
}

impl LiveSecrets {
    /// Production entrypoint.
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Testable core: `lookup` returns the env value for a name.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let private_key = Secret::new(
            lookup("PM_PRIVATE_KEY").ok_or_else(|| "PM_PRIVATE_KEY not set (export your wallet key from Polymarket settings)".to_string())?,
        );
        let proxy_address = lookup("PM_PROXY_ADDRESS");
        let deposit_wallet = lookup("PM_DEPOSIT_WALLET");
        let api = match (lookup("PM_API_KEY"), lookup("PM_API_SECRET"), lookup("PM_API_PASSPHRASE")) {
            (None, None, None) => None,
            (Some(key), Some(secret), Some(pass)) => Some(ApiCreds {
                key,
                secret: Secret::new(secret),
                passphrase: Secret::new(pass),
            }),
            (k, s, p) => {
                let mut missing = Vec::new();
                if k.is_none() {
                    missing.push("PM_API_KEY");
                }
                if s.is_none() {
                    missing.push("PM_API_SECRET");
                }
                if p.is_none() {
                    missing.push("PM_API_PASSPHRASE");
                }
                return Err(format!("partial PM_API_* credentials; missing: {}", missing.join(", ")));
            }
        };
        // Relayer API-key creds (M6, current scheme): two STATIC headers,
        // both-or-none (no HMAC secret/passphrase).
        let relayer = match (lookup("RELAYER_API_KEY"), lookup("RELAYER_API_KEY_ADDRESS")) {
            (None, None) => None,
            (Some(api_key), Some(api_key_address)) => Some(RelayerCreds {
                api_key,
                api_key_address,
            }),
            (k, a) => {
                let mut missing = Vec::new();
                if k.is_none() {
                    missing.push("RELAYER_API_KEY");
                }
                if a.is_none() {
                    missing.push("RELAYER_API_KEY_ADDRESS");
                }
                return Err(format!("partial RELAYER_API_* credentials; missing: {}", missing.join(", ")));
            }
        };
        let rpc_url = lookup("RPC_URL");
        Ok(LiveSecrets {
            private_key,
            proxy_address,
            deposit_wallet,
            api,
            relayer,
            rpc_url,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn secret_debug_is_redacted() {
        let s = Secret::new("0xdeadbeef".into());
        assert_eq!(format!("{s:?}"), "Secret(<redacted>)");
        assert_eq!(s.expose(), "0xdeadbeef");
    }

    #[test]
    fn private_key_strips_0x_prefix() {
        let s = Secret::new(format!("0x{}", "ab".repeat(32)));
        assert_eq!(s.expose_key_hex(), "ab".repeat(32));
        let s = Secret::new("ab".repeat(32));
        assert_eq!(s.expose_key_hex(), "ab".repeat(32));
    }

    #[test]
    fn live_secrets_from_env_reports_what_is_missing() {
        // Read from a closure-provided lookup so tests don't mutate process env.
        let lookup = |k: &str| match k {
            "PM_PRIVATE_KEY" => Some("0x".to_string() + &"cd".repeat(32)),
            "PM_PROXY_ADDRESS" => Some("0x".to_string() + &"11".repeat(20)),
            "PM_DEPOSIT_WALLET" => Some("0x".to_string() + &"22".repeat(20)),
            _ => None,
        };
        let s = LiveSecrets::from_lookup(lookup).unwrap();
        assert_eq!(s.private_key.expose_key_hex(), "cd".repeat(32));
        let expected_proxy = format!("0x{}", "11".repeat(20));
        assert_eq!(s.proxy_address.as_deref(), Some(expected_proxy.as_str()));
        let expected_deposit = format!("0x{}", "22".repeat(20));
        assert_eq!(
            s.deposit_wallet.as_deref(),
            Some(expected_deposit.as_str()),
            "PM_DEPOSIT_WALLET is read (V2 deposit-wallet maker)"
        );
        assert!(s.api.is_none(), "no PM_API_* given → derive at startup");
        assert!(s.relayer.is_none(), "no RELAYER_API_* given → relayer not configured");
        assert!(s.rpc_url.is_none(), "no RPC_URL given → relayer not configured");

        let none = |_: &str| None::<String>;
        let err = LiveSecrets::from_lookup(none).unwrap_err();
        assert!(err.contains("PM_PRIVATE_KEY"), "error names the missing var: {err}");
        assert!(
            err.contains("Polymarket settings"),
            "error keeps the export-from-settings guidance: {err}"
        );
    }

    #[test]
    fn api_creds_require_all_three() {
        let partial = |k: &str| match k {
            "PM_PRIVATE_KEY" => Some("ab".repeat(32)),
            "PM_API_KEY" => Some("key".into()),
            _ => None,
        };
        let err = LiveSecrets::from_lookup(partial).unwrap_err();
        assert!(err.contains("PM_API_SECRET"), "{err}");
        assert!(err.contains("PM_API_PASSPHRASE"), "both missing vars are named: {err}");
    }

    #[test]
    fn relayer_creds_load_from_env() {
        // Both RELAYER_API_* present → Some(RelayerCreds) with the key + bound
        // owner/signer address passed through verbatim; RPC_URL loaded alongside
        // (kept for compatibility, now unused by the relayer path).
        let lookup = |k: &str| match k {
            "PM_PRIVATE_KEY" => Some("0x".to_string() + &"cd".repeat(32)),
            "RELAYER_API_KEY" => Some("703629aa-relayer-key".to_string()),
            "RELAYER_API_KEY_ADDRESS" => Some("0x".to_string() + &"33".repeat(20)),
            "RPC_URL" => Some("https://polygon-bor-rpc.publicnode.com".to_string()),
            _ => None,
        };
        let s = LiveSecrets::from_lookup(lookup).unwrap();
        let r = s.relayer.unwrap();
        assert_eq!(r.api_key, "703629aa-relayer-key");
        assert_eq!(r.api_key_address, format!("0x{}", "33".repeat(20)));
        assert_eq!(
            s.rpc_url.as_deref(),
            Some("https://polygon-bor-rpc.publicnode.com")
        );
    }

    #[test]
    fn relayer_creds_require_both() {
        // Partial RELAYER_API_* (key only) → error naming the missing address,
        // mirroring the PM_API_* all-or-none contract.
        let partial = |k: &str| match k {
            "PM_PRIVATE_KEY" => Some("ab".repeat(32)),
            "RELAYER_API_KEY" => Some("rkey".into()),
            _ => None,
        };
        let err = LiveSecrets::from_lookup(partial).unwrap_err();
        assert!(
            err.contains("RELAYER_API_KEY_ADDRESS"),
            "the missing var is named: {err}"
        );
    }
}
