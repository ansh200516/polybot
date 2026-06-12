//! Env-only secret loading for live trading (spec 2026-06-13 §Config & env).
//!
//! Secrets never appear in config files, git, logs, or SQLite. `Secret`'s
//! Debug impl is redacted; there is intentionally NO Display impl.

/// An owned secret string. Debug prints `Secret(<redacted>)`; no Display.
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
#[derive(Debug)]
pub struct ApiCreds {
    pub key: String,
    pub secret: Secret,
    pub passphrase: Secret,
}

/// Everything live mode reads from the environment.
#[derive(Debug)]
pub struct LiveSecrets {
    pub private_key: Secret,
    /// Magic/email accounts: the Polymarket proxy wallet (maker). Optional —
    /// when absent the binary refuses to start live (RECON-M5: no reliable
    /// unauthenticated lookup; the operator copies it from the profile page).
    pub proxy_address: Option<String>,
    /// When present, API-key derivation is skipped.
    pub api: Option<ApiCreds>,
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
        Ok(LiveSecrets {
            private_key,
            proxy_address,
            api,
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
            _ => None,
        };
        let s = LiveSecrets::from_lookup(lookup).unwrap();
        assert_eq!(s.private_key.expose_key_hex(), "cd".repeat(32));
        assert_eq!(s.proxy_address.as_deref(), Some(&*("0x".to_string() + &"11".repeat(20))));
        assert!(s.api.is_none(), "no PM_API_* given → derive at startup");

        let none = |_: &str| None::<String>;
        let err = LiveSecrets::from_lookup(none).unwrap_err();
        assert!(err.contains("PM_PRIVATE_KEY"), "error names the missing var: {err}");
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
    }
}
