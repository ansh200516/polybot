//! Polymarket deposit-wallet RELAYER client (M6): live merge/redeem via the
//! relayer WALLET batch. See
//! docs/superpowers/specs/2026-06-25-m6-deposit-wallet-relayer-design.md
//!
//! M6-1 lays only the Polygon-137 contract constants + (Step 2) the builder
//! credentials used by the later tasks (EIP-712 `Batch` signing, merge/redeem
//! calldata, the relayer I/O). NO signing, ABI encoding, or network I/O happens
//! here yet — this is foundational scaffolding only.

use alloy_primitives::{Address, U256, address, hex};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolStruct, eip712_domain, sol};

// ---------------------------------------------------------------------------
// Polygon-137 contracts
// ---------------------------------------------------------------------------

/// Deposit-wallet factory (Polygon 137) — the `to` of a WALLET batch — pinned
/// to the `py-builder-relayer-client@e7108cd` config this port targets
/// (design §2.3).
///
/// NOTE: Polymarket's *current* public docs list a NEWER factory
/// (`0x00000000000Fb5C9ADea0298D729A0CB3823Cc07`, beacon-based). We deliberately
/// pin the reference-client value the rest of M6 is ported against; reconcile
/// against the live factory before the funded staging run if the relayer
/// rejects the batch.
pub const DEPOSIT_WALLET_FACTORY: Address =
    address!("0x894Ee6B254f251518206f709E9B115f214ebDf17");

/// Deposit-wallet implementation (Polygon 137), same pinned reference config.
/// Consumed by the M6-3 CREATE2 / ERC-1967 address-derivation sanity helper.
pub const DEPOSIT_WALLET_IMPL: Address =
    address!("0x55913A0bdecCbB77b7Af781A48300e6394B5EEAE");

/// pUSD-native `CtfCollateralAdapter` (Polygon 137) — the merge/redeem TARGET
/// for STANDARD (binary) CTF markets. A WALLET batch call targets this adapter:
/// it pulls the deposit wallet's YES/NO ERC-1155 positions, runs the underlying
/// CTF merge/redeem, wraps the proceeds back into pUSD, and returns pUSD to the
/// wallet. Calldata (M6-3): `mergePositions(collateral, parentCollectionId=0x0,
/// conditionId, partition=[1,2], amount)` / `redeemPositions(.., indexSets=[1,2])`.
///
/// CONFIRMED (resolves design §6 Unknown B / M6-B) from THREE agreeing sources:
///   1. Polymarket's contracts reference (docs.polymarket.com/resources/contracts),
///   2. Polymarket's "Inventory Management" merge/split code example — same
///      address with exactly the design §2.3 `mergePositions` ABI, and
///   3. a registry verified live on-chain (chainID 137) + Polygonscan (2026-05-09).
///
/// LIVE-SAFETY: a STALE adapter address is a documented failure mode (the relayer
/// rejects the batch). This const is pinned by the test below, but a green test
/// only proves the ENCODED address — the relayer submit → `STATE_CONFIRMED`
/// round-trip is validated for real only at the operator's first FUNDED STAGING
/// run (design §5/§8) before any prod use. The relayer is OFF by default.
pub const CTF_COLLATERAL_ADAPTER: Address =
    address!("0xAdA100Db00Ca00073811820692005400218FcE1f");

/// pUSD-native `NegRiskCtfCollateralAdapter` (Polygon 137) — the merge/redeem
/// TARGET for NEGATIVE-RISK (multi-outcome) markets (design §2.3). Same
/// confirmation sources + live-safety caveat as [`CTF_COLLATERAL_ADAPTER`];
/// selecting standard-vs-NegRisk per market is a later task.
pub const NEGRISK_CTF_COLLATERAL_ADAPTER: Address =
    address!("0xadA2005600Dec949baf300f4C6120000bDB6eAab");

// ---------------------------------------------------------------------------
// EIP-712 deposit-wallet `Batch` signing (M6-2 — THE golden-vector gate)
// ---------------------------------------------------------------------------

sol! {
    /// One call inside a deposit-wallet batch. Field NAMES, ORDER, and TYPES
    /// are the EIP-712 typestring — never reorder or rename. `value` is
    /// `uint256` (NOT u64) and `data` is dynamic `bytes` (NOT a fixed array);
    /// the golden vector enforces this.
    #[derive(Debug)]
    struct Call {
        address target;
        uint256 value;
        bytes data;
    }

    /// The deposit-wallet batch the owner EOA signs (primaryType `Batch`).
    /// `calls` is a struct array — EIP-712 encodes it as the keccak of the
    /// concatenated `Call` member hashes. Matches
    /// `py-builder-relayer-client@e7108cd` `builder/deposit_wallet.py`
    /// (design §2.1); field order `wallet,nonce,deadline,calls` is load-bearing.
    #[derive(Debug)]
    struct Batch {
        address wallet;
        uint256 nonce;
        uint256 deadline;
        Call[] calls;
    }
}

#[derive(Debug)]
pub enum RelayerError {
    /// The owner EOA private key string could not be parsed into a signer.
    BadKey(String),
    /// The local signer failed to produce a signature.
    Sign(String),
}

impl std::fmt::Display for RelayerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RelayerError::BadKey(e) => write!(f, "invalid signer key: {e}"),
            RelayerError::Sign(e) => write!(f, "batch signing error: {e}"),
        }
    }
}

impl std::error::Error for RelayerError {}

/// Sign a deposit-wallet `Batch` (EIP-712) with the owner EOA `pk`, returning
/// the 65-byte `r || s || v` signature as a `0x`-prefixed hex string (132
/// chars). This is the offline-validatable core of M6 — proven byte-identical
/// to Polymarket's golden vector by `sign_batch_matches_polymarket_golden_vector`.
///
/// The EIP-712 domain is `{ name: "DepositWallet", version: "1", chainId,
/// verifyingContract: <wallet> }` — the verifying contract is the DEPOSIT
/// WALLET itself (NOT the factory). `v` is encoded as 27/28 (Electrum
/// notation, via `Signature::as_bytes`), matching the eth_account convention
/// that produced the reference vector — IDENTICAL to `sign::sign_order` and
/// `auth::l1_signature`.
pub fn sign_batch(
    pk: &str,
    chain_id: u64,
    wallet: Address,
    nonce: u64,
    deadline: u64,
    calls: &[Call],
) -> Result<String, RelayerError> {
    let signer = pk
        .parse::<PrivateKeySigner>()
        .map_err(|e| RelayerError::BadKey(e.to_string()))?;
    let domain = eip712_domain! {
        name: "DepositWallet",
        version: "1",
        chain_id: chain_id,
        verifying_contract: wallet,
    };
    let batch = Batch {
        wallet,
        nonce: U256::from(nonce),
        deadline: U256::from(deadline),
        calls: calls.to_vec(),
    };
    let hash = batch.eip712_signing_hash(&domain);
    let sig = signer
        .sign_hash_sync(&hash)
        .map_err(|e| RelayerError::Sign(e.to_string()))?;
    Ok(format!("0x{}", hex::encode(sig.as_bytes())))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    /// THE M6 GATE (M6-2): reproduce Polymarket's deposit-wallet `Batch` golden
    /// signature byte-for-byte. Fixture from `py-builder-relayer-client@e7108cd`
    /// `tests/builder/test_deposit_wallet.py` (chain 137, the public anvil key).
    /// A mismatch means the EIP-712 typed-data encoding is wrong — do not weaken
    /// this test; debug the encoding until it is byte-exact.
    #[test]
    fn sign_batch_matches_polymarket_golden_vector() {
        use alloy_primitives::{Address, U256};
        let pk = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"; // public anvil key
        let wallet: Address = "0xa2927E7834648F1C03b4961CeeA4597292e3c025".parse().unwrap();
        let token: Address = "0x0000000000000000000000000000000000000001".parse().unwrap();
        let data = alloy_primitives::hex::decode(
            "095ea7b30000000000000000000000000000000000000000000000000000000000000002ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        ).unwrap();
        let calls = vec![Call { target: token, value: U256::ZERO, data: data.into() }];
        let sig = sign_batch(pk, 137u64, wallet, 0u64, 1234567890u64, &calls).unwrap();
        assert_eq!(
            sig,
            "0x7827946c566e7860f6c5f2e641587ed6928989c8618e463a00dd56832e7300023b7436c67a2ea82d6d506b1a5eda3e27526e9e2ffaad52128d75c47c2e9d1fac1b"
        );
        assert_eq!(sig.len(), 132);
    }

    /// LIVE GATE (M6-B): the collateral-adapter consts MUST be real, non-zero
    /// Polygon-137 addresses before any live merge/redeem batch is built — a
    /// zero/wrong adapter would silently send a real on-chain batch to the wrong
    /// target. We pin the exact EIP-55-checksummed mainnet addresses: the
    /// `to_checksum` round-trip RE-DERIVES the checksum from the const's own
    /// bytes, so a typo'd const (or a non-canonical literal) fails this test.
    ///
    /// Reminder: green here proves the ENCODED address only; the relayer
    /// submit → `STATE_CONFIRMED` round-trip is validated at the operator's
    /// first funded STAGING run (design §5/§8).
    #[test]
    fn adapter_address_must_be_set_before_live() {
        assert_ne!(
            CTF_COLLATERAL_ADAPTER,
            Address::ZERO,
            "CTF_COLLATERAL_ADAPTER is unset (zero) — refuse to go live (M6-B)"
        );
        assert_eq!(
            CTF_COLLATERAL_ADAPTER.to_checksum(None),
            "0xAdA100Db00Ca00073811820692005400218FcE1f",
            "CTF_COLLATERAL_ADAPTER must equal the confirmed Polygon-137 adapter"
        );

        assert_ne!(
            NEGRISK_CTF_COLLATERAL_ADAPTER,
            Address::ZERO,
            "NEGRISK_CTF_COLLATERAL_ADAPTER is unset (zero) — refuse to go live (M6-B)"
        );
        assert_eq!(
            NEGRISK_CTF_COLLATERAL_ADAPTER.to_checksum(None),
            "0xadA2005600Dec949baf300f4C6120000bDB6eAab",
            "NEGRISK_CTF_COLLATERAL_ADAPTER must equal the confirmed Polygon-137 adapter"
        );
    }

    /// The factory/impl are the pinned reference-client values (design §2.3).
    /// Pin them too so a careless edit is caught; they are distinct and non-zero.
    #[test]
    fn deposit_wallet_contracts_are_pinned() {
        assert_eq!(
            DEPOSIT_WALLET_FACTORY.to_checksum(None),
            "0x894Ee6B254f251518206f709E9B115f214ebDf17"
        );
        assert_eq!(
            DEPOSIT_WALLET_IMPL.to_checksum(None),
            "0x55913A0bdecCbB77b7Af781A48300e6394B5EEAE"
        );
        assert_ne!(DEPOSIT_WALLET_FACTORY, Address::ZERO);
        assert_ne!(DEPOSIT_WALLET_IMPL, Address::ZERO);
        assert_ne!(DEPOSIT_WALLET_FACTORY, DEPOSIT_WALLET_IMPL);
    }
}
