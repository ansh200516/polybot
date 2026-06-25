//! Polymarket deposit-wallet RELAYER client (M6): live merge/redeem via the
//! relayer WALLET batch. See
//! docs/superpowers/specs/2026-06-25-m6-deposit-wallet-relayer-design.md
//!
//! M6-1 lays only the Polygon-137 contract constants + (Step 2) the builder
//! credentials used by the later tasks (EIP-712 `Batch` signing, merge/redeem
//! calldata, the relayer I/O). NO signing, ABI encoding, or network I/O happens
//! here yet — this is foundational scaffolding only.

use alloy_primitives::{Address, address};

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

#[cfg(test)]
mod tests {
    use super::*;

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
