//! Polymarket deposit-wallet RELAYER client (M6): live merge/redeem via the
//! relayer WALLET batch. See
//! docs/superpowers/specs/2026-06-25-m6-deposit-wallet-relayer-design.md
//!
//! M6-1 lays only the Polygon-137 contract constants + (Step 2) the builder
//! credentials used by the later tasks (EIP-712 `Batch` signing, merge/redeem
//! calldata, the relayer I/O). NO signing, ABI encoding, or network I/O happens
//! here yet — this is foundational scaffolding only.

use alloy_primitives::{Address, B256, U256, address, b256, hex, keccak256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolCall, SolStruct, eip712_domain, sol};

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

// ---------------------------------------------------------------------------
// (A) CtfCollateralAdapter merge/redeem CALLDATA (M6-3, design §2.3)
// ---------------------------------------------------------------------------

sol! {
    /// `CtfCollateralAdapter.mergePositions` — pulls the deposit wallet's
    /// YES+NO ERC-1155 legs, merges the complete set via the underlying CTF,
    /// and returns `amount` of `collateralToken`. The function NAME, ARG
    /// ORDER, and TYPES form the 4-byte selector preimage
    /// (`mergePositions(address,bytes32,bytes32,uint256[],uint256)`) — never
    /// reorder or rename (design §2.3).
    function mergePositions(
        address collateralToken,
        bytes32 parentCollectionId,
        bytes32 conditionId,
        uint256[] partition,
        uint256 amount
    );

    /// `CtfCollateralAdapter.redeemPositions` — redeems a RESOLVED position
    /// back to `collateralToken`. `indexSets` selects the outcome slots
    /// (binary market: `[1, 2]`). Selector preimage
    /// `redeemPositions(address,bytes32,bytes32,uint256[])`.
    function redeemPositions(
        address collateralToken,
        bytes32 parentCollectionId,
        bytes32 conditionId,
        uint256[] indexSets
    );
}

/// Build the WALLET-batch [`Call`] that merges a complete YES+NO set back to
/// `collateral` on `adapter` (the standard or NegRisk `CtfCollateralAdapter`).
///
/// `partition = [1, 2]` (the two binary outcome slots) and `parentCollectionId
/// = 0x0` (top-level condition) are fixed by the protocol; `amount` is the set
/// count in `collateral` base units. `value` is `0` — no native token moves.
/// `collateral`/`adapter` are PARAMETERS (the exact pUSD/USDC.e collateral is
/// reconciled in M6-6); nothing is hardcoded here.
pub fn merge_call(adapter: Address, collateral: Address, condition_id: B256, amount: U256) -> Call {
    let data = mergePositionsCall {
        collateralToken: collateral,
        parentCollectionId: B256::ZERO,
        conditionId: condition_id,
        partition: vec![U256::from(1), U256::from(2)],
        amount,
    }
    .abi_encode();
    Call {
        target: adapter,
        value: U256::ZERO,
        data: data.into(),
    }
}

/// Build the WALLET-batch [`Call`] that redeems a RESOLVED position back to
/// `collateral` on `adapter`. `indexSets = [1, 2]` and `parentCollectionId =
/// 0x0` are fixed by the protocol (binary market); `value` is `0`.
pub fn redeem_call(adapter: Address, collateral: Address, condition_id: B256) -> Call {
    let data = redeemPositionsCall {
        collateralToken: collateral,
        parentCollectionId: B256::ZERO,
        conditionId: condition_id,
        indexSets: vec![U256::from(1), U256::from(2)],
    }
    .abi_encode();
    Call {
        target: adapter,
        value: U256::ZERO,
        data: data.into(),
    }
}

// ---------------------------------------------------------------------------
// (B) Deposit-wallet address derivation (M6-3) — CREATE2 over the ERC-1967 clone
// ---------------------------------------------------------------------------
// Ported 1:1 from `py-builder-relayer-client` `builder/derive.py`; proven
// byte-exact against its golden vector (`derive_deposit_wallet_matches_golden_vector`).

/// Low 10 bytes of the ERC-1967 clone init-code, as a big-endian integer.
/// The deploy-time `(len(args) << 56)` is folded in by ADDITION (matching the
/// Python `ERC1967_PREFIX + (n << 56)`), then the low 10 bytes are emitted.
const ERC1967_PREFIX: u128 = 0x61003D3D8160233D3973;

/// Tail constants of the ERC-1967 minimal-clone init-code (Solady-style).
/// `CONST2` is emitted BEFORE `CONST1` (load-bearing order, per the Python).
const ERC1967_CONST1: B256 =
    b256!("0xcc3735a920a3ca505d382bbc545af43d6000803e6038573d6000fd5b3d6000f3");
const ERC1967_CONST2: B256 =
    b256!("0x5155f3363d3d373d3d363d7f360894a13ba1a3210667c828492db98dca3e2076");

/// `keccak256` of the ERC-1967 minimal-clone init-code for `implementation`
/// with trailing immutable `args`. Layout (port of `init_code_hash_erc1967`):
/// `prefix(10) ‖ implementation(20) ‖ 0x6009 ‖ CONST2(32) ‖ CONST1(32) ‖ args`.
fn init_code_hash_erc1967(implementation: Address, args: &[u8]) -> B256 {
    let n = args.len() as u128;
    let combined = (ERC1967_PREFIX + (n << 56)).to_be_bytes();
    let mut init_code = Vec::with_capacity(10 + 20 + 2 + 32 + 32 + args.len());
    init_code.extend_from_slice(&combined[6..16]); // low 10 bytes, big-endian
    init_code.extend_from_slice(implementation.as_slice());
    init_code.extend_from_slice(&[0x60, 0x09]);
    init_code.extend_from_slice(ERC1967_CONST2.as_slice());
    init_code.extend_from_slice(ERC1967_CONST1.as_slice());
    init_code.extend_from_slice(args);
    keccak256(init_code)
}

/// EIP-1014 CREATE2 address: `keccak256(0xff ‖ from ‖ salt ‖ bytecode_hash)[12:]`.
/// Port of `get_create2_address`.
fn get_create2_address(bytecode_hash: B256, from_address: Address, salt: B256) -> Address {
    let mut buf = [0u8; 85];
    buf[0] = 0xff;
    buf[1..21].copy_from_slice(from_address.as_slice());
    buf[21..53].copy_from_slice(salt.as_slice());
    buf[53..85].copy_from_slice(bytecode_hash.as_slice());
    Address::from_word(keccak256(buf))
}

/// Derive the deterministic deposit-wallet address for `owner` under the
/// relayer's `factory`/`implementation` (CREATE2 over the ERC-1967 minimal
/// clone). Port of `derive_deposit_wallet`:
/// - `wallet_id = keccak256(owner)` (keccak of the 20-byte owner address),
/// - `args = abi_encode(address factory, bytes32 wallet_id)` (64 bytes),
/// - `salt = keccak256(args)`,
/// - `bytecode_hash = init_code_hash_erc1967(implementation, args)`,
/// - return `CREATE2(from = factory, salt, bytecode_hash)`.
///
/// The returned [`Address`] compares byte-for-byte; render with `to_checksum`
/// for the EIP-55 form. Proven byte-exact by the golden-vector test.
pub fn derive_deposit_wallet(owner: Address, factory: Address, implementation: Address) -> Address {
    let wallet_id = keccak256(owner.as_slice());

    // args = ABI encoding of (address factory, bytes32 wallet_id): the address
    // is right-aligned in the first 32-byte word; wallet_id fills the second.
    let mut args = [0u8; 64];
    args[12..32].copy_from_slice(factory.as_slice());
    args[32..64].copy_from_slice(wallet_id.as_slice());

    let salt = keccak256(args);
    let bytecode_hash = init_code_hash_erc1967(implementation, &args);
    get_create2_address(bytecode_hash, factory, salt)
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

    /// (A) `merge_call` must produce the exact `CtfCollateralAdapter.
    /// mergePositions` calldata: a hand-computed keccak selector over the
    /// canonical signature, the protocol-fixed `parentCollectionId = 0x0` /
    /// `partition = [1, 2]`, and a clean round-trip of every arg via the
    /// generated decoder. `target` is the adapter; `value` is 0.
    #[test]
    fn merge_call_selector_and_args() {
        let adapter = CTF_COLLATERAL_ADAPTER;
        // Arbitrary non-zero collateral (USDC.e on 137); the real pUSD/USDC.e
        // address is reconciled in M6-6 — this test only pins the ENCODING.
        let collateral: Address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".parse().unwrap();
        let condition_id: B256 =
            "0xabcdef0000000000000000000000000000000000000000000000000000000123".parse().unwrap();
        let amount = U256::from(1_500_000u64);

        let call = merge_call(adapter, collateral, condition_id, amount);

        assert_eq!(call.target, adapter, "merge target must be the adapter");
        assert_eq!(call.value, U256::ZERO, "merge value must be 0");

        // Selector = keccak256(canonical signature)[..4], hand-computed.
        let selector = keccak256("mergePositions(address,bytes32,bytes32,uint256[],uint256)".as_bytes());
        assert_eq!(&call.data[..4], &selector[..4], "mergePositions selector mismatch");

        // Args round-trip through the generated decoder (also re-validates the
        // 4-byte selector, since `abi_decode` strips and checks it).
        let decoded = mergePositionsCall::abi_decode(&call.data).unwrap();
        assert_eq!(decoded.collateralToken, collateral);
        assert_eq!(decoded.parentCollectionId, B256::ZERO);
        assert_eq!(decoded.conditionId, condition_id);
        assert_eq!(decoded.partition, vec![U256::from(1), U256::from(2)]);
        assert_eq!(decoded.amount, amount);
    }

    /// (A) `redeem_call` mirror of [`merge_call_selector_and_args`] for
    /// `redeemPositions(address,bytes32,bytes32,uint256[])` with `indexSets =
    /// [1, 2]`. Uses the NegRisk adapter to prove `adapter` is a parameter.
    #[test]
    fn redeem_call_selector_and_args() {
        let adapter = NEGRISK_CTF_COLLATERAL_ADAPTER;
        let collateral: Address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".parse().unwrap();
        let condition_id: B256 =
            "0x00000000000000000000000000000000000000000000000000000000deadbeef".parse().unwrap();

        let call = redeem_call(adapter, collateral, condition_id);

        assert_eq!(call.target, adapter, "redeem target must be the adapter");
        assert_eq!(call.value, U256::ZERO, "redeem value must be 0");

        let selector = keccak256("redeemPositions(address,bytes32,bytes32,uint256[])".as_bytes());
        assert_eq!(&call.data[..4], &selector[..4], "redeemPositions selector mismatch");

        let decoded = redeemPositionsCall::abi_decode(&call.data).unwrap();
        assert_eq!(decoded.collateralToken, collateral);
        assert_eq!(decoded.parentCollectionId, B256::ZERO);
        assert_eq!(decoded.conditionId, condition_id);
        assert_eq!(decoded.indexSets, vec![U256::from(1), U256::from(2)]);
    }

    /// (B) THE M6-3 derivation GATE: reproduce Polymarket's deposit-wallet
    /// CREATE2/ERC-1967 golden vector byte-for-byte (from py
    /// `tests/builder/test_derive.py`). The vector uses the Amoy factory/impl,
    /// which is fine — it validates the ALGORITHM (keccak owner → wallet_id →
    /// abi-encoded args → salt + ERC-1967 init-code hash → CREATE2). A mismatch
    /// means the derivation is wrong; debug until byte-exact, do not weaken.
    #[test]
    fn derive_deposit_wallet_matches_golden_vector() {
        let owner: Address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".parse().unwrap();
        let factory: Address = "0x801c740Bcd28531d75a5da176D5511F3329Ab049".parse().unwrap();
        let implementation: Address = "0x24f3257BF9451bA575E864777ab6f8D7Eac0139B".parse().unwrap();
        let wallet = derive_deposit_wallet(owner, factory, implementation);
        assert_eq!(
            wallet,
            "0x63cB1B4eC2F274Ed553aD5079c6A2542d1c02bd7".parse::<Address>().unwrap(),
            "deposit-wallet derivation diverged from the Polymarket golden vector"
        );
    }
}
