//! EIP-712 signing of Polymarket CLOB **V2** orders (RECON-M5-V2, 2026-06-13).
//! Pure: no I/O. Constants are RECON-pinned — a mismatch with docs/RECON-M5-V2.md
//! is a stop-and-fix, not a local edit.
//!
//! V2 (vs V1): the signed struct drops `taker, expiration, nonce, feeRateBps`
//! and adds `timestamp, metadata, builder`; the domain version is "2" (name
//! unchanged); the verifying contracts are the V2 exchange addresses below.

use alloy_primitives::{Address, B256, U256, hex};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{Eip712Domain, SolStruct, eip712_domain, sol};

use pm_core::num::{Px, Qty, TickSize, buy_cost, sell_proceeds};
use pm_engine::Action;

/// RECON-pinned (RECON-M5-V2.md). CHAIN_ID stays 137 (Polygon mainnet); the
/// exchange addresses are the V2 contracts.
pub const CHAIN_ID: u64 = 137;
pub const CTF_EXCHANGE: &str = "0xE111180000d2663C0091e4f400237545B87B996B";
pub const NEG_RISK_CTF_EXCHANGE: &str = "0xe2222d279d744050d28e00520010520000310F59";

sol! {
    /// Field NAMES, ORDER, and TYPES are the EIP-712 typestring — never reorder
    /// or rename. This is the V2 struct (RECON-M5-V2).
    struct Order {
        uint256 salt;
        address maker;
        address signer;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint8 side;
        uint8 signatureType;
        uint256 timestamp;
        bytes32 metadata;
        bytes32 builder;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    /// EIP-712 encoding: BUY = 0, SELL = 1 (RECON-M5 order struct).
    const fn as_u8(self) -> u8 {
        match self {
            Side::Buy => 0,
            Side::Sell => 1,
        }
    }
}

/// A CLOB V2 order ready for signing/serialisation. Amounts are 6-decimal
/// integers (µ units); `token_id` is the venue's decimal string.
#[derive(Debug, Clone)]
pub struct ClobOrder {
    pub salt: u64,
    pub maker: Address,  // proxy wallet (signature_type 1)
    pub signer: Address, // EOA exported from Magic
    pub token_id: String,
    pub maker_amount: u64,
    pub taker_amount: u64,
    pub side: Side,
    pub signature_type: u8, // 1 = POLY_PROXY (prod), 3 = POLY_1271 (vector)
    pub timestamp: u64,     // unix MILLISECONDS (V2)
    pub metadata: B256,     // bytes32; zero by default (V2)
    pub builder: B256,      // bytes32; zero = no builder fee (V2)
}

#[derive(Debug)]
pub enum SignError {
    /// `token_id` was not a valid uint256 decimal string.
    BadTokenId(String),
    /// The local signer failed to produce a signature.
    Signer(String),
}

impl std::fmt::Display for SignError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SignError::BadTokenId(t) => write!(f, "invalid token id: {t}"),
            SignError::Signer(e) => write!(f, "signer error: {e}"),
        }
    }
}

impl std::error::Error for SignError {}

/// EIP-712 domain for the CLOB **V2** exchange (regular or neg-risk variant).
///
/// `chain_id` is a parameter so the offline reference vector (AMOY, 80002) can
/// be reproduced; production callers pass [`CHAIN_ID`] (137). Version is "2"
/// (V2); the name is unchanged.
///
/// The verifying contract is a RECON-pinned compile-time constant; parsing it
/// cannot fail at runtime. We use a `match` rather than `unwrap` (the crate
/// denies it) — the `Err` arm is unreachable for these constants, and falling
/// back to `Address::ZERO` would yield a domain separator that no operator key
/// could ever match, so a constant typo surfaces immediately as a signing-hash
/// mismatch against the vectors rather than a panic.
fn domain(neg_risk: bool, chain_id: u64) -> Eip712Domain {
    let contract = if neg_risk {
        NEG_RISK_CTF_EXCHANGE
    } else {
        CTF_EXCHANGE
    };
    let verifying_contract = match contract.parse::<Address>() {
        Ok(addr) => addr,
        Err(_) => Address::ZERO,
    };
    eip712_domain! {
        name: "Polymarket CTF Exchange",
        version: "2",
        chain_id: chain_id,
        verifying_contract: verifying_contract,
    }
}

/// Sign `order` for the given exchange variant on Polygon mainnet
/// ([`CHAIN_ID`]), returning the 65-byte `r || s || v` signature as a
/// `0x`-prefixed hex string.
///
/// `v` is encoded as 27/28 (Electrum notation, via `Signature::as_bytes`),
/// matching the eth_account convention that produced the reference vectors.
pub fn sign_order(
    signer: &PrivateKeySigner,
    order: &ClobOrder,
    neg_risk: bool,
) -> Result<String, SignError> {
    sign_order_with_chain(signer, order, neg_risk, CHAIN_ID)
}

/// As [`sign_order`] but for an explicit `chain_id` — used to reproduce the
/// AMOY (80002) reference vector offline. Production signs on [`CHAIN_ID`].
pub fn sign_order_with_chain(
    signer: &PrivateKeySigner,
    order: &ClobOrder,
    neg_risk: bool,
    chain_id: u64,
) -> Result<String, SignError> {
    let token_id =
        U256::from_str_radix(&order.token_id, 10).map_err(|_| SignError::BadTokenId(order.token_id.clone()))?;

    let sol_order = Order {
        salt: U256::from(order.salt),
        maker: order.maker,
        signer: order.signer,
        tokenId: token_id,
        makerAmount: U256::from(order.maker_amount),
        takerAmount: U256::from(order.taker_amount),
        side: order.side.as_u8(),
        signatureType: order.signature_type,
        timestamp: U256::from(order.timestamp),
        metadata: order.metadata,
        builder: order.builder,
    };

    let hash = sol_order.eip712_signing_hash(&domain(neg_risk, chain_id));
    let sig = signer
        .sign_hash_sync(&hash)
        .map_err(|e| SignError::Signer(e.to_string()))?;
    Ok(format!("0x{}", hex::encode(sig.as_bytes())))
}

/// The wallet (account) EIP-712 domain for a deposit wallet, used as the inner
/// domain of the ERC-7739 `TypedDataSign` envelope (POLY_1271). Name
/// "DepositWallet", version "1", `verifyingContract` = the deposit wallet, salt
/// 0x0 — RECON-pinned (RECON-M5-V2-1271.md, confirmed vs Solady ERC1271.sol).
///
/// `pub(crate)` so the auth-side ClobAuth L1 wrap (which binds the API key to
/// the deposit wallet, RECON-M5-V2-1271 "Auth binding") reuses the SAME wallet
/// domain as orders — the deposit-wallet account validates both via one
/// ERC-1271 implementation.
pub(crate) fn wallet_domain(chain_id: u64, deposit_wallet: Address) -> Eip712Domain {
    eip712_domain! {
        name: "DepositWallet",
        version: "1",
        chain_id: chain_id,
        verifying_contract: deposit_wallet,
        salt: B256::ZERO,
    }
}

/// Generic Solady ERC-7739 `TypedDataSign` wrap (POLY_1271). The order path and
/// the auth-side ClobAuth L1 path share this; they differ ONLY in the app
/// domain and the wrapped `contents` struct.
///
/// Inputs:
/// - `app_domain_separator` = the *app's* EIP-712 domain separator
///   (`domain.hash_struct()`): the Exchange V2 domain for orders, the
///   ClobAuthDomain for L1 auth.
/// - `contents_hash` = the contents struct's `eip712_hash_struct()`.
/// - `contents_type` = the contents struct's EIP-712 type string (e.g. the
///   `Order(...)` or `ClobAuth(...)` typestring). Its leading token up to the
///   first `(` is `contents_name` (Solady implicit mode).
/// - `wallet_domain` = the DepositWallet account domain.
///
/// Produces the nested digest `keccak256(0x1901 ‖ appDomainSeparator ‖
/// hashStruct(TypedDataSign))`, signs it with the EOA, and assembles the wire
/// `innerSig(65) ‖ appDomainSeparator(32) ‖ contentsHash(32) ‖ contentsType ‖
/// uint16_be(len(contentsType))`. RECON-M5-V2-1271 "Pinned algorithm".
pub(crate) fn erc7739_wrap(
    signer: &PrivateKeySigner,
    app_domain_separator: B256,
    contents_hash: B256,
    contents_type: &str,
    wallet_domain: &Eip712Domain,
) -> Result<String, SignError> {
    use alloy_primitives::keccak256;

    // contentsName = contentsType up to its first '(' (Solady implicit mode).
    let contents_name = contents_type
        .split_once('(')
        .map_or(contents_type, |(n, _)| n);

    // typedDataSignTypehash = keccak256("TypedDataSign(" + contentsName +
    // " contents,string name,string version,uint256 chainId,address
    // verifyingContract,bytes32 salt)" + contentsType). Domain fields are
    // ALWAYS name,version,chainId,verifyingContract,salt (5 fields, no
    // extensions) — RECON-M5-V2-1271, confirmed vs Solady ERC1271.sol.
    let mut type_preimage = String::with_capacity(64 + contents_name.len() + contents_type.len());
    type_preimage.push_str("TypedDataSign(");
    type_preimage.push_str(contents_name);
    type_preimage.push_str(
        " contents,string name,string version,uint256 chainId,address verifyingContract,bytes32 salt)",
    );
    type_preimage.push_str(contents_type);
    let typed_data_sign_typehash = keccak256(type_preimage.as_bytes());

    // hashStruct(TypedDataSign): 7 × 32-byte words. `contents` BEFORE `name`
    // (order is load-bearing). Wallet-domain fields.
    let chain_id = wallet_domain.chain_id.map_or(0, |c| c.to::<u64>());
    let verifying_contract = wallet_domain.verifying_contract.unwrap_or(Address::ZERO);
    let salt = wallet_domain.salt.unwrap_or(B256::ZERO);
    let mut buf = [0u8; 7 * 32];
    buf[0..32].copy_from_slice(typed_data_sign_typehash.as_slice());
    buf[32..64].copy_from_slice(contents_hash.as_slice());
    buf[64..96]
        .copy_from_slice(keccak256(wallet_domain.name.as_deref().unwrap_or("").as_bytes()).as_slice());
    buf[96..128].copy_from_slice(
        keccak256(wallet_domain.version.as_deref().unwrap_or("").as_bytes()).as_slice(),
    );
    buf[128..160].copy_from_slice(&U256::from(chain_id).to_be_bytes::<32>());
    // address left-padded to 32 bytes (the low 20 bytes are the address).
    buf[172..192].copy_from_slice(verifying_contract.as_slice());
    buf[192..224].copy_from_slice(salt.as_slice());
    let hash_struct = keccak256(buf);

    // Nested digest = keccak256(0x1901 ‖ appDomainSeparator ‖ hashStruct).
    let mut digest_buf = [0u8; 2 + 32 + 32];
    digest_buf[0] = 0x19;
    digest_buf[1] = 0x01;
    digest_buf[2..34].copy_from_slice(app_domain_separator.as_slice());
    digest_buf[34..66].copy_from_slice(hash_struct.as_slice());
    let nested_digest = keccak256(digest_buf);

    // EOA signs the nested digest (ECDSA, v = 27/28 via as_bytes).
    let sig = signer
        .sign_hash_sync(&nested_digest)
        .map_err(|e| SignError::Signer(e.to_string()))?;
    let inner = sig.as_bytes(); // 65 bytes r‖s‖v

    // Wrap: innerSig(65) ‖ appDomainSeparator(32) ‖ contentsHash(32) ‖
    // contentsType(ascii) ‖ uint16_be(len).
    let ct = contents_type.as_bytes();
    let mut wire = Vec::with_capacity(65 + 32 + 32 + ct.len() + 2);
    wire.extend_from_slice(&inner);
    wire.extend_from_slice(app_domain_separator.as_slice());
    wire.extend_from_slice(contents_hash.as_slice());
    wire.extend_from_slice(ct);
    wire.extend_from_slice(&(ct.len() as u16).to_be_bytes());

    Ok(format!("0x{}", hex::encode(wire)))
}

/// The V2 Order EIP-712 type string (`contentsType`), 186 bytes. This is the
/// SAME string the `sol! Order` derives; we pin it as a constant so the
/// ERC-7739 typehash and the wire `contentsType` are byte-exact, and assert
/// equality with alloy's derived type string in tests.
const ORDER_CONTENTS_TYPE: &str = "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";

/// ERC-7739 wrapped signature for a deposit-wallet (POLY_1271, sigType 3) order.
/// `deposit_wallet` is the order maker AND the wallet-domain verifyingContract.
///
/// Implements the Solady `_erc1271IsValidSignatureViaNestedEIP712` scheme
/// (RECON-M5-V2-1271.md "Pinned algorithm"):
/// - app domain  = the Exchange V2 domain ([`domain`]); its separator is
///   `appDomainSeparator`.
/// - wallet domain = [`wallet_domain`] (DepositWallet/1/chainId/wallet/salt0).
/// - contentsHash = the Order struct hash (`Order::eip712_hash_struct`).
/// - typedDataSignTypehash = keccak256("TypedDataSign(Order contents,string
///   name,string version,uint256 chainId,address verifyingContract,bytes32
///   salt)" + contentsType).
/// - hashStruct = keccak256(typehash ‖ contentsHash ‖ keccak(name) ‖
///   keccak(version) ‖ chainId ‖ verifyingContract ‖ salt)  (7 words; the
///   `contents`-before-`name` order is load-bearing).
/// - nested digest the EOA signs = keccak256(0x1901 ‖ appDomainSeparator ‖
///   hashStruct).
/// - wire = innerSig(65) ‖ appDomainSeparator(32) ‖ contentsHash(32) ‖
///   contentsType(ascii) ‖ uint16_be(len(contentsType)).
///
/// The plain POLY_PROXY ([`sign_order`], sigType 1) path is unaffected.
pub fn sign_order_1271(
    signer: &PrivateKeySigner,
    order: &ClobOrder,
    neg_risk: bool,
    chain_id: u64,
    deposit_wallet: Address,
) -> Result<String, SignError> {
    let token_id =
        U256::from_str_radix(&order.token_id, 10).map_err(|_| SignError::BadTokenId(order.token_id.clone()))?;

    let sol_order = Order {
        salt: U256::from(order.salt),
        maker: order.maker,
        signer: order.signer,
        tokenId: token_id,
        makerAmount: U256::from(order.maker_amount),
        takerAmount: U256::from(order.taker_amount),
        side: order.side.as_u8(),
        signatureType: order.signature_type,
        timestamp: U256::from(order.timestamp),
        metadata: order.metadata,
        builder: order.builder,
    };

    // App domain = the exchange V2 domain; contents = the Order struct. The
    // nesting/wrapping is the generic ERC-7739 scheme (shared with the auth-side
    // ClobAuth L1 wrap). contentsName "Order" is derived inside the helper.
    let app_domain_separator = domain(neg_risk, chain_id).hash_struct();
    let contents_hash = sol_order.eip712_hash_struct();
    erc7739_wrap(
        signer,
        app_domain_separator,
        contents_hash,
        ORDER_CONTENTS_TYPE,
        &wallet_domain(chain_id, deposit_wallet),
    )
}

/// (makerAmount, takerAmount) for a leg, µ units, against-us rounding —
/// matches the engine's own cash math AND the reference client (vectors).
pub fn clob_amounts(action: Action, ts: TickSize, limit_px: Px, qty: Qty) -> (u64, u64) {
    let px_micro = limit_px.microusdc(ts);
    // Cast safety: cost/proceeds = px_micro·qty/1e6 with px_micro < 1e6
    // (px ≤ $1 by tick construction), so the i128 result is non-negative and
    // ≤ qty.0 ≤ u64::MAX — the i128→u64 cast cannot wrap.
    match action {
        // BUY: pay µUSDC (rounded UP, against us), receive µshares.
        Action::Buy => (buy_cost(px_micro, qty).0 as u64, qty.0),
        // SELL: give µshares, receive µUSDC (rounded DOWN, against us).
        Action::Sell => (qty.0, sell_proceeds(px_micro, qty).0 as u64),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use alloy_signer_local::PrivateKeySigner;

    /// Parse a `0x`-prefixed 64-hex string into the `bytes32` type.
    fn parse_b256(s: &str) -> B256 {
        s.parse().unwrap()
    }

    /// Build the sol `Order` from the V2 fixture's `order` object.
    fn order_from_fixture(o: &serde_json::Value) -> Order {
        Order {
            salt: U256::from(o["salt"].as_u64().unwrap()),
            maker: o["maker"].as_str().unwrap().parse().unwrap(),
            signer: o["signer"].as_str().unwrap().parse().unwrap(),
            tokenId: U256::from_str_radix(o["tokenId"].as_str().unwrap(), 10).unwrap(),
            makerAmount: U256::from(o["makerAmount"].as_u64().unwrap()),
            takerAmount: U256::from(o["takerAmount"].as_u64().unwrap()),
            side: 0, // fixture side is "BUY"
            signatureType: o["signatureType"].as_u64().unwrap() as u8,
            timestamp: U256::from(o["timestamp"].as_u64().unwrap()),
            metadata: parse_b256(o["metadata"].as_str().unwrap()),
            builder: parse_b256(o["builder"].as_str().unwrap()),
        }
    }

    /// Validate the V2 EIP-712 **struct + domain hashing** against the
    /// py-clob-client-v2 reference vector (RECON-M5-V2), byte-for-byte.
    ///
    /// WHY NOT compare the leading 65 ECDSA bytes: the reference vector is a
    /// **POLY_1271 (sigType 3)** signature, which py-clob-client-v2 builds with
    /// Solady's EIP-7739 `TypedDataSign` nesting. Its on-the-wire layout is
    ///   `innerSig(65) ‖ appDomainSeparator(32) ‖ contentsHash(32) ‖ contentsType ‖ len`,
    /// and the inner 65 bytes sign the *nested* digest
    /// `keccak256(0x1901 ‖ appDomainSeparator ‖ typedDataSignStructHash)` —
    /// NOT the order's own EIP-712 hash. So the leading 65 bytes are NOT
    /// reproducible from the plain order hash (our production POLY_PROXY /
    /// sigType-1 path signs the plain order hash via `sign_order`).
    ///
    /// But the wrapper *embeds* the two values that pin the order's struct and
    /// domain: bytes [65..97) are the exchange domain separator and bytes
    /// [97..129) are the order struct (contents) hash. We assert OUR computed
    /// domain separator and struct hash equal those embedded bytes — a strict,
    /// byte-exact validation of the V2 struct typestring, field order/types,
    /// domain name, version "2", chainId, and verifying contract address. The
    /// production 65-byte r‖s‖v signature itself is exercised by the live.rs /
    /// auth.rs suites (sigType 1).
    #[test]
    fn reproduces_v2_reference_vector() {
        let raw = include_str!("../tests/fixtures/sign_vectors_v2.json");
        let v: serde_json::Value = serde_json::from_str(raw).unwrap();
        let order = order_from_fixture(&v["order"]);

        // OUR computed EIP-712 domain separator + struct (contents) hash.
        let dom = domain(
            v["neg_risk"].as_bool().unwrap(),
            v["chain_id"].as_u64().unwrap(),
        );
        let our_domain_sep = dom.hash_struct();
        let our_struct_hash = order.eip712_hash_struct();

        // The reference POLY_1271 signature uses the Solady EIP-7739 layout
        // (innerSig ‖ appDomainSeparator ‖ contentsHash ‖ contentsType ‖ len);
        // its embedded appDomainSeparator (bytes [65:97)) and contentsHash
        // (bytes [97:129)) are recorded in the fixture, transcribed from
        // py-clob-client-v2's EXPECTED_POLY_1271_SIGNATURE. Matching them pins
        // our V2 domain + order-struct hashing against Polymarket's reference.
        assert_eq!(
            hex::encode(our_domain_sep),
            v["ref_app_domain_separator"].as_str().unwrap(),
            "V2 domain separator mismatch (name/version/chainId/verifyingContract)"
        );
        assert_eq!(
            hex::encode(our_struct_hash),
            v["ref_contents_hash"].as_str().unwrap(),
            "V2 order struct (contents) hash mismatch (typestring/field order/types/values)"
        );
    }

    /// The production signer (`sign_order_with_chain`) returns a 65-byte
    /// `r‖s‖v` hex string (v = 27/28) over the order's own EIP-712 hash — the
    /// POLY_PROXY (sigType 1) production path. Pin the format/length so a
    /// regression in the signing-hash plumbing is caught here.
    #[test]
    fn sign_order_returns_65_byte_rsv_over_order_hash() {
        use alloy_primitives::Signature;
        let raw = include_str!("../tests/fixtures/sign_vectors_v2.json");
        let v: serde_json::Value = serde_json::from_str(raw).unwrap();
        let signer: PrivateKeySigner = v["private_key"].as_str().unwrap().parse().unwrap();
        let o = &v["order"];
        let chain_id = v["chain_id"].as_u64().unwrap();
        let neg_risk = v["neg_risk"].as_bool().unwrap();
        let order = ClobOrder {
            salt: o["salt"].as_u64().unwrap(),
            maker: o["maker"].as_str().unwrap().parse().unwrap(),
            signer: o["signer"].as_str().unwrap().parse().unwrap(),
            token_id: o["tokenId"].as_str().unwrap().to_string(),
            maker_amount: o["makerAmount"].as_u64().unwrap(),
            taker_amount: o["takerAmount"].as_u64().unwrap(),
            side: Side::Buy,
            signature_type: 1, // production POLY_PROXY
            timestamp: o["timestamp"].as_u64().unwrap(),
            metadata: parse_b256(o["metadata"].as_str().unwrap()),
            builder: parse_b256(o["builder"].as_str().unwrap()),
        };
        let sig_hex = sign_order_with_chain(&signer, &order, neg_risk, chain_id).unwrap();
        let bytes = hex::decode(sig_hex.trim_start_matches("0x")).unwrap();
        assert_eq!(bytes.len(), 65, "production sig is 65 bytes r‖s‖v");
        assert!(matches!(bytes[64], 27 | 28), "v is Electrum 27/28, got {}", bytes[64]);

        // It must be a valid signature over the order's own EIP-712 hash and
        // recover to the signer — i.e. `sign_order` signs the right digest.
        // Reconstruct the sol `Order` with the SAME signatureType (1) the
        // ClobOrder used, so the verification hash matches what was signed.
        let mut sol = order_from_fixture(o);
        sol.signatureType = 1;
        let signing_hash = sol.eip712_signing_hash(&domain(neg_risk, chain_id));
        let sig = Signature::from_raw(&bytes).unwrap();
        assert_eq!(
            sig.recover_address_from_prehash(&signing_hash).unwrap(),
            signer.address(),
            "production sig must recover to the signer over the order EIP-712 hash"
        );
    }

    /// THE GATE (Task 19): reproduce the FULL `EXPECTED_POLY_1271_SIGNATURE`
    /// from py-clob-client-v2 byte-for-byte via `sign_order_1271`.
    ///
    /// The fixture's order has maker = signer = 0x1111…1111, which IS the
    /// deposit wallet for this vector → the wallet-domain verifyingContract.
    /// Intermediate asserts localise any failure to the nested-digest/typehash
    /// (appDomainSeparator + contentsHash are independently known-good).
    #[test]
    fn reproduces_poly1271_reference_vector() {
        let raw = include_str!("../tests/fixtures/sign_vectors_v2.json");
        let v: serde_json::Value = serde_json::from_str(raw).unwrap();
        let signer: PrivateKeySigner = v["private_key"].as_str().unwrap().parse().unwrap();
        let o = &v["order"];
        let chain_id = v["chain_id"].as_u64().unwrap();
        let neg_risk = v["neg_risk"].as_bool().unwrap();
        let deposit_wallet: Address = o["maker"].as_str().unwrap().parse().unwrap();
        let order = ClobOrder {
            salt: o["salt"].as_u64().unwrap(),
            maker: deposit_wallet,
            signer: o["signer"].as_str().unwrap().parse().unwrap(),
            token_id: o["tokenId"].as_str().unwrap().to_string(),
            maker_amount: o["makerAmount"].as_u64().unwrap(),
            taker_amount: o["takerAmount"].as_u64().unwrap(),
            side: Side::Buy,
            signature_type: 3, // POLY_1271
            timestamp: o["timestamp"].as_u64().unwrap(),
            metadata: parse_b256(o["metadata"].as_str().unwrap()),
            builder: parse_b256(o["builder"].as_str().unwrap()),
        };

        let wrapped = sign_order_1271(&signer, &order, neg_risk, chain_id, deposit_wallet).unwrap();
        let bytes = hex::decode(wrapped.trim_start_matches("0x")).unwrap();

        // Localise: embedded appDomainSeparator [65:97) + contentsHash [97:129)
        // must equal the known-good fixture values BEFORE comparing the whole.
        assert_eq!(
            hex::encode(&bytes[65..97]),
            v["ref_app_domain_separator"].as_str().unwrap(),
            "embedded appDomainSeparator mismatch"
        );
        assert_eq!(
            hex::encode(&bytes[97..129]),
            v["ref_contents_hash"].as_str().unwrap(),
            "embedded contentsHash mismatch"
        );
        // Localise: the leading 65-byte innerSig pins the nested digest/typehash.
        assert_eq!(
            format!("0x{}", hex::encode(&bytes[0..65])),
            v["ref_inner_sig_first65"].as_str().unwrap(),
            "innerSig mismatch → nested-digest / TypedDataSign typehash wrong"
        );

        // THE GATE: full wrapped wire signature, byte-for-byte.
        assert_eq!(
            wrapped,
            v["expected_poly1271_signature_full"].as_str().unwrap(),
            "full EXPECTED_POLY_1271_SIGNATURE mismatch"
        );
    }

    /// The pinned `ORDER_CONTENTS_TYPE` constant (used to build the ERC-7739
    /// typehash AND emitted as the wire `contentsType`) MUST equal the type
    /// string alloy derives from the `sol! Order` struct. If the struct ever
    /// changes, this fails loudly rather than silently desyncing the 1271 wrap.
    #[test]
    fn order_contents_type_matches_sol_derived() {
        assert_eq!(
            ORDER_CONTENTS_TYPE,
            Order::eip712_encode_type(),
            "pinned contentsType drifted from the sol! Order type string"
        );
        // And its length is the 0x00ba (186) the reference wire trailer encodes.
        assert_eq!(ORDER_CONTENTS_TYPE.len(), 186);
    }

    #[test]
    fn buy_amounts_use_against_us_rounding() {
        // BUY 10 shares (10_000_000 µ) at px 33 of Cent ticks (0.33):
        // makerAmount (µUSDC out) = 3_300_000 exactly; takerAmount = µshares.
        let (maker, taker) = clob_amounts(
            pm_engine::Action::Buy,
            pm_core::num::TickSize::Cent,
            pm_core::num::Px::new(33, pm_core::num::TickSize::Cent).unwrap(),
            pm_core::num::Qty(10_000_000),
        );
        assert_eq!(taker, 10_000_000, "BUY taker = shares");
        assert_eq!(maker, 3_300_000, "BUY maker = µUSDC cost, rounded against us");
    }

    #[test]
    fn sell_amounts_mirror() {
        // SELL 15 shares at 0.67: maker = µshares, taker = 10_050_000 µUSDC.
        let (maker, taker) = clob_amounts(
            pm_engine::Action::Sell,
            pm_core::num::TickSize::Cent,
            pm_core::num::Px::new(67, pm_core::num::TickSize::Cent).unwrap(),
            pm_core::num::Qty(15_000_000),
        );
        assert_eq!(maker, 15_000_000);
        assert_eq!(taker, 10_050_000);
    }
}
