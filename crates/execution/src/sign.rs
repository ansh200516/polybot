//! EIP-712 signing of Polymarket CLOB orders (spec 2026-06-13; RECON-M5).
//! Pure: no I/O. Constants are RECON-pinned — a mismatch with docs/RECON-M5.md
//! is a stop-and-fix, not a local edit.

use alloy_primitives::{Address, U256, hex};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{Eip712Domain, SolStruct, eip712_domain, sol};

use pm_core::num::{Px, Qty, TickSize, buy_cost, sell_proceeds};
use pm_engine::Action;

/// RECON-pinned (RECON-M5.md).
pub const CHAIN_ID: u64 = 137;
pub const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
pub const NEG_RISK_CTF_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

sol! {
    /// Field NAMES and ORDER are the EIP-712 typestring — never reorder.
    struct Order {
        uint256 salt;
        address maker;
        address signer;
        address taker;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint256 expiration;
        uint256 nonce;
        uint256 feeRateBps;
        uint8 side;
        uint8 signatureType;
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

/// A CLOB order ready for signing/serialisation. Amounts are 6-decimal
/// integers (µ units); `token_id` is the venue's decimal string.
#[derive(Debug, Clone)]
pub struct ClobOrder {
    pub salt: u64,
    pub maker: Address,  // proxy wallet (signature_type 1)
    pub signer: Address, // EOA exported from Magic
    pub taker: Address,  // 0x0 = public order
    pub token_id: String,
    pub maker_amount: u64,
    pub taker_amount: u64,
    pub expiration: u64, // 0 for FAK
    pub nonce: u64,
    pub fee_rate_bps: u64,
    pub side: Side,
    pub signature_type: u8, // 1 = POLY_PROXY
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

/// EIP-712 domain for the CLOB exchange (regular or neg-risk variant).
///
/// The verifying contract is a RECON-pinned compile-time constant; parsing it
/// cannot fail at runtime. We use a `match` rather than `unwrap` (the crate
/// denies it) — the `Err` arm is unreachable for these constants, and falling
/// back to `Address::ZERO` would yield a domain separator that no operator key
/// could ever match, so a constant typo surfaces immediately as a signing-hash
/// mismatch against the vectors rather than a panic.
fn domain(neg_risk: bool) -> Eip712Domain {
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
        version: "1",
        chain_id: CHAIN_ID,
        verifying_contract: verifying_contract,
    }
}

/// Sign `order` for the given exchange variant, returning the 65-byte
/// `r || s || v` signature as a `0x`-prefixed hex string.
///
/// `v` is encoded as 27/28 (Electrum notation, via `Signature::as_bytes`),
/// matching the eth_account convention that produced the reference vectors.
pub fn sign_order(
    signer: &PrivateKeySigner,
    order: &ClobOrder,
    neg_risk: bool,
) -> Result<String, SignError> {
    let token_id =
        U256::from_str_radix(&order.token_id, 10).map_err(|_| SignError::BadTokenId(order.token_id.clone()))?;

    let sol_order = Order {
        salt: U256::from(order.salt),
        maker: order.maker,
        signer: order.signer,
        taker: order.taker,
        tokenId: token_id,
        makerAmount: U256::from(order.maker_amount),
        takerAmount: U256::from(order.taker_amount),
        expiration: U256::from(order.expiration),
        nonce: U256::from(order.nonce),
        feeRateBps: U256::from(order.fee_rate_bps),
        side: order.side.as_u8(),
        signatureType: order.signature_type,
    };

    let hash = sol_order.eip712_signing_hash(&domain(neg_risk));
    let sig = signer
        .sign_hash_sync(&hash)
        .map_err(|e| SignError::Signer(e.to_string()))?;
    Ok(format!("0x{}", hex::encode(sig.as_bytes())))
}

/// (makerAmount, takerAmount) for a leg, µ units, against-us rounding —
/// matches the engine's own cash math AND the reference client (vectors).
pub fn clob_amounts(action: Action, ts: TickSize, limit_px: Px, qty: Qty) -> (u64, u64) {
    let px_micro = limit_px.microusdc(ts);
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

    /// Every vector in the Task-1 fixture must reproduce byte-identically.
    #[test]
    fn reproduces_py_clob_client_signatures() {
        let raw = include_str!("../tests/fixtures/sign_vectors.json");
        let vectors: Vec<serde_json::Value> = serde_json::from_str(raw).unwrap();
        assert_eq!(vectors.len(), 3, "Task 1 produced 3 vectors");
        for v in &vectors {
            let signer: PrivateKeySigner = v["private_key"].as_str().unwrap().parse().unwrap();
            let o = &v["order"];
            let order = ClobOrder {
                // Fixture encodes EVERY numeric (incl. salt and signatureType) as a
                // decimal STRING — parse accordingly.
                salt: o["salt"].as_str().unwrap().parse().unwrap(),
                maker: o["maker"].as_str().unwrap().parse().unwrap(),
                signer: o["signer"].as_str().unwrap().parse().unwrap(),
                taker: o["taker"].as_str().unwrap().parse().unwrap(),
                token_id: o["tokenId"].as_str().unwrap().to_string(),
                maker_amount: o["makerAmount"].as_str().unwrap().parse().unwrap(),
                taker_amount: o["takerAmount"].as_str().unwrap().parse().unwrap(),
                expiration: o["expiration"].as_str().unwrap().parse().unwrap(),
                nonce: o["nonce"].as_str().unwrap().parse().unwrap(),
                fee_rate_bps: o["feeRateBps"].as_str().unwrap().parse().unwrap(),
                side: if o["side"].as_str().unwrap() == "SELL" {
                    Side::Sell
                } else {
                    Side::Buy
                },
                signature_type: 1,
            };
            let sig = sign_order(&signer, &order, v["neg_risk"].as_bool().unwrap()).unwrap();
            assert_eq!(sig, v["signature"].as_str().unwrap(), "vector {}", v["name"]);
        }
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
