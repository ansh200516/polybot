# RECON-M5-V2-1271 — Deposit-wallet / POLY_1271 flow (2026-06-13)

Why: with V2 order format accepted, the live venue rejected our order with
`{"error":"maker address not allowed, please use the deposit wallet flow"}`.
New API accounts (our operator) must trade via a **deposit wallet** using
**signatureType POLY_1271 (3)** with an **ERC-7739 wrapped signature**, not the
POLY_PROXY (1) flow.

Source: live docs.polymarket.com/trading/deposit-wallets + py-clob-client-v2 /
clob-client-v2. The exact ERC-7739 `TypedDataSign` construction follows the
Solady standard; the implementer (Task 19) must pin it byte-for-byte against
the reference vector below — DO NOT ship unvalidated.

## Deposit wallet ADDRESS (the order `maker`)

Deterministic CREATE2:
```
walletId = bytes32(owner)            # owner = signer EOA, left-padded to 32 bytes
salt     = keccak256(abi.encode(factory, walletId))
address  = CREATE2(factory, salt, initCodeHash)   # UUPS or BeaconProxy clone
factory (Polygon 137) = 0x00000000000Fb5C9ADea0298D729A0CB3823Cc07
```
SDK helpers: TS `deriveDepositWalletAddress()`, Py `get_expected_deposit_wallet()`.
Clone type (UUPS vs Beacon) requires probing the chain → **deriving it
ourselves is on-chain work; instead the operator supplies their deposit-wallet
address from the Polymarket UI** (env `PM_DEPOSIT_WALLET`). The wallet must be
DEPLOYED first (operator's manual $1 trade already deployed it).

## Signature: ERC-7739 wrapped (POLY_1271)

- signatureType = **3**.
- The EOA signs a nested **TypedDataSign** payload (Solady EIP-7739).
- **App domain** = the Exchange V2 domain (name "Polymarket CTF Exchange",
  version "2", chainId, verifyingContract = exchange_v2 / neg_risk_exchange_v2)
  — already implemented in `sign.rs::domain`. Its separator for the vector is
  `a440cbd865bc0c6243d7a8df9a8bf48a8827b0a4abbb61c30e96d305423af148`.
- **Wallet domain** = name "DepositWallet", version "1", chainId,
  verifyingContract = **the deposit wallet address**, salt = 0x0…0.
- Nested digest the EOA signs (Solady 7739): `keccak256(0x1901 ‖ appDomainSeparator ‖ hashStruct(TypedDataSign))`
  where `hashStruct(TypedDataSign)` mixes the TypedDataSign typehash, the order
  `contentsHash`, and the WALLET domain fields. **Exact typehash string +
  field order: pin from Solady / Polymarket source in Task 19.**
- Wire wrapped signature = `innerSig(65) ‖ appDomainSeparator(32) ‖ contentsHash(32) ‖ contentsType(bytes) ‖ uint16(len(contentsType))`.
  `contentsType` = the Order EIP-712 type string
  `Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)`; its length is `0x00ba` (186) in the reference.

## Reference vector (fixture sign_vectors_v2.json + full expected in py-clob-client-v2)

The fixture is exactly a POLY_1271 case: key `0xac0974…ff80`, chainId AMOY 80002,
exchange verifyingContract 0xE111…996B, order {salt 479249096354, maker = signer
= **0x1111…1111** (= the deposit wallet for the test → wallet-domain
verifyingContract), tokenId 1234, makerAmount 100000000, takerAmount 50000000,
side BUY, sigType 3, timestamp 1710000000000, metadata/builder zero}.
- embedded appDomainSeparator = `a440cbd8…423af148` (matches our exchange domain ✓, Task 16)
- embedded contentsHash = `d23d42d3…5dd83804` (matches our Order struct hash ✓, Task 16)
- leading 65-byte innerSig = `0xa3a093c83b6c20c83355c16ce94c92e6e9fcbdeb840618cc74f6c57a42ad145b2b98db73d2c73cbf1f2b6af288566ae81960ddbc3a13921027358a8bff3be6ff1c`
- FULL `EXPECTED_POLY_1271_SIGNATURE` (ends `00ba`): in
  py-clob-client-v2 tests/order_utils/test_exchange_order_builder_v2.py —
  Task 19 fetches it and validates the WHOLE wrapped output byte-for-byte.

Since Task 16 already proved appDomainSeparator + contentsHash match, the only
new thing Task 19 must get right is the **innerSig** (the nested-digest
construction) and the wrapper assembly. Reproducing the leading 65-byte innerSig
(with wallet domain verifyingContract = 0x1111…1111, chainId 80002) validates the
nested digest; reproducing the full expected validates the wrapper.

## Auth

L1 ClobAuth + L2 HMAC unchanged (the operator already derives an API key fine —
the earlier "live venue armed" success proves auth works).
