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

## Pinned algorithm (Task 19)

Pinned byte-for-byte against Solady `src/accounts/ERC1271.sol`
(`_erc1271IsValidSignatureViaNestedEIP712`, the canonical ERC-7739
implementation Polymarket's deposit wallet uses) AND reproduced against the
py-clob-client-v2 `EXPECTED_POLY_1271_SIGNATURE` (full literal, ends `00ba`).

**contentsType** (the V2 Order EIP-712 type string, the SAME string our `sol!`
Order produces) — ASCII, length 186 = `0x00ba` (matches the wire trailer):
```
Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)
```
**contentsName** = the substring up to the first `(` = `Order` (Solady implicit
mode: the wire's `contentsDescription` == `contentsType`, which starts with the
name; no separate appended name).

**TypedDataSign typehash** = `keccak256(preimage)` where the preimage is
(Solady line 248 — `TypedDataSign({ContentsName} contents,...){ContentsType}`):
```
TypedDataSign(Order contents,string name,string version,uint256 chainId,address verifyingContract,bytes32 salt)Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)
```
i.e. `"TypedDataSign(" + contentsName + " contents,string name,string version,uint256 chainId,address verifyingContract,bytes32 salt)" + contentsType`.
Domain fields are ALWAYS `name,version,chainId,verifyingContract,salt` (5 fields,
NO `extensions`, no conditional) — confirmed from the literal at lines 271–273.

**Wallet domain** = { name "DepositWallet", version "1", chainId, verifyingContract
= deposit_wallet, salt = 0x0 }. salt IS part of the struct (the 7th word).

**hashStruct(TypedDataSign)** = `keccak256(7 words)` (Solady `keccak256(t,0xe0)`,
order is load-bearing — `contents` BEFORE `name`, line 156):
```
[0] typedDataSignTypehash
[1] contentsHash                 (= Order.eip712_hash_struct(), known-good d23d42d3…)
[2] keccak256("DepositWallet")   (wallet domain name)
[3] keccak256("1")               (wallet domain version)
[4] chainId                      (uint256, e.g. 80002)
[5] verifyingContract            (deposit_wallet, left-padded to 32)
[6] salt                         (0x0…0)
```

**Nested digest the EOA signs** (Solady line 283, `keccak256(0x1e, 0x42)`):
```
keccak256(0x1901 ‖ appDomainSeparator ‖ hashStruct(TypedDataSign))
```
appDomainSeparator = the EXCHANGE V2 domain separator (known-good a440cbd8…),
NOT the wallet domain. Sign this 32-byte digest with the EOA (ECDSA, v=27/28).

**Wrapped wire signature** (Solady lines 158–159):
```
innerSig(65) ‖ appDomainSeparator(32) ‖ contentsHash(32) ‖ contentsType(186 ascii) ‖ uint16_be(186)=00ba
```
Returned as `0x` + hex. RESULT: reproduces the full
`EXPECTED_POLY_1271_SIGNATURE` byte-for-byte (validated in
`sign.rs::reproduces_poly1271_reference_vector`).

## Auth binding (Task 22)

Why: with the V2 order format + POLY_1271 order signature both correct, a live
order is rejected with `400 {"error":"the order signer address has to be the
address of the API KEY"}`. We tried `order.signer` = EOA (rejected) AND
`order.signer` = deposit wallet (rejected, same error). The reason: the API key
the bot derives is bound to NEITHER usefully — it is bound to the **EOA**, while
the venue requires `order.signer == the API key's bound address`, and a
deposit-wallet order's signer is the **deposit wallet**. The earlier "auth
works / live venue armed" success was a false positive: deriving a key against
the EOA succeeds (you get valid creds), but that key is bound to the EOA, not
the deposit wallet — the binding only bites at order time.

**This is a known, still-open SDK bug.** Both the Python and TS V2 SDKs have the
same defect, reported in detail with fix sketches but no maintainer response and
no merged PR as of this recon (2026-06-13):
- py-clob-client-v2 #70 "POLY_1271 (sig type 3) order placement fails: L1 auth
  always binds API key to EOA, never the deposit wallet"
- py-clob-client-v2 #64, #71, #90 (Safe wallet) — same symptom.
- clob-client-v2 (TS) #65 — same defect, with the clearest fix sketch.

Root cause, quoted from py-clob-client-v2 #70:
> "_l1_headers ignores self.signature_type and self.funder entirely"
> "sign_clob_auth_message puts signer.address() (the EOA) into the ClobAuth.address
>  field of the EIP-712 payload"

From clob-client-v2 #65:
> "order.signer == api_key.address on every order. For POLY_1271 the SDK sends
>  order.signer = funderAddress (deposit wallet) while the api-key is bound to the
>  EOA. The two values can never match."

### The 5 questions, answered authoritatively

1. **API-key creation/derivation — what address does the key BIND to, and is the
   L1 sig plain ECDSA or ERC-1271/7739 wrapped?**
   For a POLY_1271 deposit wallet the key must bind to the **DEPOSIT WALLET**.
   The ClobAuth `address` field = **deposit wallet (funder)**, and the L1
   signature is an **ERC-7739 `TypedDataSign`-wrapped** signature (the EOA signs
   on behalf of the deposit wallet; the CLOB validates it via the deposit
   wallet's ERC-1271 `isValidSignature`). Source — clob-client-v2 #65 fix sketch:
   > "ClobAuth.address should be set to the deposit wallet address (funderAddress)…
   >  Must use ERC-7739/EIP-1271 wrapping (TypedDataSign)… verifyingContract:
   >  funderAddress (the deposit wallet)… The pattern mirrors order signing:
   >  buildOrderSignature for signatureType === 3 … wraps via TypedDataSign with
   >  verifyingContract: msg.signer so the deposit wallet's ERC-1271 implementation
   >  can validate it."
   py-clob-client-v2 #70 fix sketch (Python): when `signature_type == POLY_1271`,
   build `ClobAuth(address=funder, …)`, ERC-7739-wrap with the **deposit-wallet
   domain (name "DepositWallet", version "1", verifyingContract = funder)**, sign
   with the EOA, return headers with `POLY_ADDRESS = funder`.
   (For a plain EOA/POLY_PROXY account: ClobAuth.address = EOA, plain ECDSA — the
   existing path, unchanged.)

2. **L2 headers POLY_ADDRESS — EOA or deposit wallet?** The **deposit wallet**.
   clob-client-v2 #65:
   > "The same change is needed in createL2Headers for L2 auth; otherwise reads
   >  against api-keys registered to deposit wallets break the same way."
   (POLY_ADDRESS must equal the API key's bound address; the key is now bound to
   the deposit wallet.)

3. **order.signer / order.maker — both = deposit wallet?** Yes. Confirmed by the
   reference vector (maker == signer == the deposit wallet) and by #65
   ("the SDK sends order.signer = funderAddress"). Already implemented in live.rs.

4. **order "owner" field — api key string or an address?** The **api key string**
   (the `apiKey` UUID), NOT an address. Already implemented (`owner: creds.key`).
   The address binding is enforced via the bound key + POLY_ADDRESS, not `owner`.

5. **How does the official client make `order.signer == the API KEY's address`
   hold?** By binding the API key to the **deposit wallet** at create/derive time
   (ClobAuth.address = deposit wallet + ERC-7739-wrapped L1 sig + L1 POLY_ADDRESS
   = deposit wallet), AND setting order.signer = deposit wallet. Both sides equal
   the deposit wallet. The bot's bug was binding the key to the EOA, so neither
   EOA-signer nor deposit-wallet-signer could ever equal the bound address.

### ClobAuth EIP-712 struct (py-clob-client-v2 signing/eip712.py, confirmed)
Domain: name **"ClobAuthDomain"**, version **"1"**, chainId, **NO
verifyingContract**. Struct fields in order: `address` (address), `timestamp`
(string), `nonce` (uint256), `message` (string). Message constant =
"This message attests that I control the given wallet". (Matches `auth.rs`.)

### Implementation (Task 22) — generalised ERC-7739 wrap
The order-path nesting in `sign.rs::sign_order_1271` is generalised into
`erc7739_wrap(signer, app_domain_sep, contents_hash, contents_type,
contents_name, wallet_domain)`; the order path reproduces its reference vector
byte-for-byte (gate intact). For the ClobAuth L1 wrap:
- app domain = **ClobAuthDomain/1/chainId** (no verifyingContract) — separator
  computed from that domain (NOT the exchange domain — ClobAuth is a different
  app).
- contents = the ClobAuth struct; contentsHash = `ClobAuth.eip712_hash_struct()`;
  contentsType = `"ClobAuth(address address,string timestamp,uint256 nonce,string
  message)"`; contentsName = "ClobAuth".
- wallet domain = **DepositWallet/1/chainId/deposit_wallet/salt0** (same wallet
  domain as orders).
- nested digest the EOA signs = `keccak256(0x1901 ‖ clobAuthDomainSep ‖
  hashStruct(TypedDataSign))`; wire = `innerSig(65) ‖ clobAuthDomainSep(32) ‖
  contentsHash(32) ‖ contentsType(ascii) ‖ uint16_be(len)`.

NOTE: no Polymarket-published ClobAuth-1271 reference vector exists (the SDK bug
means the official clients never produce one). The construction is pinned to the
SAME Solady ERC-7739 scheme proven byte-exact for orders, only swapping the app
domain + contents — so it is mechanically validated, but the FIRST live run is
the end-to-end proof. Diagnostics (below) make that run decisive.

### Diagnostics added (Part B)
- `auth.rs::derive_or_create_api_key`: logs the raw create/derive JSON response
  with `secret`+`passphrase` REDACTED, ALL other fields shown (reveals any bound
  `address`/`profileAddress` the venue returns), plus the ClobAuth `address` used
  and the L1 POLY_ADDRESS sent.
- `live.rs::submit_fak`: on the first order logs `maker`, `signer`, `owner`
  (api-key id), and the L2 POLY_ADDRESS sent. (Addresses/ids only — no secrets.)
