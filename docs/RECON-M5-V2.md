# RECON-M5-V2 — Polymarket CLOB **V2** migration (2026-06-13)

Why this exists: the live `/order` endpoint rejected every M5 order with
`400 {"error":"invalid order version, please use the latest..."}`. Root cause:
Polymarket migrated the CLOB to **V2** *after* the latest pip `py-clob-client`
(0.34.6, which M5 was built against) — so our V1 orders are now rejected.

Method: read-only. The V2 client (`py-clob-client-v2`) is **GitHub-only, not on
pip**, and installing+executing it here was blocked (untrusted-code sandbox), so
all facts below are read from the V2 source via GitHub
(github.com/Polymarket/py-clob-client-v2) + live docs.polymarket.com. **Not yet
executed/validated against a running V2 client or the live venue.**

## What changed V1 → V2

| Aspect | V1 (what M5 ships) | V2 (live, required) |
|---|---|---|
| EIP-712 order struct | salt, maker, signer, **taker**, tokenId, makerAmount, takerAmount, **expiration, nonce, feeRateBps**, side, signatureType | salt, maker, signer, tokenId, makerAmount, takerAmount, side, signatureType, **timestamp, metadata, builder** |
| Domain version | `"1"` | `"2"` (name still "Polymarket CTF Exchange") |
| Exchange (regular) | 0x4bFb…82E | **0xE111180000d2663C0091e4f400237545B87B996B** |
| Exchange (neg-risk) | 0xC5d5…80a | **0xe2222d279d744050d28e00520010520000310F59** |
| **Collateral** | **USDC.e 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174** | **pUSD 0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB** |
| Client | py-clob-client (pip) | py-clob-client-v2 / clob-client-v2 (GitHub only) |
| L1 ClobAuth | domain "ClobAuthDomain" v1 | **unchanged** |
| L2 HMAC headers | base64url HMAC-SHA256 | **unchanged** |

**The collateral change is the headline risk.** V2 settles in pUSD
(0xC011…), not the USDC.e the operator funded. `approve_allowances.py` in the
V2 repo approves `contract_config.collateral` (= pUSD) to the V2 exchange +
CTF. If the operator's Polymarket balance is USDC.e (not pUSD), a V2 order will
be rejected for insufficient balance even with a perfect order format. Open
question: does Polymarket auto-hold deposits as pUSD now? (The operator's $1
manual trade succeeding on the live site would indicate their balance is
already usable V2 collateral.)

## V2 order construction (from order_builder/builder.py)

- `timestamp = str(time.time_ns() // 1_000_000)` — current time in **ms**, string.
- `metadata` defaults to `BYTES32_ZERO` (0x00…00, 32 bytes).
- `builder` = the builder code; **optional** (omit → zero bytes32, no builder fee). We send zero.
- Amounts: BUY → taker = round_down(size), maker = taker·price; SELL mirror.
  Same orientation as V1 (BUY maker = µUSDC, taker = µshares) → our
  `clob_amounts` (pm-core buy_cost/sell_proceeds) stands for tick-aligned px.
- verifyingContract = `neg_risk_exchange_v2` if neg_risk else `exchange_v2`.

## V2 POST /order wire body (from order_data_v2.py `order_to_json_v2`)

```json
{"order":{
  "salt": <int>, "maker": "0x..", "signer": "0x..", "tokenId": "<dec str>",
  "makerAmount": "<µ str>", "takerAmount": "<µ str>", "side": "BUY"|"SELL",
  "expiration": "<str>", "signatureType": <int>, "timestamp": "<ms str>",
  "metadata": "0x00..00", "builder": "0x00..00", "signature": "0x.."},
 "owner": "<api key>", "orderType": "FAK", "deferExec": <bool>, "postOnly": <bool>}
```

NOTE: `salt` and `signatureType` are JSON **numbers**; everything else string.
`side` is the **string**. The wire carries `expiration` (string, "0") even
though it is NOT in the signed struct. New top-level key `deferExec` (bool).

## V2 signing test vector (tests/order_utils/test_exchange_order_builder_v2.py)

Validates struct/domain hashing offline (it is **POLY_1271 / sigType 3** on
**AMOY**, not our POLY_PROXY/sigType 1 on mainnet — but signing is the same
ECDSA-over-EIP712-hash op, so the first 65 sig bytes pin the hash):

- private key `0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80`
- chainId AMOY (80002), verifyingContract = exchange_v2 (0xE111…996B)
- salt 479249096354, maker = signer = 0x1111…1111, tokenId 1234
- makerAmount 100000000, takerAmount 50000000, side BUY(0), signatureType 3
- timestamp 1710000000000, metadata 0x00..00, builder 0x00..00
- expected sig (first 65 bytes) `0xa3a093c83b6c20c83355c16ce94c92e6e9fcbdeb840618cc74f6c57a42ad145b2b98db73d2c73cbf1f2b6af288566ae81960ddbc3a13921027358a8bff3be6ff1c`
  (the full `EXPECTED_POLY_1271_SIGNATURE` appends 1271-specific bytes ending `00ba`; we only match the leading 65-byte ECDSA sig)

## Scope to reach a live V2 fill

1. sign.rs → V2 struct + domain v2 + new addresses (validate vs vector). [no money]
2. live.rs → V2 wire body (timestamp/metadata/builder/deferExec; drop taker/nonce/feeRateBps). [no money]
3. Collateral: operator's funds must be pUSD; if USDC.e, needs conversion
   (on-chain — the M6-deferred relayer/proxy path). **Possible hard blocker.**
4. Re-shadow (free) → re-canary (free unless it fills; diagnosability fix shows any error).

Unvalidated until a live submit: exact wire acceptance, amount rounding vs V2
round_config, the collateral situation.
