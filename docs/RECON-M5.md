# RECON-M5 — Polymarket CLOB trading API (live-verified 2026-06-13)

Method: public endpoints curled live; signing/auth recipes pinned against
`py-clob-client 0.34.6` (Python 3.11 venv) with a THROWAWAY key
(`0xadad…ad`, address `0x45C4Ef602bC5EB4493070d69B4F3b6a74f952216` — never
fund); response shapes and rate limits from docs.polymarket.com (llms-full
dump, 2026-06-13). Reference vectors live in
`crates/execution/tests/fixtures/` (`sign_vectors.json`, `auth_vectors.json`,
`clob_responses/`).

## Verified constants

| Item | Verified value | Source |
|---|---|---|
| Chain id | 137 | `get_contract_config(137)` |
| CTF Exchange (regular) | `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` | client `ContractConfig` |
| NegRisk CTF Exchange | `0xC5d563A36AE78145C45a50134d48A1215220f80a` | client `ContractConfig` |
| Collateral (USDC.e) | `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174` | client `ContractConfig` |
| Order EIP-712 domain | `name="Polymarket CTF Exchange"`, `version="1"`, chainId, verifyingContract | `py_order_utils.builders.base_builder` source |
| Order struct | `salt,maker,signer,taker,tokenId,makerAmount,takerAmount,expiration,nonce,feeRateBps,side(uint8),signatureType(uint8)` | py_order_utils model (vectors prove hash) |
| signature_type 1 (POLY_PROXY) | `maker` = funder (proxy wallet), `signer` = EOA | generated vectors |
| ClobAuth domain | `name="ClobAuthDomain"`, `version="1"`, chainId=137, no contract | `py_clob_client.signing.eip712` |
| ClobAuth message | `"This message attests that I control the given wallet"` | `MSG_TO_SIGN` |
| L1 endpoints | `GET /auth/derive-api-key`, `POST /auth/api-key`; headers POLY_ADDRESS / POLY_SIGNATURE / POLY_TIMESTAMP / POLY_NONCE | client source |
| L2 HMAC | base64url-with-padding(HMAC-SHA256(base64url-decode(secret), ts+METHOD+path+body)) | vectors (incl. GET/no-body) |
| L2 GET paths | HMAC signs the request PATH ONLY — query params (cursor etc.) appended after header creation | `get_trades`/`get_orders` source |
| Server time | `GET /time` → unix seconds (plain text), e.g. `1781297313` | live curl |

## Wire format (POST /order)

`py-clob-client` serialises compactly (`separators=(",", ":")`) and HMACs the
exact serialized body string:

```json
{"order": {"salt": <int>, "maker": "0x…", "signer": "0x…", "taker": "0x…",
  "tokenId": "<dec str>", "makerAmount": "<µ str>", "takerAmount": "<µ str>",
  "expiration": "0", "nonce": "0", "feeRateBps": "0", "side": "BUY"|"SELL",
  "signatureType": 1, "signature": "0x<65-byte hex>"},
 "owner": "<api key>", "orderType": "FAK", "postOnly": false}
```

NOTE: wire `side` is the STRING; the SIGNED struct encodes side as uint8
(0=BUY, 1=SELL). `salt` is a plain int on the wire (`round(now*random())`,
fits u64); all other numerics are strings.

## Amounts (vectors prove)

BUY: `makerAmount` = µUSDC outlay = px·qty exact (10 sh @ 0.33 → 3,300,000);
`takerAmount` = µshares. SELL: mirror. Matches `pm-core` `buy_cost` /
`sell_proceeds` exactly on tick-aligned prices (no rounding divergence found —
prices are always tick-aligned in our engine).

## Order placement response (POST /order, 200)

```json
{"success": true, "errorMsg": "", "orderID": "0x…", "takingAmount": "…",
 "makingAmount": "…", "status": "matched", "transactionsHashes": ["…"],
 "tradeIDs": ["…"]}
```

Statuses: `live` (resting — should not occur for FAK), `matched` (immediate),
`delayed` (async matching delay; sports/crypto markets add 1 s / 250 ms taker
delays), `unmatched` (marketable but failed to delay — placement still
"successful", treat as zero-fill). Processing failures come back HTTP 200
with `success:false` + `errorMsg` (e.g. "not enough balance / allowance");
validation failures are HTTP 4xx. BOTH must map to `VenueError::Live`.
`matched` responses carry aggregate `makingAmount`/`takingAmount` — per-level
fill detail comes from `GET /data/trades` (paginated `{"data": […],
"next_cursor": "LTE="}`; cursor `MA==`=start, `LTE=`=end; trade rows carry
`taker_order_id`, `price`, `size`, `side`, `asset_id`, `status`
MATCHED→MINED→CONFIRMED, and `maker_orders[]`).

## Minimums & fees (CORRECTIONS to plan assumptions)

- **`minimum_order_size` = 5 and the unit is SHARES, not dollars** (live
  markets sampled 2026-06-13; both sampled markets also `minimum_tick_size`
  0.001). Plan's `[live] min_leg_usd` is the wrong shape → replaced by
  `min_leg_shares` (default 5.0), gate compares leg qty µshares.
- Fee fields are live and non-zero on some markets (sampled market:
  `maker_base_fee` = `taker_base_fee` = 1000 bps) — `feeRateBps` in the order
  must carry the market's taker fee (our `Order.fee_bps` already does).
- Sports markets: resting orders auto-cancelled at game start; 1 s marketable
  delay. Crypto/finance up-down: 250 ms taker delay → FAK may return
  `delayed`; treat as zero-fill after the fill window.

## Rate limits (docs, Cloudflare-throttled not rejected)

- `POST /order`: 5,000 req/10 s burst, 120,000 req/10 min sustained.
- `/data/orders`, `/data/trades`: 500 req/10 s. Auth endpoints: 100 req/10 s.
- Our deterministic 5 req/s limiter is ~100× under the venue limits — keep it.

## Magic/email (proxy) account specifics

- `signature_type 1`; the FUNDER (= `maker`) is the Polymarket proxy wallet
  shown in the UI profile; the SIGNER is the EOA whose key is exported from
  Polymarket settings. `PM_PROXY_ADDRESS` env carries the funder; no reliable
  unauthenticated lookup endpoint found → binary fatals with instructions
  when unset (operator copies it from the profile page).
- Allowances: proxy-wallet accounts have exchange allowances managed by the
  proxy system (UI-driven); the docs' allowance endpoints exist
  (`GET balance allowance`, 200 req/10 s) — the shadow session (Task 13)
  verifies trading readiness end-to-end before funding.
- Relayer txn tier limits exist (100/day unverified) but only affect
  on-chain ops (deposits, splits) — NOT CLOB orders, which are gasless. M5
  pure-buy live trading is unaffected; M6's split path must mind them.

## Throwaway-key vector provenance

`sign_vectors.json`: 3 orders (BUY cent/regular, BUY milli/neg-risk, SELL
cent/regular), each with full wire fields + signature, sig_type 1, funder
`0x1111…11`. `auth_vectors.json`: L1 ClobAuth signature (ts 1750000000,
nonce 0) + 2 L2 HMAC vectors (POST with body, GET without). Regenerate with
the snippet in the M5 plan Task 1 if the client version moves.
