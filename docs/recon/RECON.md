# Polymarket API Recon — M2 Ingestion

Captured: 2026-06-12. All data confirmed live from this machine.

---

## 1. Endpoints Used

| Endpoint | Base URL | Notes |
|---|---|---|
| Gamma markets | `https://gamma-api.polymarket.com/markets` | **DEPRECATED** — `Deprecation: true`, `Sunset: 2026-05-01`, `Warning: 299 "use /markets/keyset"` |
| Gamma events | `https://gamma-api.polymarket.com/events` | **DEPRECATED** — same headers; successor is `/events/keyset` |
| Gamma markets keyset | `https://gamma-api.polymarket.com/markets/keyset` | Returns `{ markets: [...], next_cursor: <opaque-b64> }` — key-based cursor, not offset-based |
| CLOB markets | `https://clob.polymarket.com/markets` | Returns `{ data, next_cursor, limit, count }` — offset-based cursor |
| CLOB time | `https://clob.polymarket.com/time` | Returns a bare integer (Unix epoch seconds, no envelope) |
| CLOB book | `https://clob.polymarket.com/book?token_id=<ID>` | Returns full book for one token; no pagination |
| WS market feed | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | Subscribe with `{"type":"market","assets_ids":[...]}` |

### CLOB /markets Pagination

- **Cursor parameter**: `next_cursor=` (query param, empty string for first page)
- **Cursor encoding**: base64 of ASCII decimal offset, e.g. `MTAwMA==` → `"1000"`, `MjAwMA==` → `"2000"`
- **Terminal value**: NOT observed (pages 1–4 all returned `count: 1000`; at least 4000 markets exist)
- **Default page size**: 1000 (field `limit` in envelope; also equals `count` when a full page is returned)
- **Observed on page 1**: `next_cursor: "MTAwMA=="`, on page 2: `"MjAwMA=="` — strictly sequential offsets

### Gamma /markets (deprecated) Pagination

- Uses `limit` and `offset` query params directly (observed: `limit=5` works)
- **Recommended replacement**: `/markets/keyset` with opaque base64 JSON cursor (contains `keys`, `oh`, `v` fields)

---

## 2. Gamma Market Fields (`gamma_markets.json`)

Fixture: bare JSON array. 2 entries, `negRisk: false`.

| Field | JSON type | Example | Notes |
|---|---|---|---|
| `id` | string | `"540817"` | Gamma-internal integer ID as string |
| `conditionId` | string | `"0x1fad72fae..."` | 32-byte hex (0x-prefixed) |
| `questionID` | string | `"0x2d5ddf657e..."` | Distinct from conditionId |
| `slug` | string | `"new-rhianna-album-before-gta-vi-926"` | URL slug |
| `clobTokenIds` | **string** | `"[\"982...\", \"538...\"]"` | **STRINGIFIED JSON ARRAY** of strings — must `json.parse()` twice |
| `outcomes` | **string** | `"[\"Yes\", \"No\"]"` | Also stringified JSON array |
| `outcomePrices` | **string** | `"[\"0.51\", \"0.49\"]"` | Stringified array of price strings |
| `negRisk` | bool | `false` | Boolean (not string) |
| `negRiskRequestID` | string | `""` or hex | Empty string when not negRisk |
| `active` | bool | `true` | |
| `closed` | bool | `false` | |
| `makerBaseFee` | **int** | `1000` | Basis points (1000 = 100 bps = 1%) |
| `takerBaseFee` | **int** | `1000` | Same units as maker |
| `orderPriceMinTickSize` | float | `0.001` | Numeric, not string |
| `orderMinSize` | float | `5` | |
| `volume24hr` | float | `2537222.3` | USDC |
| `volumeClob` | float | `...` | CLOB-specific volume |
| `liquidityClob` | float | `...` | |
| `endDate` | string | `"2026-07-31T12:00:00Z"` | ISO 8601 |
| `lastTradePrice` | float | `0.51` | |
| `bestBid` / `bestAsk` | float | `0.50` / `0.52` | |
| `enableOrderBook` | bool | `true` | |
| `acceptingOrders` | bool | `true` | |
| `events` | array | `[{"id":"...","slug":"..."}]` | Parent events list (brief objects) |

**Critical**: `clobTokenIds`, `outcomes`, `outcomePrices` are ALL stringified JSON — naive `string` deserialization is wrong.

---

## 3. Gamma Event Fields (`gamma_events.json`)

Fixture: bare JSON array. 3 entries: 2 `negRisk: true` (World Cup, Fed Decision), 1 `negRisk: false`.

| Field | JSON type | Example | Notes |
|---|---|---|---|
| `id` | string | `"30615"` | |
| `title` | string | `"World Cup Winner "` | May have trailing whitespace |
| `negRisk` | bool | `true` | Boolean |
| `enableNegRisk` | bool | `true` | Separate from negRisk |
| `negRiskAugmented` | bool | `false` | Third negRisk-related flag |
| `markets` | array | `[{...}, ...]` | Member markets; see gamma market fields above |
| `volume24hr` | float | `132116686.3` | |
| `active` | bool | `true` | |
| `closed` | bool | `false` | |

**NegRisk market additional fields**:
| Field | JSON type | Example | Notes |
|---|---|---|---|
| `negRisk` | bool | `true` | Inherited from event |
| `negRiskMarketID` | string | `"0xb5c32a9a..."` | 0x-prefixed contract address |
| `negRiskRequestID` | string | `"0x7976b8db..."` | Matches CLOB `market` field |
| `groupItemTitle` | string | `"Spain"` | Team/candidate name for this leg |
| `makerBaseFee` | int | `1000` | 100 bps for negRisk markets |
| `takerBaseFee` | int | `1000` | 100 bps for negRisk markets |

---

## 4. CLOB Market Fields (`clob_markets.json`)

Fixture: object with `next_cursor`, `limit`, `count` envelope + `data` array of 3 entries.

| Field | JSON type | Example | Notes |
|---|---|---|---|
| `condition_id` | string | `"0x5eed579f..."` | 32-byte hex; may be empty string `""` for old markets |
| `question_id` | string | `"0x2d5ddf65..."` | |
| `minimum_tick_size` | **float** | `0.01` | Numeric (not string); see tick size section |
| `minimum_order_size` | int | `15` | USDC |
| `neg_risk` | bool | `false` | Boolean; NOTE: ALL 4000+ observed entries have `false` |
| `neg_risk_market_id` | string | `""` | Empty when not neg_risk |
| `neg_risk_request_id` | string | `""` | |
| `maker_base_fee` | int | `0` | All observed = 0 in CLOB /markets |
| `taker_base_fee` | int | `0` or `200` | 200 bps taker fee observed on ~rare entries |
| `active` | bool | `true` | |
| `closed` | bool | `true` | Note: active+closed can both be true |
| `accepting_orders` | bool | `false` | |
| `enable_order_book` | bool | `false` | |
| `fpmm` | string | `"0x28560c82..."` | Fixed-price market maker address |
| `tokens` | array | `[{token_id, outcome, price, winner}]` | |
| `tokens[].token_id` | string | `"7347054..."` | Large decimal integer as string; may be `""` for old markets |
| `tokens[].outcome` | string | `"Yes"` / `"No"` / team name | May be `""` for old markets |
| `tokens[].price` | int/float | `0` or `1` | Typically 0 or 1 (resolved markets); float for active |
| `tokens[].winner` | bool | `true` / `false` | |
| `rewards` | object | `{rates, min_size, max_spread}` | `rates` may be null |

---

## 5. CLOB Book Fields (`clob_book.json`)

Fixture: single object with 10 bids + 10 asks.

| Field | JSON type | Example | Notes |
|---|---|---|---|
| `market` | string | `"0x7976b8db..."` | 0x-prefixed bytes32 market hash |
| `asset_id` | string | `"4394372887..."` | Token ID (decimal integer as string) |
| `timestamp` | **string** | `"1781252338277"` | Unix milliseconds as string |
| `hash` | string | `"018e2bbe5d..."` | 40-char hex (no 0x prefix); book state hash |
| `bids` | array | `[{price, size}, ...]` | Sorted descending by price |
| `asks` | array | `[{price, size}, ...]` | Sorted ascending by price |
| `bids[].price` | **string** | `"0.169"` | Decimal string, up to 3 d.p. |
| `bids[].size` | **string** | `"1234567.8"` | Decimal string, up to 2 d.p. |
| `min_order_size` | **string** | `"5"` | String, not number |
| `tick_size` | **string** | `"0.001"` | String (contrast: CLOB market has numeric `minimum_tick_size`) |
| `neg_risk` | bool | `true` | Boolean |
| `last_trade_price` | **string** | `"0.169"` | String |

---

## 6. CLOB Time (`clob_time.json`)

Bare integer: `1781252232` (Unix epoch seconds, no JSON envelope).

---

## 7. Tick Sizes Observed

Across 4000+ CLOB markets:

| Tick Size | Count (page 1, n=1000) | Note |
|---|---|---|
| `0.01` | 968 | Standard — vast majority |
| `0.001` | 31 | Liquid NegRisk markets (World Cup, elections) |
| **`0.04`** | **1** | **ANOMALY — single market ("Will Coinbase staking be available…"); condition_id is empty string; old/legacy market** |

**FLAG: 0.04 tick size exists.** Any implementation assuming only 0.01 and 0.001 will break on this market. The 0.04 market appears to be a legacy entry with empty `condition_id` and `token_id`.

---

## 8. Fee Values Observed

### Gamma /markets (`makerBaseFee` / `takerBaseFee`)
- Standard markets: `1000` / `1000` (100 bps each)
- NegRisk markets: `1000` / `1000`

### CLOB /markets (`maker_base_fee` / `taker_base_fee`)
- Vast majority: `0` / `0`
- Rare legacy entries: `0` / `200` (taker-only fee of 200 bps)

**INCONSISTENCY**: Gamma reports 1000 bps fees on the same markets that CLOB reports 0 bps. Gamma fees appear to be the protocol-level UMA/resolution bond fee, not order-book trading fees. CLOB fees are the actual taker/maker rebate applied on order matching.

---

## 9. WS Event Types Observed

Observed in 60-second capture (3 liquid token IDs subscribed):

| Event type | Count | Envelope | Notes |
|---|---|---|---|
| `book` | 10 | **Array** `[{...}, ...]` | Initial full book snapshots; one object per token in array |
| `price_change` | 1090 | **Object** `{market, price_changes:[...], timestamp, event_type}` | Most common; contains array of `price_changes` items |
| `last_trade_price` | 3 | **Object** `{market, asset_id, price, size, fee_rate_bps, side, timestamp, event_type, transaction_hash}` | Sent on actual trade execution |

**`tick_size_change` was NOT observed** in the 60-second window.

### WS Frame Shapes

**`book` frame** (array envelope — CRITICAL: frame is a JSON array, not object):
```
[
  {
    "market": "0x...",
    "asset_id": "12345...",
    "timestamp": "1781252556841",   // string milliseconds
    "hash": "ccd85dda...",          // 40-char hex, no 0x prefix
    "bids": [{"price":"0.001","size":"1934403.3"}, ...],
    "asks": [{"price":"0.999","size":"31113102.71"}, ...],
    "tick_size": "0.001",           // string
    "event_type": "book",
    "last_trade_price": "0.002"
  },
  ...
]
```

**`price_change` frame** (object envelope):
```
{
  "market": "0x7976b8db...",
  "price_changes": [
    {
      "asset_id": "112680...",
      "price": "0.01",     // string
      "size": "313.95",    // string
      "side": "BUY",       // "BUY" or "SELL"
      "hash": "ad30ec4f...",  // 40-char hex, no 0x prefix
      "best_bid": "0.83",
      "best_ask": "0.831"
    },
    ...
  ],
  "timestamp": "1781252601405",  // string milliseconds
  "event_type": "price_change"
}
```

**`last_trade_price` frame** (object envelope):
```
{
  "market": "0x7976b8db...",
  "asset_id": "4394372887...",
  "price": "0.169",           // string
  "size": "128.09",           // string
  "fee_rate_bps": "0",        // string
  "side": "SELL",             // "BUY" or "SELL"
  "timestamp": "1781252604438",
  "event_type": "last_trade_price",
  "transaction_hash": "0x3ecc05d3..."  // 0x-prefixed tx hash (only event with this)
}
```

---

## 10. WS Behavior

- **Initial messages**: Immediately on subscribe, full book snapshots arrive for each subscribed token as an **array** frame (one object per token). The 60-second window showed 10 book frames (initial snapshots + periodic refreshes).
- **Frame cadence**: `price_change` frames arrived at very high frequency (1090 in 60s ≈ 18/sec) for active World Cup markets.
- **PING handling**: Not explicitly observed (no explicit PING/PONG frames needed; the tokio-tungstenite default handled keepalive without issues).
- **Multiple tokens per subscribe**: The `assets_ids` field accepts an array; all tokens can be in a single subscription message.

---

## 11. Type Consistency Cross-Reference

| Field | REST /book | WS book frame | CLOB /markets | Gamma /markets |
|---|---|---|---|---|
| `timestamp` | string (ms) | string (ms) | N/A | ISO 8601 string |
| `tick_size` / `minimum_tick_size` | string | string | **float** | float |
| `price` (levels) | string | string | float (token.price) | string (in outcomePrices) |
| `size` (levels) | string | string | N/A | N/A |
| `neg_risk` / `negRisk` | bool | absent | bool | bool |
| `hash` | string (no 0x) | string (no 0x) | N/A | N/A |
| `min_order_size` / `minimum_order_size` | string | absent | int | float |
| fees | N/A | N/A | int (bps) | int (bps) |

---

## 12. Deprecation Warning

`gamma-api.polymarket.com/markets` and `/events` return:
- `Deprecation: true`
- `Sunset: Fri, 01 May 2026 00:00:00 GMT` (already past — endpoint still responds)
- `Warning: 299 - "use /markets/keyset"` and `"use /events/keyset"`

The keyset endpoint (`/markets/keyset`) uses a different cursor format: an opaque base64-encoded JSON blob with `{v, k, oh, keys}` structure, NOT the offset-based cursor of CLOB /markets.

---

## 13. NegRisk Field Observation

- Gamma `/events` uses `negRisk` (camelCase, boolean)
- Gamma `/markets` member within events uses `negRisk` (camelCase, boolean)
- CLOB `/markets` uses `neg_risk` (snake_case, boolean)
- CLOB `/book` uses `neg_risk` (snake_case, boolean)
- WS `book` frame: `neg_risk` field is **absent** (not present in WS book envelope)
- In 4000+ CLOB `/markets` pages observed, **zero** entries had `neg_risk: true` — the flag appears set elsewhere or only on active negRisk markets returned by specific queries
