# M6 — Live On-Chain Merge/Redeem via the Deposit-Wallet Relayer

Date: 2026-06-25
Status: Design — awaiting user review before planning/build
Scope: Add LIVE on-chain `merge` (and `redeem`) for the V2 deposit wallet, so the
reward-farm MM can recycle locked YES+NO pairs on live (and claim resolved
winners), and the arb settle path can merge complete sets on live. Replaces the
`NotSupportedLive` stubs in `LiveVenue`.

## 1. Why / context
The V2 deposit wallet (sig type 3 / POLY_1271) holds the conditional tokens. Its
on-chain calls (split/merge/redeem) execute ONLY through Polymarket's **relayer
`WALLET` batch** (gasless; targets the `CtfCollateralAdapter`) — there is no
direct EOA/execTransaction path. Today `LiveVenue::merge`/`split` return
`NotSupportedLive`, so live hedged pairs lock until resolution and the gross cap
throttles. M6 implements the relayer `WALLET` batch in Rust (no Rust client
exists; ported from Polymarket's `py-builder-relayer-client`).

**Hard dependency:** Polymarket **Relayer API keys** — the current scheme from
the "Relayer API keys" settings page: two STATIC headers `RELAYER_API_KEY` +
`RELAYER_API_KEY_ADDRESS` (no HMAC/secret/passphrase), separate from the CLOB
`PM_API_*` keys. The relayer rejects unauthenticated batches; the wallet action
itself is authorized by the EIP-712 Batch signature. (User confirmed they have
these.)

## 2. The protocol (from py-builder-relayer-client@e7108cd)

### 2.1 EIP-712 batch signature (the load-bearing piece)
- Domain: `{ name: "DepositWallet", version: "1", chainId, verifyingContract:
  <deposit_wallet_address> }`.
- Types: `Call { target: address, value: uint256, data: bytes }`;
  `Batch { wallet: address, nonce: uint256, deadline: uint256, calls: Call[] }`.
- primaryType `Batch`; message `{ wallet, nonce, deadline, calls[] }`; signed by
  the **owner EOA** (standard EIP-712 → 65-byte sig, 132 hex chars).
- **Golden vector (Polygon 137)** for an offline signing test (from
  `tests/builder/test_deposit_wallet.py`): pk `0xac09…ff80`, wallet
  `0xa292…c025`, one Call `{target:0x..01, value:"0", data:0x095ea7b3…ffff}`,
  nonce `0`, deadline `1234567890` → signature
  `0x7827946c566e7860f6c5f2e641587ed6928989c8618e463a00dd56832e7300023b7436c67a2ea82d6d506b1a5eda3e27526e9e2ffaad52128d75c47c2e9d1fac1b`.

### 2.2 Relayer request (POST submit-transaction)
```json
{ "type": "WALLET", "from": "<eoa>", "to": "<deposit_wallet_factory>",
  "nonce": "<n>", "signature": "0x…",
  "depositWalletParams": { "depositWallet": "<wallet>", "deadline": "<ts>",
    "calls": [ { "target": "<adapter>", "value": "0", "data": "0x…" } ] } }
```
+ the two **static Relayer-API-key headers** (`RELAYER_API_KEY` +
`RELAYER_API_KEY_ADDRESS` — see §6 Unknown A) and poll the transaction id until
`STATE_CONFIRMED` (`STATE_MINED` is insufficient).

### 2.3 Contracts (Polygon 137)
- deposit_wallet_factory `0x894Ee6B254f251518206f709E9B115f214ebDf17`
- deposit_wallet_implementation `0x55913A0bdecCbB77b7Af781A48300e6394B5EEAE`
- merge/redeem **target = `CtfCollateralAdapter`** (NegRisk → `NegRiskCtfCollateralAdapter`); calldata:
  - `mergePositions(collateralToken, parentCollectionId=0x0, conditionId, partition=[1,2], amount)`
  - `redeemPositions(collateralToken, parentCollectionId=0x0, conditionId, indexSets=[1,2])`
- One-time approvals (ERC-1155 `setApprovalForAll` on the CTF for the adapter)
  submitted as a `WALLET` batch.

## 3. Architecture
New module `crates/execution/src/relayer.rs`:
- `RelayerClient { http, relayer_url, chain_id, signer, relayer_creds, contracts }`.
- `sign_batch(wallet, nonce, deadline, calls) -> sig` — EIP-712 `Batch` via
  alloy `sol!` (same pattern as `auth.rs`/`sign.rs`), pinned to the golden vector.
- `execute_wallet_batch(calls, wallet, nonce, deadline) -> tx_id` — build request
  + the static Relayer-API-key headers + POST + return tx id.
- `poll_until_confirmed(tx_id)`.
- Calldata builders: `merge_call(adapter, collateral, condition_id, amount)`,
  `redeem_call(adapter, collateral, condition_id)` (alloy `sol!` ABI encode).
- `deposit_wallet_nonce()` — read the wallet's current batch nonce via Polygon
  RPC (alloy provider; `RPC_URL`).

Wiring:
- `LiveVenue::merge` (and a new `redeem`) call `RelayerClient` instead of
  `NotSupportedLive`. The `MakerVenue` trait stays CLOB-only; the relayer is a
  SEPARATE capability the live MM holds (a `Option<RelayerClient>`), so paper is
  untouched and the relayer is only constructed when relayer creds are set.
- MM `maybe_merge_sets` (B5): on live, instead of the no-op, enqueue a relayer
  merge for the matched set (and redeem on resolution). **Decision:** do this as
  a PERIODIC sweep (e.g. once per quote cycle, rate-limited), NOT inline in the
  hot path — an on-chain batch is slow + must not block quoting.
- Config `[reward_farm]`/`[live]`: `relayer_url`, relayer creds from env
  (`RELAYER_API_KEY`, `RELAYER_API_KEY_ADDRESS`); a `relayer_enabled` gate
  (default off) and a `staging` flag (use `relayer-v2-staging.polymarket.dev`).

## 4. Scope (this spec)
- **Merge** complete YES+NO sets (MM capital recycling) and **redeem** resolved
  winners. **Split** is NOT needed (reward farming buys, never splits) — omit.
- One-time **approvals** batch (setApprovalForAll) — a one-shot bootstrap command.
- Arb live-merge settlement reuses the same `RelayerClient` (the arm currently
  forced to `Hold`) — wire opportunistically, but the MM path is primary.

## 5. De-risking the blind port (no creds/funds needed to build)
- **Golden-vector test**: `sign_batch` over the §2.1 fixture MUST equal the
  golden signature — proves the EIP-712 encoding is byte-exact offline. This is
  the single highest-risk piece and it's fully testable now.
- **Calldata tests**: `merge_call`/`redeem_call` ABI encodings checked against
  hand-computed selectors + args.
- **Request-shape test**: the JSON body matches the py client's `to_dict()`.
- The relayer **auth + endpoint + poll** can only be fully validated at the
  user's first funded run (staging first) — documented; everything else is unit-tested.

## 6. Unknowns to resolve during the build (research tasks)
- **A — relayer auth header scheme** (RESOLVED): the operator's account uses
  Polymarket's CURRENT "Relayer API keys" scheme — two STATIC headers
  `RELAYER_API_KEY` + `RELAYER_API_KEY_ADDRESS` (no HMAC/timestamp/passphrase).
  The earlier builder-HMAC guess (the stale `py-builder-relayer-client@e7108cd`
  reference) is NOT what this account uses; the wallet action is authorized by
  the EIP-712 Batch signature, so these headers only authenticate the submitter.
- **B — `CtfCollateralAdapter` address** (137) + exact `mergePositions`/
  `redeemPositions` signatures on the adapter (pUSD-native). From the inventory
  docs / on-chain.
- **C — relayer submit endpoint path + response/poll JSON** (`SUBMIT_TRANSACTION`,
  the state poll).
- **D — deposit-wallet nonce source**: read on-chain (RPC) vs relayer-provided.

## 7. Safety
- `relayer_enabled` default OFF; constructed only with relayer creds.
- **Staging-first**: a `staging` flag points at `relayer-v2-staging` so the first
  real batch is on testnet/staging.
- Merge/redeem amounts derive from REAL held inventory (`min(yes,no)` for merge;
  resolved condition for redeem) — never speculative.
- On-chain ops are a periodic sweep, rate-limited, never in the quote hot path;
  a failed batch logs + retries next sweep (never blocks quoting or crashes).
- Money stays integer; a merged set = exactly $1/set (matches the paper sim).

## 8. Testing
- `sign_batch` golden-vector equality (the de-risk).
- `merge_call`/`redeem_call` calldata vs hand-computed.
- WALLET request body shape vs the py `to_dict()` reference.
- `deposit_wallet` address derivation vs the py golden vector (sanity).
- Wiring: live `maybe_merge_sets` enqueues a relayer merge (mocked client);
  paper unchanged (no relayer constructed).
- Live round-trip (staging then prod, tiny) = the user's funded validation.

## 9. Out of scope
- Split (unused by reward farming).
- Wallet DEPLOY (`WALLET-CREATE`) — the user's deposit wallet already exists.
- The CLOB order-signing path (unchanged; already POLY_1271).
