# M6 — Deposit-Wallet Relayer (Live Merge/Redeem) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Live on-chain `merge`/`redeem` for the V2 deposit wallet via Polymarket's relayer `WALLET` batch — recycle locked YES+NO pairs and claim resolved winners on live — replacing the `NotSupportedLive` stubs.

**Architecture:** New `crates/execution/src/relayer.rs` `RelayerClient`: EIP-712 `Batch` sign (alloy `sol!`, golden-vector-validated) → builder-auth headers → POST to relayer → poll `STATE_CONFIRMED`; `merge`/`redeem` calldata for the `CtfCollateralAdapter`. The live MM holds an optional `RelayerClient`; merge/redeem run as a rate-limited periodic sweep (never inline). Gated (`relayer_enabled` off by default; staging-first). Paper untouched.

**Tech Stack:** Rust, `alloy` (`sol!` EIP-712 + provider for the nonce read — already a dep for signing), `reqwest`, the existing `auth.rs` `l2_headers` (likely reusable for builder auth), `pm-execution`/`pm-app`.

**Spec:** `docs/superpowers/specs/2026-06-25-m6-deposit-wallet-relayer-design.md`. **Reference:** Polymarket `py-builder-relayer-client@e7108cd` (deposit-wallet support).

**BUILD ORDER:** the offline-validatable core (M6-1/2/3) first (golden-vector + calldata tests, no creds/funds), then the relayer I/O (M6-4/5/6), then wiring (M6-7) + integration (M6-8).

---

## Task M6-1: Contracts + config + secrets

**Files:** `crates/execution/src/relayer.rs` (new, contracts const + `BuilderCreds`); `crates/config/src/lib.rs` (`[live]` relayer knobs); `crates/execution/src/secrets.rs` (`BUILDER_API_*`, `RPC_URL`).

- [ ] **Step 1** Add `crates/execution/src/relayer.rs` with the Polygon-137 contract constants (typed `Address`): `DEPOSIT_WALLET_FACTORY = 0x894Ee6B254f251518206f709E9B115f214ebDf17`, `DEPOSIT_WALLET_IMPL = 0x55913A0bdecCbB77b7Af781A48300e6394B5EEAE`. Add a `CTF_COLLATERAL_ADAPTER` const (RESEARCH the 137 address — Unknown B; leave a clearly-marked `// TODO(M6-B): confirm adapter address` + a unit test asserting it's set before live use). Declare `pub mod relayer;` in `lib.rs`.
- [ ] **Step 2** `secrets.rs`: load `BUILDER_API_KEY`, `BUILDER_SECRET`, `BUILDER_PASS_PHRASE`, `RPC_URL` (same `.env`/env pattern as `PM_API_*`). Add a `BuilderCreds { key, secret, passphrase }` (mirror the CLOB `ApiCreds`). Test the loader.
- [ ] **Step 3** `config.rs`: `[live]` gains `relayer_enabled: bool` (default false), `relayer_staging: bool` (default true), `relayer_url: Option<String>` (default None → derive from staging flag). Validate. Test parse + default.
- [ ] **Step 4** `cargo test -p pm-execution -p pm-config && cargo clippy -p pm-execution -p pm-config --all-targets -- -D warnings` (use `CARGO_TARGET_DIR=/Users/ansh.singh/test/target` if the sandbox libsqlite3-sys bindgen error appears).
- [ ] **Step 5** Commit: `feat(execution,config): M6 relayer contracts + builder creds + [live] relayer config`

---

## Task M6-2: EIP-712 Batch signing (THE de-risk — golden vector)

**Files:** `crates/execution/src/relayer.rs` + test.

- [ ] **Step 1 — failing golden-vector test** (the highest-value test in M6):
```rust
#[test]
fn sign_batch_matches_polymarket_golden_vector() {
    // From py-builder-relayer-client tests/builder/test_deposit_wallet.py (chain 137).
    let pk = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    let wallet: Address = "0xa2927E7834648F1C03b4961CeeA4597292e3c025".parse().unwrap();
    let token: Address = "0x0000000000000000000000000000000000000001".parse().unwrap();
    let data = hex::decode("095ea7b30000000000000000000000000000000000000000000000000000000000000002ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff").unwrap();
    let calls = vec![Call { target: token, value: U256::ZERO, data: data.into() }];
    let sig = sign_batch(&signer_from(pk), 137, wallet, 0u64, 1234567890u64, &calls);
    assert_eq!(sig, "0x7827946c566e7860f6c5f2e641587ed6928989c8618e463a00dd56832e7300023b7436c67a2ea82d6d506b1a5eda3e27526e9e2ffaad52128d75c47c2e9d1fac1b");
    assert_eq!(sig.len(), 132); // 0x + 65 bytes
}
```
- [ ] **Step 2** Run → FAIL.
- [ ] **Step 3 — implement** using alloy `sol!` (mirror `auth.rs`/`sign.rs`):
```rust
sol! {
    #[derive(Debug)]
    struct Call { address target; uint256 value; bytes data; }
    #[derive(Debug)]
    struct Batch { address wallet; uint256 nonce; uint256 deadline; Call[] calls; }
}
```
Domain: `eip712_domain! { name: "DepositWallet", version: "1", chain_id, verifying_contract: wallet }`. `sign_batch(signer, chain_id, wallet, nonce, deadline, calls) -> String`: build `Batch`, compute `batch.eip712_signing_hash(&domain)`, sign with the EOA `PrivateKeySigner` (sync ECDSA), return `0x`+hex of the 65-byte sig. CRITICAL: field order/types MUST match §2.1 exactly (the golden vector enforces it).
- [ ] **Step 4** Run the golden-vector test → PASS. If it doesn't match, the EIP-712 encoding is off (check: `value`/`nonce`/`deadline` as `uint256`, `data` as dynamic `bytes`, `calls` as a struct array, domain `verifyingContract = wallet`). **Do not proceed until byte-exact.**
- [ ] **Step 5** `cargo test -p pm-execution sign_batch && cargo clippy …` → green/clean.
- [ ] **Step 6** Commit: `feat(execution): EIP-712 deposit-wallet Batch signing (Polymarket golden-vector validated)`

---

## Task M6-3: merge/redeem calldata + deposit-wallet derivation

**Files:** `crates/execution/src/relayer.rs` + tests.

- [ ] **Step 1 — failing tests:** `merge_call` / `redeem_call` produce calldata with the right selector + ABI-encoded args; `derive_deposit_wallet` matches the py golden vector (owner `0xf39F…2266`, factory `0x801c…b049`, impl `0x24f3…139B` → `0x63cB1B4eC2F274Ed553aD5079c6A2542d1c02bd7`).
- [ ] **Step 2** FAIL.
- [ ] **Step 3 — implement** via alloy `sol!`:
```rust
sol! {
    function mergePositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] partition, uint256 amount);
    function redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets);
}
```
`merge_call(adapter, collateral, condition_id, amount) -> Call` with `partition=[1,2]`, `parentCollectionId=0x0`; `redeem_call(...)` with `indexSets=[1,2]`. `derive_deposit_wallet(owner, factory, impl)` via the CREATE2 + ERC-1967 init-code-hash from the py `derive.py` (port `init_code_hash_erc1967` + `get_create2_address`). (We already HAVE the wallet address from `.env`; this is a sanity/verification helper + lets us assert the configured wallet matches the owner.)
- [ ] **Step 4** Tests pass; `cargo clippy …` clean.
- [ ] **Step 5** Commit: `feat(execution): CtfCollateralAdapter merge/redeem calldata + deposit-wallet derivation`

---

## Task M6-4: Builder auth + WALLET request + poll (relayer I/O)

**Files:** `crates/execution/src/relayer.rs` + tests. **Research Unknowns A + C.**

- [ ] **Step 1 — RESEARCH the builder-auth header scheme** (`py-builder-signing-sdk`): determine the headers the relayer requires. Compare to the existing `auth.rs::l2_headers` (POLY-* HMAC-SHA256 over `ts+method+path+body`). If identical, REUSE `l2_headers` with the builder creds; if different, implement the SDK's scheme. Document the finding. Also confirm the **submit endpoint path** + the **poll endpoint + state JSON** (Unknown C).
- [ ] **Step 2 — failing test:** the WALLET request body serializes to the py `to_dict()` shape:
```json
{ "type":"WALLET", "from":<eoa>, "to":<factory>, "nonce":<n>, "signature":"0x…",
  "depositWalletParams": { "depositWallet":<wallet>, "deadline":<ts>,
    "calls":[{"target":<adapter>,"value":"0","data":"0x…"}] } }
```
(unit test the serializer against a fixed fixture; mirror `test_client_deposit_wallet.py`'s assertions.)
- [ ] **Step 3 — implement:** `build_wallet_request(from, factory, nonce, deadline, wallet, calls, sig) -> serde_json::Value`; `execute_wallet_batch(...)` = sign (M6-2) + build body + builder-auth headers + `reqwest` POST to `<relayer_url>/<submit_path>` → return `transaction_id`; `poll_until_confirmed(tx_id)` polls the state endpoint until `STATE_CONFIRMED` (timeout + the intermediate states). Errors → `VenueError::Live(...)` (never panic).
- [ ] **Step 4** `cargo test -p pm-execution && cargo clippy …` → green. (No live call in tests — the POST/poll are exercised at the user's funded staging run.)
- [ ] **Step 5** Commit: `feat(execution): relayer WALLET request + builder auth + STATE_CONFIRMED poll`

---

## Task M6-5: Deposit-wallet nonce via RPC

**Files:** `crates/execution/src/relayer.rs` + test.

- [ ] **Step 1** Determine the wallet's current batch nonce. Read it on-chain via an alloy JSON-RPC provider (`RPC_URL`): call the deposit wallet's `nonce()` view (confirm the getter name during M6-4 research — Unknown D; fall back to a configured/`DEPOSIT_WALLET_NONCE` override if the getter is unknown). `deposit_wallet_nonce(rpc_url, wallet) -> u64`.
- [ ] **Step 2** Test with a mocked provider / or gate behind `#[ignore]` requiring a live RPC; at minimum unit-test the request encoding.
- [ ] **Step 3** `cargo clippy …` clean. Commit: `feat(execution): read deposit-wallet batch nonce via Polygon RPC`

---

## Task M6-6: RelayerClient assembly + LiveVenue merge/redeem

**Files:** `crates/execution/src/relayer.rs`, `crates/execution/src/live.rs`.

- [ ] **Step 1** `RelayerClient { http, relayer_url, chain_id, signer, builder_creds, rpc_url, adapter, factory, wallet }`; `RelayerClient::new(...)` from config+secrets (Some only when `relayer_enabled` + creds + RPC present; staging flag picks the URL). `merge(condition_id, amount) -> Result<Usdc>` = nonce → `merge_call` → `execute_wallet_batch` → `poll_until_confirmed` → return recovered collateral (`amount × $1`). `redeem(condition_id) -> Result<Usdc>`.
- [ ] **Step 2** `LiveVenue`: replace the `merge` `NotSupportedLive` stub to delegate to a held `Option<RelayerClient>` (return `NotSupportedLive` only when the relayer isn't configured). Add `redeem`. Keep `split` `NotSupportedLive` (out of scope).
- [ ] **Step 3** Tests: `RelayerClient` constructed-only-when-enabled; merge/redeem call the (mocked) batch path; disabled → `NotSupportedLive`. `cargo test -p pm-execution && cargo clippy …` green.
- [ ] **Step 4** Commit: `feat(execution): RelayerClient + LiveVenue merge/redeem via relayer (gated)`

---

## Task M6-7: MM live merge sweep + redeem (wire into the strategy)

**Files:** `crates/app/src/strategy/mm.rs`, `crates/app/src/main.rs`.

- [ ] **Step 1** Thread an `Option<RelayerClient>` to the MM live path (constructed in `main.rs` from config+secrets, only when `relayer_enabled`). 
- [ ] **Step 2** `maybe_merge_sets` (B5): on LIVE with a relayer present, instead of the no-op, enqueue/perform a relayer `merge` for the matched set — but as a **rate-limited periodic sweep** (e.g. at most once per N cycles), NON-blocking: spawn the on-chain op so the quote loop isn't stalled; on success reduce inventory + credit cash exactly like the paper sim; on failure log + retry next sweep. Without a relayer, keep the logged no-op.
- [ ] **Step 3** Redeem: when a held market has resolved (detect via the Data API `redeemable` flag — reused from the reconcile path), enqueue a relayer `redeem`. (Periodic, same sweep.)
- [ ] **Step 4** mm test: with a mocked relayer, a live `maybe_merge_sets` over a matched set invokes the relayer merge + applies the inventory/cash change; paper unchanged. `cargo test -p pm-app && cargo clippy …` green.
- [ ] **Step 5** Commit: `feat(app): live MM merge/redeem sweep via the relayer (periodic, non-blocking)`

---

## Task M6-8: Integration + final review

- [ ] **Step 1** End-to-end (mocked relayer) test: live hedging accumulates a set → the sweep merges it via the relayer → inventory/cash recycle → gross frees up for more quoting. Plus a `relayer_enabled=false` test proving the live path is the documented hold-to-resolution no-op.
- [ ] **Step 2** `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings` (pinned target) → green/clean.
- [ ] **Step 3** Commit: `test(execution,app): M6 relayer merge/redeem integration`
- [ ] **Step 4** Final whole-M6 review subagent: golden-vector signing, calldata correctness, gating (relayer only when enabled+creds), staging-first, non-blocking sweep, no panics on relayer failure, money integer.
- [ ] **Step 5 (operator)** First funded validation: set `BUILDER_API_*` + `RPC_URL`, `relayer_enabled=true`, `relayer_staging=true` → run a tiny set merge on staging, confirm `STATE_CONFIRMED`; then prod.

## Notes for the implementer
- **The golden-vector test (M6-2) is the gate** — do not build the relayer I/O on top of an unvalidated signer.
- Relayer is OFF by default + constructed only with creds+RPC; paper/non-relayer paths byte-for-byte unchanged.
- On-chain ops NEVER block the quote loop (spawned/periodic) and NEVER panic on failure (log + retry).
- Reuse `auth.rs` `l2_headers` for builder auth IF the scheme matches (confirm in M6-4).
- A merged set = exactly $1/set (matches the paper sim + the inventory math).
