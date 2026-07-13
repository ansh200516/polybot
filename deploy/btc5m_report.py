#!/usr/bin/env python3
"""Phase-1 gate report + resolution backfill for the BTC Up/Down 5m shadow harness.

The live loop can only log a PROXY strike (our composite spot at window-open) —
Gamma's real strike/close (`eventMetadata.{priceToBeat,finalPrice}`) is populated
only AFTER a window resolves. So this offline report BACKFILLS the truth from
Gamma once per window (cached in a `btc5m_resolution` table), then reports:

  (A) PROXY-strike gate      — the live-logged view (works before resolution).
  (B) TRUE-strike gate       — recomputed on the real strike, with REALIZED
                               win-rate vs the real outcome (the trustworthy gate).
  (C) BASIS                  — proxy-strike minus true-strike distribution
                               (how much error the live proxy introduces).
  (D) CALIBRATION            — model p_up vs realized win-rate + Brier skill.

Usage: python3 deploy/btc5m_report.py [db_path] [z_threshold] [--no-backfill]
Stdlib only (sqlite3/urllib/json/statistics) so it runs on the box unchanged.
"""
import sqlite3, sys, statistics, os, json, time, urllib.request

GAMMA = "https://gamma-api.polymarket.com"
UA = {"User-Agent": "pm-arb-bot/1.0"}
BUCKETS = [(0, 10), (10, 20), (20, 45), (45, 90)]

args = [a for a in sys.argv[1:] if not a.startswith("--")]
flags = {a for a in sys.argv[1:] if a.startswith("--")}
db = args[0] if len(args) > 0 else os.path.expanduser("~/copybot/data/copy-canary.sqlite")
Z = float(args[1]) if len(args) > 1 else 1.5
DO_BACKFILL = "--no-backfill" not in flags


def jget(url, timeout=20):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def window_open_secs(ts_ms, secs_to_go):
    """Reconstruct the window's open unix-secs (300-aligned) from a shadow row.
    close ≈ ts_ms/1000 + secs_to_go; open = close − 300; snap to the 5-min grid."""
    close = ts_ms / 1000.0 + secs_to_go
    return int(round((close - 300.0) / 300.0) * 300)


def ensure_resolution_table(con):
    con.execute(
        "CREATE TABLE IF NOT EXISTS btc5m_resolution ("
        "condition_id TEXT PRIMARY KEY, price_to_beat REAL, final_price REAL, "
        "outcome_up INTEGER, fetched_ts INTEGER)"
    )
    con.commit()


def backfill(con):
    """Fetch TRUE strike/close/outcome from Gamma for each resolved window not yet
    cached. Returns (fetched, still_unresolved). Reconstructs each window's slug
    from a shadow row and verifies conditionId matches before trusting it."""
    ensure_resolution_table(con)
    have = {r[0] for r in con.execute("SELECT condition_id FROM btc5m_resolution")}
    cids = [r[0] for r in con.execute("SELECT DISTINCT condition_id FROM btc5m_shadow")]
    fetched, unresolved, mismatch = 0, 0, 0
    for cid in cids:
        if cid in have:
            continue
        rr = con.execute(
            "SELECT ts_ms, secs_to_go FROM btc5m_shadow WHERE condition_id=? "
            "ORDER BY secs_to_go DESC LIMIT 1", (cid,)).fetchone()
        slug = f"btc-updown-5m-{window_open_secs(rr[0], rr[1])}"
        try:
            ev = jget(f"{GAMMA}/events?slug={slug}")
        except Exception as e:
            print(f"  ! backfill {cid[:12]}… slug={slug}: {type(e).__name__}: {e}", file=sys.stderr)
            continue
        if not ev or not ev[0].get("markets"):
            continue
        m = ev[0]["markets"][0]
        if str(m.get("conditionId", "")).lower() != cid.lower():
            mismatch += 1  # slug reconstruction landed on the wrong window
            continue
        meta = ev[0].get("eventMetadata") or {}
        ptb, fin = meta.get("priceToBeat"), meta.get("finalPrice")
        if ptb is None or fin is None:
            unresolved += 1  # not resolved yet (eventMetadata null during live window)
            continue
        ptb, fin = float(ptb), float(fin)
        con.execute("INSERT OR REPLACE INTO btc5m_resolution VALUES (?,?,?,?,?)",
                    (cid, ptb, fin, 1 if fin >= ptb else 0, int(time.time())))
        fetched += 1
    con.commit()
    if mismatch:
        print(f"  ! {mismatch} window(s) skipped (slug reconstruction mismatch)", file=sys.stderr)
    return fetched, unresolved


def leader_econ(spot, strike, sig, p_up, bid, ask, Z):
    """Given a shadow row, return (fires, up_leads, net_edge_c) for 'buy the leader'
    using strike to pick the side. `fair`/`offer` from the YES book: UP→ask/p_up,
    DOWN→(1−bid)/(1−p_up). None net if no book. `fires` = |z|>=Z."""
    if not sig:
        return (False, None, None)
    z = (spot - strike) / sig
    if abs(z) < Z:
        return (False, None, None)
    up = z > 0
    if up:
        offer, fair = ask / 1e6, p_up
    else:
        offer, fair = (1.0 - bid / 1e6 if bid else None), 1.0 - p_up
    if not offer or offer <= 0:
        return (True, up, None)
    fee = 0.07 * offer * (1 - offer)
    return (True, up, (fair - offer - fee) * 100.0)


def gate_table(rows, Z, strike_of, outcome_of=None):
    """Print a T-bucket gate table. `strike_of(r)` picks proxy vs true strike;
    `outcome_of(r)` (if given) yields 1/0 UP outcome → adds a realized win%."""
    hdr = f"{'bucket':>9} {'n_leader':>9} {'edge>=2c%':>9} {'med_edge_c':>11}"
    if outcome_of:
        hdr += f" {'win%(real)':>10}"
    print(hdr)
    for lo, hi in BUCKETS:
        nets, wins, n_lead = [], [], 0
        for r in rows:
            secs = r["secs_to_go"]
            if not (lo <= secs < hi):
                continue
            fires, up, net = leader_econ(r["spot"], strike_of(r), r["sigma_tau"],
                                         r["p_up"], r["best_bid_micro"], r["best_ask_micro"], Z)
            if not fires:
                continue
            n_lead += 1
            if net is not None:
                nets.append(net)
            if outcome_of and up is not None:
                o = outcome_of(r)
                if o is not None:
                    wins.append(1 if (up == bool(o)) else 0)
        cells = [f"{f'[{lo},{hi})':>9}", f"{n_lead:>9}"]
        cells.append(f"{(100.0*sum(x>=2.0 for x in nets)/len(nets)):>8.1f}%" if nets else f"{'-':>9}")
        cells.append(f"{statistics.median(nets):>11.2f}" if nets else f"{'-':>11}")
        if outcome_of:
            cells.append(f"{(100.0*sum(wins)/len(wins)):>9.1f}%" if wins else f"{'-':>10}")
        print(" ".join(cells))


# ---------------------------------------------------------------- load + backfill
con = sqlite3.connect(db)
try:
    raw = con.execute(
        "SELECT condition_id, secs_to_go, spot, strike, sigma_tau, p_up, "
        "best_bid_micro, best_ask_micro FROM btc5m_shadow WHERE sigma_tau > 0").fetchall()
except sqlite3.OperationalError:
    print("no btc5m_shadow table (strategy not deployed/enabled yet)."); sys.exit(0)
cols = ["condition_id", "secs_to_go", "spot", "strike", "sigma_tau", "p_up",
        "best_bid_micro", "best_ask_micro"]
rows = [dict(zip(cols, r)) for r in raw]
print(f"btc5m Phase-1 gate report  (Z={Z})  shadow_samples={len(rows)}  db={db}")
if not rows:
    print("(no shadow samples yet — enable btc5m and wait for the ~3h vol warmup.)"); sys.exit(0)

resolved = {}
if DO_BACKFILL:
    f, u = backfill(con)
    print(f"backfill: +{f} resolved window(s) fetched, {u} still unresolved (eventMetadata null).")
ensure_resolution_table(con)
resolved = {r[0]: {"ptb": r[1], "fin": r[2], "up": r[3]}
            for r in con.execute("SELECT condition_id, price_to_beat, final_price, outcome_up FROM btc5m_resolution")}
res_rows = [r for r in rows if r["condition_id"] in resolved]

# ------------------------------------------------------------------ (A) proxy gate
print("\n(A) PROXY-strike gate — live-logged view (strike = our spot@open):")
gate_table(rows, Z, strike_of=lambda r: r["strike"])

# --------------------------------------------------- (B) true-strike gate + realized
print(f"\n(B) TRUE-strike gate — real priceToBeat + REALIZED win% "
      f"({len(res_rows)}/{len(rows)} samples resolved):")
if res_rows:
    gate_table(res_rows, Z,
               strike_of=lambda r: resolved[r["condition_id"]]["ptb"],
               outcome_of=lambda r: resolved[r["condition_id"]]["up"])
else:
    print("  (no resolved windows cached yet — run again after windows close.)")

# ------------------------------------------------------------------------- (C) basis
print("\n(C) BASIS — proxy strike (spot@open) − true strike (priceToBeat), $:")
if res_rows:
    # one basis per window (constant within a window)
    per_win = {}
    for r in res_rows:
        per_win.setdefault(r["condition_id"], r["strike"] - resolved[r["condition_id"]]["ptb"])
    b = sorted(per_win.values())
    n = len(b)
    q = lambda p: b[min(n - 1, int(p * n))]
    print(f"  windows={n}  median={statistics.median(b):+.2f}  "
          f"p10={q(0.10):+.2f}  p90={q(0.90):+.2f}  max|basis|={max(abs(x) for x in b):.2f}")
    big = sum(1 for x in b if abs(x) > 5.0)
    print(f"  |basis|>$5 in {big}/{n} windows "
          f"({'proxy is a poor strike — trust the TRUE-strike gate' if big > n*0.2 else 'proxy tracks the true strike well'}).")
else:
    print("  (needs resolved windows.)")

# ------------------------------------------------------------------ (D) calibration
print("\n(D) CALIBRATION — model p_up vs realized UP-rate (resolved samples):")
if res_rows:
    brier = statistics.mean((r["p_up"] - resolved[r["condition_id"]]["up"]) ** 2 for r in res_rows)
    print(f"  {'p_up bin':>10} {'n':>6} {'mean_p':>8} {'realized':>9}")
    for lo in [i / 10 for i in range(10)]:
        hi = lo + 0.1
        bin_rows = [r for r in res_rows if lo <= r["p_up"] < hi or (hi >= 1.0 and r["p_up"] == 1.0)]
        if not bin_rows:
            continue
        mp = statistics.mean(r["p_up"] for r in bin_rows)
        rz = statistics.mean(resolved[r["condition_id"]]["up"] for r in bin_rows)
        print(f"  {f'[{lo:.1f},{hi:.1f})':>10} {len(bin_rows):>6} {mp:>8.3f} {rz:>9.3f}")
    print(f"  Brier={brier:.4f}  (skill vs 0.5 baseline = {1 - brier/0.25:+.1%}; higher = better-calibrated)")
else:
    print("  (needs resolved windows.)")

print("\nGATE (spec §5, Gate 1→2): proceed to Phase 2 only if (B) the TRUE-strike")
print("[0,20)s buckets show a positive median net edge AND high realized win% on a")
print("meaningful n_leader — and (C) the basis is small enough to trust the live proxy.")
