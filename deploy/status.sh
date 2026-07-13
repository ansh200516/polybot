#!/usr/bin/env bash
# Verbose status for the copy bot: open positions with MARKET NAMES, current
# value, and unrealized P&L (joins the local DB with live Data-API prices), plus
# a portfolio summary. Usage: bash deploy/status.sh [db_path] [env_path]
set -euo pipefail
DB="${1:-$HOME/copybot/data/copy-canary.sqlite}"
ENV_FILE="${2:-$HOME/copybot/.env}"
[ -f "$DB" ] || { echo "no DB at $DB (bot not started yet?)"; exit 0; }
# The bot's own wallet — its Data-API positions carry the market names + marks.
WALLET="$(grep -E '^PM_DEPOSIT_WALLET=' "$ENV_FILE" 2>/dev/null | head -1 | cut -d= -f2- | tr -d "\"' ")" || true

# --- Whole-account portfolio, from the bot's last equity-refresh log: cash + all
# positions = the Polymarket "Portfolio" figure (the bot recomputes it ~every 60s
# from the authed CLOB balance + Data-API /value). Strip tracing's ANSI first. ---
EQ_LINE="$(journalctl -u copybot --no-pager -o cat 2>/dev/null | sed -r 's/\x1b\[[0-9;]*m//g' | grep 'equity refreshed' | tail -1)" || true
if [ -n "$EQ_LINE" ]; then
  val() { printf '%s\n' "$EQ_LINE" | grep -oE "$1=[0-9]+" | head -1 | cut -d= -f2; }
  # Age of this reading (the bot refreshes equity every cycle; a large age ⇒ the
  # loop is stalled/down — a stale portfolio here would NOT match the live UI).
  EQ_TS="$(printf '%s' "$EQ_LINE" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:]+' | head -1)"
  AGE=$(( $(date -u +%s) - $(date -u -d "${EQ_TS:-@0}" +%s 2>/dev/null || date -u +%s) ))
  awk -v c="$(val cash_micro)" -v p="$(val positions_micro)" -v e="$(val equity_micro)" \
      -v mg="$(val max_gross_micro)" -v pp="$(val per_position_micro)" -v mc="$(val max_concurrent)" -v age="$AGE" 'BEGIN{
    stale = (age > 180) ? "  ⚠ STALE — bot idle/halted/down, may not match live UI" : "";
    printf "=== ACCOUNT — Polymarket portfolio (updated %ds ago)%s ===\n", age, stale;
    printf "  Portfolio: $%.2f    =    cash $%.2f    +    positions $%.2f\n", e/1e6, c/1e6, p/1e6;
    printf "  Copy caps: max_gross $%.2f (%.0f%% of equity)   per-copy $%.2f   max_concurrent %d\n\n",
           mg/1e6, (e>0 ? 100*mg/e : 0), pp/1e6, mc;
  }'
else
  echo "=== ACCOUNT ==="; echo "  (no equity-refresh log yet — bot starting up, or gross_pct=0)"; echo
fi

python3 - "$DB" "$WALLET" <<'PY' || true
import sqlite3, json, sys, urllib.request

db, wallet = sys.argv[1], (sys.argv[2] or "").strip()
con = sqlite3.connect(db)
rows = con.execute(
    "SELECT condition_id, outcome_index, qty_micro, cost_micro, trader "
    "FROM copy_positions ORDER BY condition_id"
).fetchall()
r = con.execute(
    "SELECT realized_micro FROM day_realized WHERE strategy='copy' ORDER BY utc_day DESC LIMIT 1"
).fetchone()
day_realized = (r[0] if r else 0) / 1e6
fills = con.execute("SELECT COUNT(*) FROM fills").fetchone()[0]

# Live market data (names + current price) for OUR wallet's holdings.
info = {}
if wallet:
    url = f"https://data-api.polymarket.com/positions?user={wallet}&sizeThreshold=0&limit=500"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "copybot-status/1.0"})
        for p in json.load(urllib.request.urlopen(req, timeout=20)):
            info[(str(p.get("conditionId", "")).lower(), int(p.get("outcomeIndex", -1)))] = p
    except Exception as e:
        print(f"(could not fetch live prices/names: {e})\n")
else:
    print("(PM_DEPOSIT_WALLET not found in .env — showing cost only, no live prices)\n")

def field(cid, oi, key):
    return (info.get((cid.lower(), oi)) or {}).get(key)

print("=== COPY BOT — open positions (tracked + managed) ===\n")
hdr = f"{'MARKET':<50} {'SIDE':<4} {'SHARES':>8} {'COST':>7} {'PRICE':>6} {'VALUE':>7} {'UPNL':>8}"
print(hdr); print("-" * len(hdr))

all_cost = 0.0            # cost of every tracked position
m_cost = m_val = 0.0      # cost + value of positions WITH a live price (apples-to-apples)
unmatched = 0
for cid, oi, qmic, cmic, trader in rows:
    shares, cost = qmic / 1e6, cmic / 1e6
    side = "Yes" if oi == 0 else "No"
    title = field(cid, oi, "title") or (cid[:14] + "…")
    name = (title[:49] + "…") if len(title) > 50 else title
    try:
        px = float(field(cid, oi, "curPrice"))
    except (TypeError, ValueError):
        px = None
    all_cost += cost
    if px is not None:
        val = shares * px; m_val += val; m_cost += cost
        px_s, val_s, upnl_s = f"{px:.3f}", f"${val:.2f}", f"{val - cost:+.2f}"
    else:
        unmatched += 1
        px_s, val_s, upnl_s = "n/a", "n/a", "n/a"
    print(f"{name:<50} {side:<4} {shares:>8.2f} {'$%.2f' % cost:>7} {px_s:>6} {val_s:>7} {upnl_s:>8}")

print()
print(f"positions: {len(rows)}   fills(all-time): {fills}   cost basis (all): ${all_cost:.2f}")
if m_val or m_cost:
    print(f"of the {len(rows) - unmatched} with a live price: value ${m_val:.2f} vs cost ${m_cost:.2f}  →  unrealized {m_val - m_cost:+.2f}")
if unmatched:
    print(f"⚠ {unmatched} position(s) had NO live price — likely resolved/left the wallet but still in the DB (stale row; the on-chain reconcile guard would clear these)")
print(f"realized P&L today (copy): ${day_realized:+.2f}")
PY

python3 - "$DB" <<'PY' || true
import sqlite3, sys
db = sys.argv[1]
con = sqlite3.connect(db)
try:
    n = con.execute("SELECT COUNT(*) FROM btc5m_shadow").fetchone()[0]
except sqlite3.OperationalError:
    print("=== BTC 5M BOT ===\n  (no btc5m_shadow table yet — strategy not deployed/enabled)\n")
    raise SystemExit(0)
try:
    r = con.execute("SELECT realized_micro FROM day_realized WHERE strategy='btc5m' ORDER BY utc_day DESC LIMIT 1").fetchone()
except sqlite3.OperationalError:
    r = None
realized = (r[0] if r else 0) / 1e6
last = con.execute("SELECT condition_id, secs_to_go, spot, strike, p_up, best_bid_micro, best_ask_micro FROM btc5m_shadow ORDER BY ts_ms DESC LIMIT 1").fetchone()
print("=== BTC 5M BOT — shadow measurement ===")
print(f"  shadow samples: {n}    realized P&L today (btc5m): ${realized:+.2f}")
if last:
    cid, secs, spot, strike, p_up, bid, ask = last
    print(f"  latest: {cid[:10]}…  T-{secs}s  spot ${spot:,.2f} vs strike ${strike:,.2f}  fair(up)={p_up:.3f}  book {bid/1e6:.3f}/{ask/1e6:.3f}")
print()
PY
