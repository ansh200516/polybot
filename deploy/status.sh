#!/usr/bin/env bash
# Quick unattended status: open copy positions, fills, and today's realized P&L.
# Usage: bash deploy/status.sh [db_path]
set -euo pipefail
DB="${1:-$HOME/copybot/data/copy-canary.sqlite}"

if [ ! -f "$DB" ]; then
  echo "no DB at $DB (bot not started yet?)"
  exit 0
fi

echo "=== OPEN copy positions (tracked + managed) ==="
sqlite3 -header -column "$DB" \
  "SELECT substr(condition_id,1,12)||'…' AS market, outcome_index AS oi, \
          substr(trader,1,10)||'…' AS trader, \
          printf('%.2f', qty_micro/1e6) AS shares, \
          printf('%.2f', cost_micro/1e6) AS cost_usd \
   FROM copy_positions ORDER BY condition_id;"

OPEN=$(sqlite3 "$DB" "SELECT COUNT(*) FROM copy_positions;")
DEPLOYED=$(sqlite3 "$DB" "SELECT printf('%.2f', COALESCE(SUM(cost_micro),0)/1e6) FROM copy_positions;")
FILLS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM fills;")
DAYPNL=$(sqlite3 "$DB" "SELECT printf('%.2f', COALESCE((SELECT realized_micro FROM day_realized WHERE strategy='copy' ORDER BY utc_day DESC LIMIT 1),0)/1e6);")
echo
echo "open=$OPEN  deployed=\$$DEPLOYED  fills=$FILLS  realized_today(copy)=\$$DAYPNL"
