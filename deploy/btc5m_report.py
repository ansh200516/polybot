#!/usr/bin/env python3
"""Phase-1 gate: is the terminal-convergence edge harvestable?
For late buckets, among windows where a leader exists (|z|>=Z), report how often
the leader's best offer is below fair (i.e. a taker could buy the ~sure side
cheap). Usage: python3 deploy/btc5m_report.py [db_path] [z_threshold]"""
import sqlite3, sys, statistics, os
db = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/copybot/data/copy-canary.sqlite")
Z = float(sys.argv[2]) if len(sys.argv) > 2 else 1.5
con = sqlite3.connect(db)
rows = con.execute(
    "SELECT secs_to_go, spot, strike, sigma_tau, p_up, best_bid_micro, best_ask_micro "
    "FROM btc5m_shadow WHERE sigma_tau > 0").fetchall()
BUCKETS = [(0,10),(10,20),(20,45),(45,90)]
print(f"btc5m Phase-1 gate report  (Z={Z})  samples={len(rows)}")
print(f"{'bucket(s)':>10} {'n_leader':>9} {'edge>=2c%':>9} {'med_net_edge_c':>15}")
for lo, hi in BUCKETS:
    nets = []
    n_leader = 0
    for secs, spot, strike, sig, p_up, bid, ask in rows:
        if not (lo <= secs < hi):
            continue
        z = (spot - strike) / sig if sig else 0.0
        if abs(z) < Z:
            continue
        n_leader += 1
        up_leads = z > 0
        if up_leads:
            offer = ask / 1e6
            fair = p_up
        else:
            offer = 1.0 - (bid / 1e6) if bid else None
            fair = 1.0 - p_up
        if not offer or offer <= 0:
            continue
        fee = 0.07 * offer * (1 - offer)
        net_edge_c = (fair - offer - fee) * 100.0
        nets.append(net_edge_c)
    if nets:
        pct = 100.0 * sum(1 for x in nets if x >= 2.0) / len(nets)
        print(f"{f'[{lo},{hi})':>10} {n_leader:>9} {pct:>8.1f}% {statistics.median(nets):>15.2f}")
    else:
        print(f"{f'[{lo},{hi})':>10} {n_leader:>9} {'-':>9} {'-':>15}")
print("\nGATE: proceed to Phase 2 only if the [0,20)s buckets show a positive median")
print("net edge on a meaningful n_leader (see spec §5, Gate 1→2).")
