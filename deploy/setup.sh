#!/usr/bin/env bash
# On-box setup for the copy bot (Amazon Linux 2023 / ec2-user). Idempotent —
# safe to re-run after `git pull`/rsync to rebuild + restart.
#
# Run FROM the app dir on the box:  cd ~/copybot && bash deploy/setup.sh
set -euo pipefail

APP_DIR="$HOME/copybot"
cd "$APP_DIR"

echo "==> [1/6] build + runtime deps (gcc/cmake/clang for highs-sys, sqlite CLI for status)"
# highs-sys (the LP solver, a compile-time workspace dep) needs BOTH cmake (builds
# the HiGHS C++ lib) AND libclang (bindgen generates the Rust bindings). sqlite =
# the status.sh CLI.
sudo dnf -y install gcc gcc-c++ make cmake clang clang-devel git openssl-devel sqlite >/dev/null

echo "==> [2/6] swap (Rust release builds are memory-hungry; small instances OOM)"
if ! sudo swapon --show | grep -q '/swapfile'; then
  sudo dd if=/dev/zero of=/swapfile bs=1M count=2048 status=none
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile >/dev/null
  sudo swapon /swapfile
  echo "    added 2G swap"
else
  echo "    swap already present"
fi

echo "==> [3/6] Rust toolchain"
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
# shellcheck disable=SC1091
source "$HOME/.cargo/env"

echo "==> [4/6] build release binary (this can take 5-15 min on a small instance)"
cargo build --release --bin arb
cp -f target/release/arb "$APP_DIR/arb"
mkdir -p "$APP_DIR/data"

echo "==> [5/6] install + enable systemd service"
sudo cp "$APP_DIR/deploy/copybot.service" /etc/systemd/system/copybot.service
sudo systemctl daemon-reload
sudo systemctl enable copybot >/dev/null

echo "==> [6/6] preflight"
if [ ! -f "$APP_DIR/.env" ]; then
  echo "    !! MISSING $APP_DIR/.env — copy your secrets there (chmod 600) BEFORE starting:"
  echo "       scp -i ansh.pem .env ec2-user@<host>:~/copybot/.env"
else
  chmod 600 "$APP_DIR/.env"
  echo "    .env present (chmod 600)"
fi
echo
echo "Done. Start / watch with:"
echo "    sudo systemctl start copybot"
echo "    journalctl -u copybot -f            # live logs"
echo "    bash ~/copybot/deploy/status.sh      # positions + P&L"
