# Migrating bot access to a new laptop

Sets up SSH + the convenience aliases (`pnl`, `botlogs`, `botssh`) on a new
machine so you can operate the 24/7 copy bot running on the EC2 box. **The bot
keeps running on EC2 the whole time** — this is purely local access setup, no
downtime.

> Secrets are never in git. The SSH key (`*.pem`) and `.env` are git-ignored and
> must be moved by hand, never committed/emailed/Slacked.

## 1. Get the SSH key onto the new laptop

AWS does not let you re-download a `.pem`, so either:

- **Option A — copy the existing key (simplest).** Transfer `ansh.pem` from the
  old laptop securely (AirDrop / encrypted USB / password manager). Put it at
  e.g. `~/.ssh/ansh.pem`.

- **Option B — add a new key (if you can't copy it).** On the NEW laptop:
  ```bash
  ssh-keygen -t ed25519 -f ~/.ssh/copybot
  ```
  Then from the OLD laptop (which still has access), authorize it on the box:
  ```bash
  cat ~/.ssh/copybot.pub | botssh 'cat >> ~/.ssh/authorized_keys'
  ```
  Use `~/.ssh/copybot` as the key path below.

Lock the key down (SSH refuses world-readable keys):
```bash
chmod 600 ~/.ssh/ansh.pem
```

## 2. Add the aliases to the new laptop's shell

Append this block to `~/.zshrc` (bash users: `~/.bashrc`). Set `COPYBOT_KEY` to
where you put the key and `COPYBOT_HOST` to the box's current public DNS (from
the AWS console, or your Elastic IP):

```bash
# --- Polymarket copy bot on EC2 ---
export COPYBOT_HOST="ec2-user@<YOUR-EC2-PUBLIC-DNS>"
export COPYBOT_KEY="$HOME/.ssh/ansh.pem"
alias pnl='ssh -i "$COPYBOT_KEY" "$COPYBOT_HOST" "bash ~/copybot/deploy/status.sh"'
alias botlogs='ssh -i "$COPYBOT_KEY" "$COPYBOT_HOST" "journalctl -u copybot -f"'
alias botssh='ssh -i "$COPYBOT_KEY" "$COPYBOT_HOST"'
```

Reload:
```bash
source ~/.zshrc
```

## 3. Test

```bash
botssh 'echo connected; systemctl is-active copybot'   # → connected / active
pnl                                                     # positions + P&L
```

## Notes

- **The EC2 public DNS changes on stop/start** (not on reboot), which breaks
  `pnl`/`botlogs`/`botssh`. Fix once by assigning an **Elastic IP** (stays fixed,
  free while attached to a running instance), or update `COPYBOT_HOST` in
  `~/.zshrc` after each stop/start.
- **Only one bot per wallet.** Do not also run the bot locally against the same
  deposit wallet while the EC2 instance is live.
- To change the BOT CODE and redeploy, see the **"24/7 deployment"** section in
  `README.md` (rsync → `deploy/setup.sh` → `sudo systemctl restart copybot`).
- The bot's runtime secrets live in `~/copybot/.env` **on the box**, never in a
  laptop repo. See `handoff.md` for full project context.
