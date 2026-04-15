# House

House is a Python trading service that monitors U.S. House financial disclosures,
builds a 130/30 long-short portfolio, and routes paper-trading orders through Alpaca.

## Features

- Polls House Clerk, Quiver, and Capitol Trades sources
- Parses official PTR PDFs from the House Clerk archive
- Stores filings, orders, risk events, and snapshots in SQLite
- Can generate a read-only Claude Sonnet operator brief from local bot state
- Applies decay, context scoring, conflict resolution, and portfolio caps
- Generates rebalance plans, daily summaries, and JSONL audit logs
- Defaults to Alpaca paper trading and requires explicit `MODE=LIVE` to switch

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env
```

Set the required environment variables, then run one of:

```bash
house alpaca-check
house ai-brief
house status
house sync-fills
house dashboard
house ingest
house standby
house rebalance
house daily-report
house risk-check
house run
```

## Notes

- `house` automatically reads a local `.env` file from the current directory or a parent directory.
- Set `BACKEND_ID`, `DB_PATH`, `LOG_PATH`, and `REPORT_PATH` per agent when multiple bots share an Alpaca account.
- `house sync-fills` imports House-owned Alpaca fills into a backend-scoped local ledger using the order prefix.
- `house ai-brief` is optional and only needs `ANTHROPIC_API_KEY`; it never places trades or changes portfolio state.
- `house dashboard` starts a read-only local web app at `http://127.0.0.1:8765` so you can watch bot activity in real time.
- `house standby` freezes trading on already-known filings, keeps polling, and only re-enables the strategy after the next newly ingested disclosure arrives.
- House is conservative by design: ambiguous filings are flagged and skipped.
- Orders are only submitted during regular market hours.
- `client_order_id` values are deterministic per rebalance date and symbol to keep execution idempotent.

## Server Deploy

For a real server or VPS, run the scheduler as a long-lived process. Two deploy paths are included:

- Docker: `docker compose up -d --build`
- systemd: use `deploy/install-server.sh`, then start `house.service`

Recommended layout on a Linux VPS:

```bash
sudo mkdir -p /opt/house
sudo chown -R $USER:$USER /opt/house
git clone <repo-url> /opt/house
cd /opt/house
cp .env.example .env
python3.13 -m venv .venv
source .venv/bin/activate
pip install -e .
sudo cp deploy/house.service /etc/systemd/system/house.service
sudo cp deploy/house-dashboard.service /etc/systemd/system/house-dashboard.service
sudo systemctl daemon-reload
sudo systemctl enable --now house.service
sudo systemctl enable --now house-dashboard.service
```

The scheduler service runs `house run`. The dashboard service exposes the read-only UI on port `8765`.
