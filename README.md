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
