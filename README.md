# House

House is a Python trading service that monitors U.S. House financial disclosures,
builds a 130/30 long-short portfolio, and routes paper-trading orders through Alpaca.

## Features

- Polls House Clerk, Quiver, and Capitol Trades sources
- Parses official PTR PDFs from the House Clerk archive
- Stores filings, orders, risk events, and snapshots in SQLite
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
house ingest
house rebalance
house daily-report
house risk-check
house run
```

## Notes

- House is conservative by design: ambiguous filings are flagged and skipped.
- Orders are only submitted during regular market hours.
- `client_order_id` values are deterministic per rebalance date and symbol to keep execution idempotent.
