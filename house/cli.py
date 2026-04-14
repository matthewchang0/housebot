from __future__ import annotations

import argparse
import json

from .bot import HouseBot
from .dashboard import serve_dashboard


def main() -> None:
    parser = argparse.ArgumentParser(description="House U.S. House trading bot")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("alpaca-check", help="Verify Alpaca credentials and connectivity")
    dashboard = subparsers.add_parser("dashboard", help="Run the local monitoring web app")
    dashboard.add_argument("--host", default="127.0.0.1", help="Host interface to bind")
    dashboard.add_argument("--port", type=int, default=8765, help="Port for the dashboard server")
    subparsers.add_parser("status", help="Show local bot status, runtime markers, and latest log event")
    ai_brief = subparsers.add_parser("ai-brief", help="Generate a read-only AI operator brief from local bot state")
    ai_brief.add_argument("--focus", default="", help="Optional topic to emphasize in the brief")
    subparsers.add_parser("sync-fills", help="Sync House-owned Alpaca fills into the local ledger")
    subparsers.add_parser("ingest", help="Poll all sources and store new filings")
    standby = subparsers.add_parser(
        "standby",
        help="Ignore existing filings, wait for the next fresh disclosure, and flatten positions",
    )
    standby.add_argument(
        "--no-liquidate",
        action="store_true",
        help="Arm standby mode without attempting to cancel open orders or close positions",
    )
    rebalance = subparsers.add_parser("rebalance", help="Build targets and execute or preview orders")
    rebalance.add_argument("--plan-only", action="store_true", help="Build the rebalance plan without submitting orders")
    subparsers.add_parser("risk-check", help="Evaluate hard risk limits and take protective actions")
    subparsers.add_parser("daily-report", help="Generate the daily summary report")
    subparsers.add_parser("run", help="Run the continuous scheduler loop")
    args = parser.parse_args()

    bot = HouseBot()
    try:
        if args.command == "alpaca-check":
            print(json.dumps(bot.alpaca_check()))
        elif args.command == "dashboard":
            bot.close()
            serve_dashboard(host=args.host, port=args.port)
        elif args.command == "status":
            print(json.dumps(bot.status()))
        elif args.command == "ai-brief":
            print(json.dumps(bot.ai_brief(focus=args.focus or None)))
        elif args.command == "sync-fills":
            print(json.dumps({"new_fills": bot.sync_broker_fills(), "ledger": bot._ledger_summary()}))
        elif args.command == "ingest":
            print(json.dumps({"new_filings": bot.ingest_once()}))
        elif args.command == "standby":
            print(
                json.dumps(
                    bot.standby_for_next_filing(execute_liquidation=not args.no_liquidate)
                )
            )
        elif args.command == "rebalance":
            result = bot.rebalance(execute=not args.plan_only)
            print(
                json.dumps(
                    {
                        "rebalance_date": result.rebalance_date.isoformat(),
                        "targets": len(result.targets),
                        "orders": len(result.planned_orders),
                        "skipped": result.skipped_symbols,
                    }
                )
            )
        elif args.command == "risk-check":
            print(json.dumps({"actions": bot.risk_check()}))
        elif args.command == "daily-report":
            summary = bot.daily_report()
            print(json.dumps({"nav": summary.nav, "daily_pnl": summary.daily_pnl}))
        elif args.command == "run":
            bot.run()
    finally:
        bot.close()
