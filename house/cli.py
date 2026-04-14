from __future__ import annotations

import argparse
import json

from .bot import HouseBot


def main() -> None:
    parser = argparse.ArgumentParser(description="House U.S. House trading bot")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("ingest", help="Poll all sources and store new filings")
    rebalance = subparsers.add_parser("rebalance", help="Build targets and execute or preview orders")
    rebalance.add_argument("--plan-only", action="store_true", help="Build the rebalance plan without submitting orders")
    subparsers.add_parser("risk-check", help="Evaluate hard risk limits and take protective actions")
    subparsers.add_parser("daily-report", help="Generate the daily summary report")
    subparsers.add_parser("run", help="Run the continuous scheduler loop")
    args = parser.parse_args()

    bot = HouseBot()
    try:
        if args.command == "ingest":
            print(json.dumps({"new_filings": bot.ingest_once()}))
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
