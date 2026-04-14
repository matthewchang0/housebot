from __future__ import annotations

import json
from collections import deque
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from time import sleep
from typing import Any

from .ai import AnthropicClient
from .alpaca import AlpacaClient, AssetInfo
from .config import Settings
from .db import Database
from .execution import (
    filter_targets_by_assets,
    plan_orders,
    scale_new_orders_for_buying_power,
    symbols_for_targets_and_positions,
)
from .http import HttpClient
from .jsonlog import JsonLogger
from .models import DailySummary, Filing, PlannedOrder, RebalanceResult
from .portfolio import construct_targets, targets_as_json
from .reports import write_ai_brief, write_daily_summary, write_rebalance_report
from .sources import CapitolTradesClient, ClerkClient, QuiverClient
from .utils import now_et, seconds_until_next_poll


class HouseBot:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings.load()
        self.logger = JsonLogger(self.settings.log_path)
        self.db = Database(self.settings.db_path)
        self.http = HttpClient(self.settings.user_agent)
        self.report_path = self.settings.report_path
        self.alpaca = AlpacaClient(self.settings, self.http)
        self.ai = AnthropicClient(self.settings, self.http)
        self.clerk = ClerkClient(self.http)
        self.quiver = QuiverClient(self.http, self.settings.quiver_api_key)
        self.capitol = CapitolTradesClient(self.http)

    def close(self) -> None:
        self.http.close()
        self.db.close()

    def alpaca_check(self) -> dict[str, Any]:
        if not self.alpaca.configured:
            raise RuntimeError("Missing Alpaca credentials. Set ALPACA_API_KEY and ALPACA_SECRET_KEY.")

        account = self.alpaca.account_snapshot()
        return {
            "configured": True,
            "mode": self.settings.mode,
            "base_url": self.settings.alpaca_base_url,
            "account": {
                "nav": account.nav,
                "buying_power": account.buying_power,
                "equity": account.equity,
                "cash": account.cash,
            },
            "market_open": self.alpaca.market_is_open(),
        }

    def status(self) -> dict[str, Any]:
        latest_snapshot = self.db.latest_snapshot()
        latest_log = self._latest_log_record(self.logger.path)
        runtime_keys = (
            "last_ingest_date",
            "last_preview_date",
            "last_rebalance_date",
            "last_daily_report",
            "trading_halted",
            "halt_new_entries",
            "awaiting_fresh_filing",
            "fresh_filing_armed_at",
            "fresh_filing_armed_on",
            "fresh_filing_anchor_id",
            "fresh_filing_activated_at",
            "strategy_start_filing_date",
            "strategy_started_at",
        )

        return {
            "mode": self.settings.mode,
            "alpaca_configured": self.alpaca.configured,
            "paths": {
                "db": str(self.settings.db_path),
                "log": str(self.settings.log_path),
            },
            "db_exists": self.settings.db_path.exists(),
            "log_exists": self.settings.log_path.exists(),
            "latest_filing_date": (
                self.db.latest_filing_date().isoformat()
                if self.db.latest_filing_date() is not None
                else None
            ),
            "latest_snapshot": (
                {
                    "snapshot_date": str(latest_snapshot["snapshot_date"]),
                    "nav": float(latest_snapshot["nav"]),
                    "long_exposure": float(latest_snapshot["long_exposure"]),
                    "short_exposure": float(latest_snapshot["short_exposure"]),
                    "net_exposure": float(latest_snapshot["net_exposure"]),
                }
                if latest_snapshot is not None
                else None
            ),
            "runtime_state": {
                key: self.db.get_runtime_state(key)
                for key in runtime_keys
            },
            "latest_log": latest_log,
        }

    def dashboard_data(
        self,
        *,
        log_limit: int = 40,
        order_limit: int = 20,
        filing_limit: int = 20,
        snapshot_limit: int = 14,
        risk_limit: int = 20,
    ) -> dict[str, Any]:
        latest_snapshot = self.db.latest_snapshot()
        recent_snapshots = list(reversed(self.db.recent_snapshots(limit=snapshot_limit)))
        latest_rebalance_report = self._latest_report_payload("rebalance")
        latest_daily_report = self._latest_report_payload("daily-summary")

        return {
            "generated_at": now_et().isoformat(),
            "status": self.status(),
            "latest_order": self._row_to_dict(self.db.latest_order()),
            "order_counts": self.db.order_counts_by_status(),
            "filing_counts": self.db.filing_counts_by_status(),
            "risk_event_counts": self.db.risk_event_counts(),
            "recent_orders": [self._row_to_dict(row) for row in self.db.list_orders(limit=order_limit)],
            "recent_filings": [self._row_to_dict(row) for row in self.db.recent_filings(limit=filing_limit)],
            "recent_risk_events": [self._row_to_dict(row) for row in self.db.list_risk_events(limit=risk_limit)],
            "recent_snapshots": [self._snapshot_to_dict(row) for row in recent_snapshots],
            "latest_positions": self._latest_positions(latest_snapshot),
            "recent_logs": self._read_log_records(self.logger.path, limit=log_limit),
            "latest_rebalance_report": latest_rebalance_report,
            "latest_daily_report": latest_daily_report,
        }

    def ai_brief(self, focus: str | None = None) -> dict[str, Any]:
        if not self.ai.configured:
            raise RuntimeError("Missing Anthropic credentials. Set ANTHROPIC_API_KEY.")

        self.sync_broker_fills()
        prompt_payload = self._ai_brief_payload()
        created_at = now_et()
        brief = self.ai.operator_brief(prompt_payload, focus=focus)
        report_path = write_ai_brief(created_at, brief, self.report_path)
        self.logger.log(
            "AI_BRIEF_CREATED",
            report_path=str(report_path),
            model=self.settings.anthropic_model,
            focus=focus or "",
        )
        return {
            "created_at": created_at.isoformat(),
            "model": self.settings.anthropic_model,
            "report_path": str(report_path),
            "brief": brief,
        }

    def _ai_brief_payload(self) -> dict[str, Any]:
        snapshot = self.dashboard_data(
            log_limit=10,
            order_limit=10,
            filing_limit=10,
            snapshot_limit=5,
            risk_limit=10,
        )
        status = snapshot["status"]
        safe_status = {
            "mode": status.get("mode"),
            "alpaca_configured": status.get("alpaca_configured"),
            "paths": status.get("paths"),
            "db_exists": status.get("db_exists"),
            "log_exists": status.get("log_exists"),
            "latest_filing_date": status.get("latest_filing_date"),
            "runtime_state": status.get("runtime_state"),
        }
        return {
            "generated_at": snapshot["generated_at"],
            "scope_note": (
                "This payload is intentionally limited to House bot local state and local reports. "
                "Shared Alpaca account-level NAV, exposure, and position snapshots are omitted so another "
                "agent on the same account does not bleed into the summary."
            ),
            "status": safe_status,
            "ledger": self._ledger_summary(),
            "latest_order": snapshot["latest_order"],
            "filing_counts": snapshot["filing_counts"],
            "risk_event_counts": snapshot["risk_event_counts"],
            "recent_filings": snapshot["recent_filings"][:5],
            "recent_orders": snapshot["recent_orders"][:5],
            "recent_risk_events": snapshot["recent_risk_events"][:5],
            "latest_rebalance_report": self._summarize_rebalance_report(snapshot["latest_rebalance_report"]),
            "latest_daily_report": self._summarize_daily_report(snapshot["latest_daily_report"]),
        }

    def sync_broker_fills(self) -> int:
        if not self.alpaca.configured:
            return 0
        order_map = self.db.order_map_by_alpaca_id()
        if not order_map:
            return 0

        start_date = self.db.earliest_order_date()
        if start_date is None:
            return 0

        latest_fill_time = self.db.latest_fill_time()
        if latest_fill_time is not None:
            start_date = latest_fill_time.date()

        today = now_et().date()
        client_lookup = {
            order_id: str(row["client_order_id"])
            for order_id, row in order_map.items()
        }
        inserted_total = 0
        for offset in range((today - start_date).days + 1):
            activity_date = start_date + timedelta(days=offset)
            fills = [
                fill
                for fill in self.alpaca.fill_activities(activity_date)
                if fill.order_id in client_lookup
            ]
            if not fills:
                continue
            inserted_total += self.db.insert_broker_fills(fills, client_lookup)
            for fill in fills:
                status = "FILLED" if fill.fill_type == "fill" else "PARTIALLY_FILLED"
                self.db.update_order_status(
                    client_lookup[fill.order_id],
                    status=status,
                    alpaca_order_id=fill.order_id,
                    filled_at=fill.transaction_time.isoformat(),
                )
        return inserted_total

    def _ledger_summary(self) -> dict[str, Any]:
        fills = self.db.list_broker_fills()
        if not fills:
            return {
                "fill_count": 0,
                "latest_fill_time": None,
                "realized_pnl": 0.0,
                "open_positions": [],
            }

        open_lots: dict[str, list[dict[str, float]]] = {}
        realized_pnl = 0.0
        latest_fill_time = None
        for row in fills:
            symbol = str(row["symbol"])
            side = str(row["side"]).lower()
            qty = float(row["qty"])
            price = float(row["price"])
            latest_fill_time = str(row["transaction_time"])
            lots = open_lots.setdefault(symbol, [])
            remaining = qty
            if side == "buy":
                remaining, pnl_delta = self._close_matching_lots(lots, remaining, price, closing_side="buy")
                if remaining > 0:
                    lots.append({"qty": remaining, "price": price})
            else:
                remaining, pnl_delta = self._close_matching_lots(lots, remaining, price, closing_side="sell")
                if remaining > 0:
                    lots.append({"qty": -remaining, "price": price})
            realized_pnl += pnl_delta

        open_positions = self._open_positions_from_lots(open_lots)
        self._attach_market_values(open_positions)
        return {
            "fill_count": len(fills),
            "latest_fill_time": latest_fill_time,
            "realized_pnl": round(realized_pnl, 2),
            "open_positions": open_positions[:10],
        }

    def _close_matching_lots(
        self,
        lots: list[dict[str, float]],
        qty: float,
        price: float,
        *,
        closing_side: str,
    ) -> tuple[float, float]:
        remaining = qty
        pnl_delta = 0.0
        index = 0
        while remaining > 0 and index < len(lots):
            lot = lots[index]
            lot_qty = float(lot["qty"])
            if closing_side == "buy" and lot_qty >= 0:
                index += 1
                continue
            if closing_side == "sell" and lot_qty <= 0:
                index += 1
                continue
            match_qty = min(remaining, abs(lot_qty))
            if lot_qty > 0:
                pnl_delta += (price - float(lot["price"])) * match_qty
                lot["qty"] = lot_qty - match_qty
            else:
                pnl_delta += (float(lot["price"]) - price) * match_qty
                lot["qty"] = lot_qty + match_qty
            remaining -= match_qty
            if abs(lot["qty"]) < 1e-9:
                lots.pop(index)
            else:
                index += 1
        return remaining, pnl_delta

    def _open_positions_from_lots(self, open_lots: dict[str, list[dict[str, float]]]) -> list[dict[str, Any]]:
        positions: list[dict[str, Any]] = []
        for symbol, lots in sorted(open_lots.items()):
            net_qty = sum(float(lot["qty"]) for lot in lots)
            if abs(net_qty) < 1e-9:
                continue
            side = "long" if net_qty > 0 else "short"
            cost_basis = sum(abs(float(lot["qty"])) * float(lot["price"]) for lot in lots)
            avg_entry_price = cost_basis / abs(net_qty)
            positions.append(
                {
                    "symbol": symbol,
                    "side": side,
                    "qty": abs(net_qty),
                    "avg_entry_price": round(avg_entry_price, 4),
                    "market_value": None,
                    "unrealized_pnl": None,
                }
            )
        positions.sort(key=lambda row: (row["side"], row["symbol"]))
        return positions

    def _attach_market_values(self, positions: list[dict[str, Any]]) -> None:
        if not positions or not self.alpaca.configured:
            return
        quotes = self.alpaca.latest_quotes([str(position["symbol"]) for position in positions])
        for position in positions:
            quote = quotes.get(str(position["symbol"]))
            if quote is None:
                continue
            current_price = quote.last_price or quote.ask_price or quote.bid_price
            qty = float(position["qty"])
            avg_entry = float(position["avg_entry_price"])
            if position["side"] == "long":
                position["market_value"] = round(qty * current_price, 2)
                position["unrealized_pnl"] = round((current_price - avg_entry) * qty, 2)
            else:
                position["market_value"] = round(-qty * current_price, 2)
                position["unrealized_pnl"] = round((avg_entry - current_price) * qty, 2)

    def _summarize_rebalance_report(self, report: dict[str, Any] | None) -> dict[str, Any] | None:
        if not report:
            return None
        targets = report.get("targets")
        skipped_symbols = report.get("skipped_symbols")
        return {
            "rebalance_date": report.get("rebalance_date"),
            "path": report.get("_path"),
            "target_count": len(targets) if isinstance(targets, list) else 0,
            "targets": targets[:5] if isinstance(targets, list) else [],
            "skipped_symbols": skipped_symbols if isinstance(skipped_symbols, list) else [],
        }

    def _summarize_daily_report(self, report: dict[str, Any] | None) -> dict[str, Any] | None:
        if not report:
            return None
        return {
            "as_of": report.get("as_of"),
            "path": report.get("_path"),
            "new_filings": report.get("new_filings"),
            "orders": report.get("orders")[:5] if isinstance(report.get("orders"), list) else [],
            "risk_events": report.get("risk_events")[:5] if isinstance(report.get("risk_events"), list) else [],
            "flagged_filings": (
                report.get("flagged_filings")[:5]
                if isinstance(report.get("flagged_filings"), list)
                else []
            ),
        }

    def ingest_once(self) -> int:
        today = now_et().date()
        last_ingest = self.db.get_runtime_state("last_ingest_date")
        since = (
            date.fromisoformat(last_ingest) - timedelta(days=1)
            if last_ingest
            else today - timedelta(days=self.settings.lookback_days)
        )
        years = [since.year, today.year]
        all_candidates: list[Filing] = []
        source_failures: list[str] = []
        try:
            for entry in self.clerk.list_recent_ptr_index_entries(years, since):
                all_candidates.extend(self.clerk.fetch_ptr_filings(entry))
        except Exception as exc:
            source_failures.append(f"clerk:{exc}")
            self.logger.log("SOURCE_WARNING", source="clerk", error=str(exc))
        try:
            all_candidates.extend(self.quiver.fetch())
        except Exception as exc:
            source_failures.append(f"quiver:{exc}")
            self.logger.log("SOURCE_WARNING", source="quiver", error=str(exc))
        try:
            all_candidates.extend(self.capitol.fetch(since))
        except Exception as exc:
            source_failures.append(f"capitoltrades:{exc}")
            self.logger.log("SOURCE_WARNING", source="capitoltrades", error=str(exc))
        if len(source_failures) == 3:
            self.logger.log("INGEST_HALT", rationale="All sources unavailable.", failures=source_failures)
            raise RuntimeError("All data sources were unavailable during ingestion.")
        validated = self._validate_filings(all_candidates)
        inserted = self.db.insert_filings(validated)
        activation = self._activate_on_fresh_filing()
        self.db.set_runtime_state("last_ingest_date", today.isoformat())
        self.logger.log(
            "FILINGS_INGESTED",
            count=inserted,
            candidate_count=len(all_candidates),
            activated_fresh_filing_strategy=bool(activation),
            rationale=f"{inserted} new filings ingested at {datetime.utcnow().isoformat()}",
        )
        return inserted

    def standby_for_next_filing(self, execute_liquidation: bool = True) -> dict[str, Any]:
        armed_at = now_et()
        anchor_id = self.db.max_filing_id()
        self.db.set_runtime_state("awaiting_fresh_filing", "1")
        self.db.set_runtime_state("fresh_filing_armed_at", armed_at.isoformat())
        self.db.set_runtime_state("fresh_filing_armed_on", armed_at.date().isoformat())
        self.db.set_runtime_state("fresh_filing_anchor_id", str(anchor_id))
        for key in ("fresh_filing_activated_at", "strategy_start_filing_date", "strategy_started_at"):
            self.db.delete_runtime_state(key)
        self.logger.log(
            "FRESH_FILING_STANDBY_ENABLED",
            armed_at=armed_at.isoformat(),
            anchor_id=anchor_id,
            rationale="Ignoring currently known filings until a newly disclosed filing arrives.",
        )
        liquidation = (
            self.liquidate_positions(execute=execute_liquidation, reason="standby_exit")
            if self.alpaca.configured
            else {
                "cancelled_open_orders": 0,
                "positions_found": 0,
                "orders": [],
                "submitted": False,
                "market_open": False,
            }
        )
        return {
            "awaiting_fresh_filing": True,
            "armed_at": armed_at.isoformat(),
            "anchor_filing_id": anchor_id,
            "liquidation": liquidation,
        }

    def liquidate_positions(
        self,
        *,
        execute: bool | None = None,
        reason: str = "manual_exit",
    ) -> dict[str, Any]:
        if not self.alpaca.configured:
            raise RuntimeError("Alpaca credentials are required to liquidate positions.")

        cancelled = self._cancel_open_orders()
        positions = self._positions_for_backend(self.alpaca.positions())
        if not positions:
            return {
                "cancelled_open_orders": cancelled,
                "positions_found": 0,
                "orders": [],
                "submitted": False,
                "market_open": self.alpaca.market_is_open(),
            }

        asset_map = self.alpaca.asset_map([position.symbol for position in positions])
        quotes = self.alpaca.latest_quotes([position.symbol for position in positions])
        orders = self._close_all_positions(positions, quotes, asset_map, reason=reason)
        market_open = self.alpaca.market_is_open()
        should_execute = market_open if execute is None else execute
        submitted = bool(orders) and should_execute and market_open
        if should_execute:
            self._execute_orders(orders, asset_map)
        return {
            "cancelled_open_orders": cancelled,
            "positions_found": len(positions),
            "orders": [{"symbol": order.symbol, "intent": order.intent} for order in orders],
            "submitted": submitted,
            "market_open": market_open,
        }

    def rebalance(self, execute: bool | None = None) -> RebalanceResult:
        if self.db.get_runtime_state("trading_halted") == "1":
            raise RuntimeError("Trading is halted due to a hard drawdown event.")
        if self.db.get_runtime_state("awaiting_fresh_filing") == "1":
            raise RuntimeError(
                "Bot is waiting for the next fresh disclosure and will not trade on existing filings."
            )
        if not self.alpaca.configured:
            raise RuntimeError("Alpaca credentials are required for rebalancing.")
        as_of = now_et().date()
        min_filing_date = self._strategy_start_filing_date()
        latest_filing_date = self.db.latest_filing_date(min_filing_date=min_filing_date)
        if latest_filing_date is None or (as_of - latest_filing_date).days > 7:
            raise RuntimeError("Latest filing data is older than 7 days; refusing to rebalance.")
        account = self.alpaca.account_snapshot()
        rows = self.db.list_active_filings(
            as_of,
            self.settings.lookback_days,
            min_filing_date=min_filing_date,
        )
        targets = construct_targets(rows, account.nav, self.settings, as_of)
        asset_map = self.alpaca.asset_map([target.symbol for target in targets])
        targets, skipped = filter_targets_by_assets(targets, asset_map)
        current_positions = self._positions_for_backend(self.alpaca.positions())
        quotes = self.alpaca.latest_quotes(symbols_for_targets_and_positions(targets, current_positions))
        orders = plan_orders(targets, current_positions, quotes, asset_map, as_of, self.settings.order_prefix)
        orders = scale_new_orders_for_buying_power(orders, account.buying_power)
        if self.db.get_runtime_state("halt_new_entries") == "1":
            orders = [
                order
                for order in orders
                if order.intent not in {"OPEN_LONG", "OPEN_SHORT", "INCREASE_LONG", "INCREASE_SHORT"}
            ]
        should_execute = self.alpaca.market_is_open() if execute is None else execute
        if should_execute:
            self._execute_orders(orders, asset_map)
        self._record_snapshot(account.nav, current_positions, as_of)
        result = RebalanceResult(
            rebalance_date=as_of,
            targets=targets,
            planned_orders=orders,
            skipped_symbols=skipped,
        )
        report_path = write_rebalance_report(result, skipped, self.report_path)
        self.logger.log(
            "REBALANCE_COMPLETE",
            report_path=str(report_path),
            target_count=len(targets),
            order_count=len(orders),
            skipped=skipped,
        )
        return result

    def risk_check(self) -> list[dict[str, str]]:
        if not self.alpaca.configured:
            return []
        account = self.alpaca.account_snapshot()
        positions = self._positions_for_backend(self.alpaca.positions())
        if not positions:
            return []
        peak_nav = max(self.db.peak_nav(), account.nav)
        drawdown = ((peak_nav - account.nav) / peak_nav) if peak_nav else 0.0
        asset_map = self.alpaca.asset_map([position.symbol for position in positions])
        quotes = self.alpaca.latest_quotes([position.symbol for position in positions])
        actions: list[PlannedOrder] = []
        if drawdown > self.settings.max_drawdown_hard:
            self.db.set_runtime_state("trading_halted", "1")
            actions.extend(self._close_all_positions(positions, quotes, asset_map, reason="hard_drawdown"))
            self._record_risk("DRAWDOWN", f"Drawdown {drawdown:.2%}", "Closed all positions and halted trading.")
        elif drawdown > self.settings.max_drawdown_soft:
            self.db.set_runtime_state("halt_new_entries", "1")
            actions.extend(self._reduce_all_positions(positions, quotes, asset_map, 0.5, "soft_drawdown"))
            self._record_risk("DRAWDOWN", f"Drawdown {drawdown:.2%}", "Reduced all positions by 50% and halted new entries.")
        short_book_value = sum(abs(position.market_value) for position in positions if position.side == "short")
        short_book_loss = sum(-min(position.unrealized_pl, 0.0) for position in positions if position.side == "short")
        if short_book_value and short_book_loss > 0.5 * short_book_value:
            shorts = [position for position in positions if position.side == "short"]
            actions.extend(self._close_positions(shorts, quotes, asset_map, "short_book_stop"))
            self._record_risk("SHORT_BOOK", "Short book loss exceeded 50%.", "Closed entire short book.")
        for position in positions:
            if position.side == "long" and position.unrealized_plpc <= -0.10:
                actions.extend(self._close_positions([position], quotes, asset_map, "position_stop"))
                self._record_risk(
                    "POSITION_LOSS",
                    f"{position.symbol} long loss exceeded 10%.",
                    "Closed position immediately.",
                )
            elif position.side == "short" and position.unrealized_plpc <= -0.20:
                actions.extend(self._close_positions([position], quotes, asset_map, "short_stop"))
                self._record_risk(
                    "SHORT_LOSS",
                    f"{position.symbol} short loss exceeded 20%.",
                    "Bought to cover immediately.",
                )
            elif abs(position.market_value) > self.settings.max_position_pct * account.nav:
                actions.extend(self._trim_position(position, account.nav, quotes, asset_map))
                self._record_risk(
                    "CONCENTRATION",
                    f"{position.symbol} exceeded concentration limit.",
                    "Trimmed position to max position cap.",
                )
        deduped: dict[str, PlannedOrder] = {}
        for action in actions:
            deduped[action.client_order_id] = action
        self._execute_orders(sorted(deduped.values(), key=lambda order: order.sequence), asset_map)
        return [{"symbol": order.symbol, "intent": order.intent} for order in deduped.values()]

    def daily_report(self) -> DailySummary:
        if not self.alpaca.configured:
            raise RuntimeError("Alpaca credentials are required for daily reports.")
        as_of = now_et()
        account = self.alpaca.account_snapshot()
        positions = self._positions_for_backend(self.alpaca.positions())
        previous = self.db.latest_snapshot()
        previous_nav = float(previous["nav"]) if previous else account.nav
        long_value = sum(position.market_value for position in positions if position.side == "long")
        short_value = sum(abs(position.market_value) for position in positions if position.side == "short")
        top_longs = [
            {"symbol": position.symbol, "market_value": position.market_value}
            for position in sorted(
                [p for p in positions if p.side == "long"],
                key=lambda p: p.market_value,
                reverse=True,
            )[:5]
        ]
        top_shorts = [
            {"symbol": position.symbol, "market_value": abs(position.market_value)}
            for position in sorted(
                [p for p in positions if p.side == "short"],
                key=lambda p: abs(p.market_value),
                reverse=True,
            )[:5]
        ]
        summary = DailySummary(
            as_of=as_of,
            nav=account.nav,
            daily_pnl=account.nav - previous_nav,
            long_exposure=(long_value / account.nav) if account.nav else 0.0,
            short_exposure=(short_value / account.nav) if account.nav else 0.0,
            net_exposure=((long_value - short_value) / account.nav) if account.nav else 0.0,
            top_longs=top_longs,
            top_shorts=top_shorts,
            new_filings=self.db.count_new_filings(as_of.date()),
            orders=[dict(row) for row in self.db.recent_orders(as_of.date())],
            risk_events=[dict(row) for row in self.db.recent_risk_events(as_of.date())],
            flagged_filings=[dict(row) for row in self.db.list_flagged_filings(as_of.date())],
        )
        path = write_daily_summary(summary, self.report_path)
        self._record_snapshot(account.nav, positions, as_of.date())
        self.logger.log("DAILY_SUMMARY", report_path=str(path), nav=account.nav)
        return summary

    def run(self) -> None:
        while True:
            now = now_et()
            self.ingest_once()
            if self.db.get_runtime_state("awaiting_fresh_filing") == "1":
                self._maintain_fresh_filing_standby()
            else:
                self._maybe_start_strategy_from_fresh_filing(now)
                if now.weekday() == 4 and now.hour == 16:
                    if self.db.get_runtime_state("last_preview_date") != now.date().isoformat():
                        self.rebalance(execute=False)
                        self.db.set_runtime_state("last_preview_date", now.date().isoformat())
                if now.weekday() == 0 and now.hour == 9 and now.minute >= 30:
                    if self.db.get_runtime_state("last_rebalance_date") != now.date().isoformat():
                        self.rebalance(execute=True)
                        self.db.set_runtime_state("last_rebalance_date", now.date().isoformat())
            if now.weekday() < 5 and 9 <= now.hour <= 16:
                self.risk_check()
            if now.weekday() < 5 and now.hour == 16 and now.minute >= 30:
                if self.db.get_runtime_state("last_daily_report") != now.date().isoformat():
                    self.daily_report()
                    self.db.set_runtime_state("last_daily_report", now.date().isoformat())
            sleep(seconds_until_next_poll(self.settings.poll_interval_market, self.settings.poll_interval_off, now))

    def _validate_filings(self, filings: list[Filing]) -> list[Filing]:
        if not self.alpaca.configured:
            return filings
        tradable_symbols = {filing.ticker for filing in filings if filing.ticker and filing.status != "FLAGGED"}
        asset_map: dict[str, AssetInfo] = {}
        for symbol in sorted(tradable_symbols):
            try:
                asset_map[symbol] = self.alpaca.asset(symbol)
            except Exception as exc:
                self.logger.log("TICKER_VALIDATION_FAILED", symbol=symbol, error=str(exc))
        validated: list[Filing] = []
        for filing in filings:
            if filing.status == "FLAGGED" or not filing.ticker:
                validated.append(filing)
                continue
            asset = asset_map.get(filing.ticker)
            if not asset or not asset.tradable or asset.asset_class != "us_equity":
                filing.status = "FLAGGED"
            validated.append(filing)
        return validated

    def _latest_log_record(self, path: Path) -> dict[str, Any] | None:
        records = self._read_log_records(path, limit=1)
        return records[-1] if records else None

    def _strategy_start_filing_date(self) -> date | None:
        raw = self.db.get_runtime_state("strategy_start_filing_date")
        return date.fromisoformat(raw) if raw else None

    def _activate_on_fresh_filing(self) -> date | None:
        if self.db.get_runtime_state("awaiting_fresh_filing") != "1":
            return None

        armed_on_raw = self.db.get_runtime_state("fresh_filing_armed_on")
        if not armed_on_raw:
            return None

        anchor_id = int(self.db.get_runtime_state("fresh_filing_anchor_id", "0") or 0)
        armed_on = date.fromisoformat(armed_on_raw)
        fresh_rows = self.db.filings_after_id(anchor_id, min_filing_date=armed_on)
        if not fresh_rows:
            return None

        start_date = min(date.fromisoformat(str(row["filing_date"])) for row in fresh_rows)
        activated_at = now_et().isoformat()
        self.db.set_runtime_state("awaiting_fresh_filing", "0")
        self.db.set_runtime_state("fresh_filing_activated_at", activated_at)
        self.db.set_runtime_state("strategy_start_filing_date", start_date.isoformat())
        self.db.delete_runtime_state("strategy_started_at")
        self.logger.log(
            "FRESH_FILING_TRIGGERED",
            activated_at=activated_at,
            start_filing_date=start_date.isoformat(),
            filing_count=len(fresh_rows),
            rationale="A new disclosure arrived after standby was armed; strategy may now trade fresh filings.",
        )
        return start_date

    def _maybe_start_strategy_from_fresh_filing(self, now: datetime) -> None:
        start_filing_date = self._strategy_start_filing_date()
        if start_filing_date is None or self.db.get_runtime_state("strategy_started_at"):
            return
        if not self.alpaca.configured:
            return
        if now.weekday() >= 5 or not (9 <= now.hour <= 16):
            return
        if not self.alpaca.market_is_open():
            return
        self.rebalance(execute=True)
        self.db.set_runtime_state("strategy_started_at", now.isoformat())
        self.db.set_runtime_state("last_rebalance_date", now.date().isoformat())
        self.logger.log(
            "FRESH_FILING_STRATEGY_STARTED",
            started_at=now.isoformat(),
            start_filing_date=start_filing_date.isoformat(),
        )

    def _maintain_fresh_filing_standby(self) -> None:
        if not self.alpaca.configured:
            return
        try:
            summary = self.liquidate_positions(execute=True, reason="standby_exit")
        except Exception as exc:
            self.logger.log("STANDBY_LIQUIDATION_WARNING", error=str(exc))
            return
        if summary["cancelled_open_orders"] or summary["orders"]:
            self.logger.log(
                "STANDBY_LIQUIDATION_CHECK",
                cancelled_open_orders=summary["cancelled_open_orders"],
                positions_found=summary["positions_found"],
                submitted=summary["submitted"],
            )

    def _read_log_records(self, path: Path, limit: int) -> list[dict[str, Any]]:
        if not path.exists():
            return []

        lines: deque[str] = deque(maxlen=limit)
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if stripped:
                    lines.append(stripped)

        records: list[dict[str, Any]] = []
        for line in lines:
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                records.append({"raw": line})
                continue
            records.append(payload if isinstance(payload, dict) else {"raw": line})
        return records

    def _snapshot_to_dict(self, row: Any) -> dict[str, Any]:
        payload = self._row_to_dict(row)
        if not payload:
            return {}
        try:
            positions = json.loads(str(payload.get("positions_json") or "[]"))
        except json.JSONDecodeError:
            positions = []
        payload["positions_json"] = positions
        return payload

    def _latest_positions(self, row: Any) -> list[dict[str, Any]]:
        if row is None:
            return []
        snapshot = self._snapshot_to_dict(row)
        positions = snapshot.get("positions_json", [])
        if not isinstance(positions, list):
            return []
        ranked = sorted(
            [position for position in positions if isinstance(position, dict)],
            key=lambda position: abs(float(position.get("market_value", 0.0))),
            reverse=True,
        )
        return ranked[:10]

    def _positions_for_backend(self, positions: list[Any]) -> list[Any]:
        tracked_symbols = self._tracked_symbols()
        if not tracked_symbols:
            return []
        return [position for position in positions if getattr(position, "symbol", "") in tracked_symbols]

    def _tracked_symbols(self) -> set[str]:
        rows = self.db.list_orders(limit=500)
        allowed_statuses = {
            "PENDING",
            "NEW",
            "ACCEPTED",
            "HELD",
            "PARTIALLY_FILLED",
            "FILLED",
        }
        return {
            str(row["symbol"])
            for row in rows
            if str(row["status"]).upper() in allowed_statuses and row["symbol"]
        }

    def _row_to_dict(self, row: Any) -> dict[str, Any] | None:
        if row is None:
            return None
        return {key: row[key] for key in row.keys()}

    def _latest_report_payload(self, report_name: str) -> dict[str, Any] | None:
        candidates = sorted(self.report_path.glob(f"{report_name}-*.json"))
        if not candidates:
            return None
        path = candidates[-1]
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"path": str(path), "error": "unreadable_report"}

        if isinstance(payload, dict):
            payload["_path"] = str(path)
            return payload
        return {"path": str(path), "raw": payload}

    def _execute_orders(self, orders: list[PlannedOrder], asset_map: dict[str, AssetInfo]) -> None:
        if not orders:
            return
        if not self.alpaca.market_is_open():
            self.logger.log("EXECUTION_SKIPPED", rationale="Market is closed.", order_count=len(orders))
            return
        gross_target = self.settings.long_exposure + self.settings.short_exposure
        if gross_target > 1.70:
            raise RuntimeError("Configured gross exposure exceeds strategy hard cap.")
        for order in sorted(orders, key=lambda row: row.sequence):
            if self.db.get_order(order.client_order_id):
                continue
            asset = asset_map.get(order.symbol)
            if not asset or not asset.tradable:
                self.logger.log("ORDER_SKIPPED", symbol=order.symbol, rationale="Asset not tradable.")
                continue
            if order.intent in {"OPEN_SHORT", "INCREASE_SHORT"} and not (asset.shortable and asset.easy_to_borrow):
                self.logger.log("ORDER_SKIPPED", symbol=order.symbol, rationale="Asset not easy to borrow.")
                continue
            self.db.record_order(order)
            try:
                response = self.alpaca.submit_order(order)
                self.db.update_order_status(
                    order.client_order_id,
                    status=str(response.get("status", "PENDING")).upper(),
                    alpaca_order_id=response.get("id"),
                    filled_at=response.get("filled_at"),
                )
                self.logger.log(
                    "ORDER_PLACED",
                    symbol=order.symbol,
                    side=order.side,
                    qty=order.qty,
                    limit_price=order.limit_price,
                    rationale=order.rationale,
                )
            except Exception as exc:
                self.db.update_order_status(order.client_order_id, status="REJECTED")
                self.logger.log("ORDER_REJECTED", symbol=order.symbol, error=str(exc), rationale=order.rationale)

    def _cancel_open_orders(self) -> int:
        cancelled = 0
        for order in self.alpaca.open_orders(order_prefix=self.settings.order_prefix):
            order_id = str(order.get("id") or "")
            if not order_id:
                continue
            client_order_id = str(order.get("client_order_id") or "")
            try:
                self.alpaca.cancel_order(order_id)
                cancelled += 1
                if client_order_id:
                    self.db.update_order_status(client_order_id, status="CANCELLED")
            except Exception as exc:
                self.logger.log(
                    "ORDER_CANCEL_WARNING",
                    order_id=order_id,
                    client_order_id=client_order_id,
                    error=str(exc),
                )
        return cancelled

    def _record_snapshot(self, nav: float, positions: list[Any], snapshot_date: date) -> None:
        long_value = sum(position.market_value for position in positions if position.side == "long")
        short_value = sum(abs(position.market_value) for position in positions if position.side == "short")
        self.db.record_snapshot(
            snapshot_date=snapshot_date,
            nav=nav,
            long_exposure=(long_value / nav) if nav else 0.0,
            short_exposure=(short_value / nav) if nav else 0.0,
            net_exposure=((long_value - short_value) / nav) if nav else 0.0,
            positions=[asdict(position) for position in positions],
        )

    def _record_risk(self, event_type: str, details: str, action_taken: str) -> None:
        self.db.record_risk_event(event_type, details, action_taken)
        self.logger.log("RISK_EVENT", event_type=event_type, details=details, action_taken=action_taken)

    def _close_all_positions(
        self,
        positions: list[Any],
        quotes: dict[str, Any],
        asset_map: dict[str, AssetInfo],
        reason: str,
    ) -> list[PlannedOrder]:
        return self._close_positions(positions, quotes, asset_map, reason)

    def _close_positions(
        self,
        positions: list[Any],
        quotes: dict[str, Any],
        asset_map: dict[str, AssetInfo],
        reason: str,
    ) -> list[PlannedOrder]:
        orders: list[PlannedOrder] = []
        today = now_et().date()
        for sequence, position in enumerate(positions, start=1):
            quote = quotes.get(position.symbol)
            asset = asset_map.get(position.symbol)
            if not quote or not asset:
                continue
            side = "sell" if position.side == "long" else "buy"
            limit_price = (quote.bid_price if side == "sell" else quote.ask_price) or quote.last_price
            limit_price *= 0.999 if side == "sell" else 1.001
            orders.append(
                PlannedOrder(
                    symbol=position.symbol,
                    side=side,
                    qty=abs(position.qty),
                    limit_price=limit_price,
                    rationale=f"{reason} protective exit.",
            client_order_id=f"{self.settings.order_prefix}-risk-{today:%Y%m%d}-{position.symbol}-{reason}-{side}",
                    rebalance_date=today,
                    sequence=sequence,
                    intent=reason.upper(),
                )
            )
        return orders

    def _reduce_all_positions(
        self,
        positions: list[Any],
        quotes: dict[str, Any],
        asset_map: dict[str, AssetInfo],
        fraction: float,
        reason: str,
    ) -> list[PlannedOrder]:
        orders: list[PlannedOrder] = []
        today = now_et().date()
        for sequence, position in enumerate(positions, start=1):
            quote = quotes.get(position.symbol)
            asset = asset_map.get(position.symbol)
            if not quote or not asset:
                continue
            side = "sell" if position.side == "long" else "buy"
            limit_price = (quote.bid_price if side == "sell" else quote.ask_price) or quote.last_price
            limit_price *= 0.999 if side == "sell" else 1.001
            orders.append(
                PlannedOrder(
                    symbol=position.symbol,
                    side=side,
                    qty=abs(position.qty) * fraction,
                    limit_price=limit_price,
                    rationale=f"{reason} de-risking order.",
                    client_order_id=f"{self.settings.order_prefix}-risk-{today:%Y%m%d}-{position.symbol}-{reason}-{side}",
                    rebalance_date=today,
                    sequence=sequence,
                    intent=reason.upper(),
                )
            )
        return orders

    def _trim_position(
        self,
        position: Any,
        nav: float,
        quotes: dict[str, Any],
        asset_map: dict[str, AssetInfo],
    ) -> list[PlannedOrder]:
        excess_notional = abs(position.market_value) - (self.settings.max_position_pct * nav)
        if excess_notional <= 0:
            return []
        quote = quotes.get(position.symbol)
        asset = asset_map.get(position.symbol)
        if not quote or not asset:
            return []
        side = "sell" if position.side == "long" else "buy"
        price = (quote.bid_price if side == "sell" else quote.ask_price) or quote.last_price
        price *= 0.999 if side == "sell" else 1.001
        qty = excess_notional / price
        today = now_et().date()
        return [
            PlannedOrder(
                symbol=position.symbol,
                side=side,
                qty=qty,
                limit_price=price,
                rationale="Concentration trim to max position cap.",
                client_order_id=f"{self.settings.order_prefix}-risk-{today:%Y%m%d}-{position.symbol}-trim-{side}",
                rebalance_date=today,
                sequence=5,
                intent="TRIM",
            )
        ]
