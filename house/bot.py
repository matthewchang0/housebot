from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timedelta
from time import sleep
from typing import Any

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
from .reports import write_daily_summary, write_rebalance_report
from .sources import CapitolTradesClient, ClerkClient, QuiverClient
from .utils import now_et, seconds_until_next_poll


class HouseBot:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings.load()
        self.logger = JsonLogger(self.settings.log_path)
        self.db = Database(self.settings.db_path)
        self.http = HttpClient(self.settings.user_agent)
        self.alpaca = AlpacaClient(self.settings, self.http)
        self.clerk = ClerkClient(self.http)
        self.quiver = QuiverClient(self.http, self.settings.quiver_api_key)
        self.capitol = CapitolTradesClient(self.http)

    def close(self) -> None:
        self.http.close()
        self.db.close()

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
            all_candidates.extend(self.capitol.fetch())
        except Exception as exc:
            source_failures.append(f"capitoltrades:{exc}")
            self.logger.log("SOURCE_WARNING", source="capitoltrades", error=str(exc))
        if len(source_failures) == 3:
            self.logger.log("INGEST_HALT", rationale="All sources unavailable.", failures=source_failures)
            raise RuntimeError("All data sources were unavailable during ingestion.")
        validated = self._validate_filings(all_candidates)
        inserted = self.db.insert_filings(validated)
        self.db.set_runtime_state("last_ingest_date", today.isoformat())
        self.logger.log(
            "FILINGS_INGESTED",
            count=inserted,
            candidate_count=len(all_candidates),
            rationale=f"{inserted} new filings ingested at {datetime.utcnow().isoformat()}",
        )
        return inserted

    def rebalance(self, execute: bool | None = None) -> RebalanceResult:
        if self.db.get_runtime_state("trading_halted") == "1":
            raise RuntimeError("Trading is halted due to a hard drawdown event.")
        if not self.alpaca.configured:
            raise RuntimeError("Alpaca credentials are required for rebalancing.")
        as_of = now_et().date()
        latest_filing_date = self.db.latest_filing_date()
        if latest_filing_date is None or (as_of - latest_filing_date).days > 7:
            raise RuntimeError("Latest filing data is older than 7 days; refusing to rebalance.")
        account = self.alpaca.account_snapshot()
        rows = self.db.list_active_filings(as_of, self.settings.lookback_days)
        targets = construct_targets(rows, account.nav, self.settings, as_of)
        asset_map = self.alpaca.asset_map([target.symbol for target in targets])
        targets, skipped = filter_targets_by_assets(targets, asset_map)
        current_positions = self.alpaca.positions()
        quotes = self.alpaca.latest_quotes(symbols_for_targets_and_positions(targets, current_positions))
        orders = plan_orders(targets, current_positions, quotes, asset_map, as_of)
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
        report_path = write_rebalance_report(result, skipped)
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
        positions = self.alpaca.positions()
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
        positions = self.alpaca.positions()
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
        path = write_daily_summary(summary)
        self._record_snapshot(account.nav, positions, as_of.date())
        self.logger.log("DAILY_SUMMARY", report_path=str(path), nav=account.nav)
        return summary

    def run(self) -> None:
        while True:
            now = now_et()
            self.ingest_once()
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
                    client_order_id=f"house-risk-{today:%Y%m%d}-{position.symbol}-{reason}-{side}",
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
                    client_order_id=f"house-risk-{today:%Y%m%d}-{position.symbol}-{reason}-{side}",
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
                client_order_id=f"house-risk-{today:%Y%m%d}-{position.symbol}-trim-{side}",
                rebalance_date=today,
                sequence=5,
                intent="TRIM",
            )
        ]
