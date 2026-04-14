from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from house.bot import HouseBot
from house.config import Settings
from house.db import Database
from house.models import BrokerFill, Filing, MarketQuote, Position


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        backend_id="house",
        alpaca_api_key="",
        alpaca_secret_key="",
        alpaca_base_url="https://paper-api.alpaca.markets",
        alpaca_data_base_url="https://data.alpaca.markets",
        quiver_api_key="",
        anthropic_api_key="",
        anthropic_base_url="https://api.anthropic.com",
        anthropic_model="claude-sonnet-4-20250514",
        anthropic_version="2023-06-01",
        mode="PAPER",
        lookback_days=90,
        long_exposure=1.30,
        short_exposure=0.30,
        max_position_pct=0.15,
        max_drawdown_soft=0.15,
        max_drawdown_hard=0.25,
        log_path=tmp_path / "logs" / "house.jsonl",
        db_path=tmp_path / "data" / "house.db",
        poll_interval_market=900,
        poll_interval_off=3600,
        max_long_positions=50,
        max_short_positions=20,
        min_position_size=500.0,
        user_agent="NancyBot/0.1",
        poor_accuracy_members=(),
        report_path=tmp_path / "reports" / "house",
    )


def _filing(*, filing_date: date, ticker: str = "AAPL") -> Filing:
    return Filing(
        member_name="Rep Example",
        relation="Self",
        ticker=ticker,
        direction="PURCHASE",
        tx_date=filing_date,
        filing_date=filing_date,
        amount_range="$1,001 - $15,000",
        amount_midpoint=8000.0,
        committee="Ways and Means",
        asset_type="Stock",
        source="clerk",
        raw_text=f"{ticker} sample",
    )


def test_standby_arms_until_a_fresh_filing_arrives(tmp_path) -> None:
    bot = HouseBot(settings=_settings(tmp_path))
    try:
        bot.db.insert_filings([_filing(filing_date=date(2026, 4, 10))])

        payload = bot.standby_for_next_filing(execute_liquidation=False)

        assert payload["awaiting_fresh_filing"] is True
        assert bot.db.get_runtime_state("awaiting_fresh_filing") == "1"
        assert bot.db.get_runtime_state("strategy_start_filing_date") is None

        bot.db.insert_filings([_filing(filing_date=date(2026, 4, 15), ticker="MSFT")])
        activated = bot._activate_on_fresh_filing()

        assert activated == date(2026, 4, 15)
        assert bot.db.get_runtime_state("awaiting_fresh_filing") == "0"
        assert bot.db.get_runtime_state("strategy_start_filing_date") == "2026-04-15"
    finally:
        bot.close()


def test_rebalance_refuses_while_waiting_for_fresh_filing(tmp_path) -> None:
    bot = HouseBot(settings=_settings(tmp_path))
    try:
        bot.db.set_runtime_state("awaiting_fresh_filing", "1")

        class FakeAlpaca:
            configured = True

        bot.alpaca = FakeAlpaca()  # type: ignore[assignment]

        with pytest.raises(RuntimeError, match="waiting for the next fresh disclosure"):
            bot.rebalance(execute=False)
    finally:
        bot.close()


def test_list_active_filings_can_ignore_old_disclosures(tmp_path) -> None:
    database = Database(tmp_path / "house.db")
    try:
        database.insert_filings(
            [
                _filing(filing_date=date(2026, 4, 10), ticker="AAPL"),
                _filing(filing_date=date(2026, 4, 15), ticker="MSFT"),
            ]
        )

        rows = database.list_active_filings(
            as_of=date(2026, 4, 16),
            lookback_days=90,
            min_filing_date=date(2026, 4, 15),
        )

        assert [row["ticker"] for row in rows] == ["MSFT"]
    finally:
        database.close()


def test_ai_brief_writes_report(tmp_path) -> None:
    bot = HouseBot(settings=_settings(tmp_path))
    try:
        seen_payload: dict[str, object] = {}

        class FakeAI:
            configured = True

            def operator_brief(self, payload: dict[str, object], focus: str | None = None) -> str:
                seen_payload.update(payload)
                assert focus == "risk"
                assert "status" in payload
                return "Summary\nSystem is healthy.\n\nRisks\nNo urgent risks.\n\nNext checks\nWatch filings."

        bot.ai = FakeAI()  # type: ignore[assignment]

        result = bot.ai_brief(focus="risk")

        report_path = Path(result["report_path"])
        assert report_path.exists()
        assert result["brief"].startswith("Summary")
        assert report_path.read_text(encoding="utf-8").startswith("Summary")
        assert "latest_positions" not in seen_payload
        assert seen_payload["scope_note"]
        status = seen_payload["status"]
        assert isinstance(status, dict)
        assert "latest_snapshot" not in status
    finally:
        bot.close()


def test_positions_for_backend_filters_untracked_symbols(tmp_path) -> None:
    bot = HouseBot(settings=_settings(tmp_path))
    try:
        bot.db._conn.execute(
            """
            INSERT INTO orders (
                client_order_id, symbol, side, qty, limit_price, status, rebalance_date, rationale
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("house-20260414-60-AAPL-buy", "AAPL", "buy", 1.0, 180.0, "FILLED", "2026-04-14", "test"),
        )
        bot.db._conn.commit()

        positions = [
            Position("AAPL", 1.0, 180.0, 180.0, "long", 0.0, 0.0),
            Position("MSFT", 2.0, 800.0, 400.0, "long", 0.0, 0.0),
        ]

        filtered = bot._positions_for_backend(positions)

        assert [position.symbol for position in filtered] == ["AAPL"]
    finally:
        bot.close()


def test_sync_broker_fills_builds_house_only_ledger(tmp_path) -> None:
    bot = HouseBot(settings=_settings(tmp_path))
    try:
        bot.db._conn.execute(
            """
            INSERT INTO orders (
                client_order_id, alpaca_order_id, symbol, side, qty, limit_price, status, rebalance_date, rationale
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "house-20260414-60-AAPL-buy",
                "alpaca-order-1",
                "AAPL",
                "buy",
                10.0,
                100.0,
                "ACCEPTED",
                "2026-04-14",
                "test",
            ),
        )
        bot.db._conn.commit()

        class FakeAlpaca:
            configured = True

            def fill_activities(self, activity_date: date) -> list[BrokerFill]:
                if activity_date != date(2026, 4, 14):
                    return []
                return [
                    BrokerFill(
                        activity_id="fill-1",
                        order_id="alpaca-order-1",
                        symbol="AAPL",
                        side="buy",
                        qty=10.0,
                        price=100.0,
                        transaction_time=datetime(2026, 4, 14, 14, 0, tzinfo=timezone.utc),
                        activity_type="FILL",
                        fill_type="fill",
                    ),
                    BrokerFill(
                        activity_id="fill-2",
                        order_id="alpaca-order-1",
                        symbol="AAPL",
                        side="sell",
                        qty=4.0,
                        price=110.0,
                        transaction_time=datetime(2026, 4, 14, 15, 0, tzinfo=timezone.utc),
                        activity_type="FILL",
                        fill_type="fill",
                    ),
                    BrokerFill(
                        activity_id="fill-foreign",
                        order_id="other-agent-order",
                        symbol="MSFT",
                        side="buy",
                        qty=3.0,
                        price=50.0,
                        transaction_time=datetime(2026, 4, 14, 16, 0, tzinfo=timezone.utc),
                        activity_type="FILL",
                        fill_type="fill",
                    ),
                ]

            def latest_quotes(self, symbols: list[str]) -> dict[str, MarketQuote]:
                return {
                    "AAPL": MarketQuote(
                        symbol="AAPL",
                        bid_price=120.0,
                        ask_price=120.0,
                        last_price=120.0,
                    )
                }

        bot.alpaca = FakeAlpaca()  # type: ignore[assignment]

        inserted = bot.sync_broker_fills()
        ledger = bot._ledger_summary()

        assert inserted == 2
        assert ledger["fill_count"] == 2
        assert ledger["realized_pnl"] == 40.0
        assert ledger["open_positions"][0]["symbol"] == "AAPL"
        assert ledger["open_positions"][0]["qty"] == 6.0
        assert ledger["open_positions"][0]["avg_entry_price"] == 100.0
        assert ledger["open_positions"][0]["unrealized_pnl"] == 120.0
    finally:
        bot.close()
