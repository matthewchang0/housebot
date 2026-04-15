from __future__ import annotations

import json
from pathlib import Path

from house.bot import HouseBot
from house.config import Settings
from house.db import Database
from house.dashboard import DashboardApp, _is_authorized


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


def test_dashboard_payload_collects_local_state(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    settings = _settings(tmp_path)
    database = Database(settings.db_path)
    database.set_runtime_state("last_ingest_date", "2026-04-14")
    database._conn.execute(
        """
        INSERT INTO filings (
            member_name, relation, ticker, direction, tx_date, filing_date,
            amount_range, amount_midpoint, committee, asset_type, context_score,
            status, source, raw_text
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "Rep Example",
            "Self",
            "AAPL",
            "PURCHASE",
            "2026-04-12",
            "2026-04-13",
            "$1,001 - $15,000",
            8000.0,
            "Ways and Means",
            "Stock",
            1.0,
            "ACTIVE",
            "clerk",
            "Example raw text",
        ),
    )
    database._conn.execute(
        """
        INSERT INTO portfolio_snapshots (
            snapshot_date, nav, long_exposure, short_exposure, net_exposure, positions_json
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            "2026-04-14",
            100000.0,
            1.1,
            0.2,
            0.9,
            json.dumps(
                [
                    {
                        "symbol": "AAPL",
                        "side": "long",
                        "market_value": 12000.0,
                        "unrealized_pl": 400.0,
                    }
                ]
            ),
        ),
    )
    database._conn.execute(
        """
        INSERT INTO orders (
            client_order_id, symbol, side, qty, limit_price, status, rebalance_date, rationale
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("house-1", "AAPL", "buy", 10.0, 180.0, "PENDING", "2026-04-14", "test"),
    )
    database._conn.execute(
        "INSERT INTO risk_events (event_type, details, action_taken) VALUES (?, ?, ?)",
        ("DRAWDOWN", "Test drawdown", "Reduced exposure"),
    )
    database._conn.commit()
    database.close()

    settings.log_path.parent.mkdir(parents=True, exist_ok=True)
    settings.log_path.write_text(
        json.dumps({"timestamp": "2026-04-15T01:33:25Z", "event": "FILINGS_INGESTED"}) + "\n",
        encoding="utf-8",
    )
    reports_dir = tmp_path / "reports" / "house"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "rebalance-2026-04-14.json").write_text(
        json.dumps({"rebalance_date": "2026-04-14", "targets": [], "planned_orders": []}),
        encoding="utf-8",
    )

    bot = HouseBot(settings=settings)
    app = DashboardApp(bot=bot)

    payload = app.dashboard_payload()

    bot.close()

    assert payload["status"]["mode"] == "PAPER"
    assert payload["status"]["runtime_state"]["last_ingest_date"] == "2026-04-14"
    assert payload["recent_orders"][0]["symbol"] == "AAPL"
    assert payload["recent_filings"][0]["ticker"] == "AAPL"
    assert payload["latest_positions"][0]["symbol"] == "AAPL"
    assert payload["recent_logs"][0]["event"] == "FILINGS_INGESTED"
    assert payload["latest_rebalance_report"]["rebalance_date"] == "2026-04-14"


def test_dashboard_authorization_token(monkeypatch) -> None:
    monkeypatch.delenv("DASHBOARD_BEARER_TOKEN", raising=False)
    assert _is_authorized(None)

    monkeypatch.setenv("DASHBOARD_BEARER_TOKEN", "secret-token")
    assert not _is_authorized(None)
    assert not _is_authorized("Bearer wrong-token")
    assert _is_authorized("Bearer secret-token")
