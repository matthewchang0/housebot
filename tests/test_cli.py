from __future__ import annotations

import sys
import json

from house.cli import main


def test_alpaca_check_prints_connection_summary(
    monkeypatch,
    capsys,
) -> None:
    class FakeBot:
        def __init__(self) -> None:
            self.closed = False

        def alpaca_check(self) -> dict[str, object]:
            return {
                "configured": True,
                "mode": "PAPER",
                "base_url": "https://paper-api.alpaca.markets",
                "account": {
                    "nav": 100_000.0,
                    "buying_power": 80_000.0,
                    "equity": 100_000.0,
                    "cash": 20_000.0,
                },
                "market_open": True,
            }

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr("house.cli.HouseBot", FakeBot)
    monkeypatch.setattr(sys, "argv", ["house", "alpaca-check"])

    main()

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["configured"] is True
    assert payload["mode"] == "PAPER"
    assert payload["account"]["nav"] == 100_000.0
    assert payload["market_open"] is True


def test_status_prints_local_runtime_summary(
    monkeypatch,
    capsys,
) -> None:
    class FakeBot:
        def status(self) -> dict[str, object]:
            return {
                "mode": "PAPER",
                "alpaca_configured": True,
                "paths": {
                    "db": "/tmp/house.db",
                    "log": "/tmp/house.jsonl",
                },
                "db_exists": True,
                "log_exists": True,
                "latest_filing_date": "2026-04-14",
                "latest_snapshot": {
                    "snapshot_date": "2026-04-14",
                    "nav": 100_000.0,
                    "long_exposure": 1.2,
                    "short_exposure": 0.2,
                    "net_exposure": 1.0,
                },
                "runtime_state": {
                    "last_ingest_date": "2026-04-14",
                    "last_preview_date": None,
                    "last_rebalance_date": "2026-04-14",
                    "last_daily_report": None,
                    "trading_halted": None,
                    "halt_new_entries": None,
                },
                "latest_log": {
                    "timestamp": "2026-04-15T01:33:25Z",
                    "event": "FILINGS_INGESTED",
                },
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr("house.cli.HouseBot", FakeBot)
    monkeypatch.setattr(sys, "argv", ["house", "status"])

    main()

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["mode"] == "PAPER"
    assert payload["alpaca_configured"] is True
    assert payload["latest_snapshot"]["nav"] == 100_000.0
    assert payload["latest_log"]["event"] == "FILINGS_INGESTED"


def test_dashboard_invokes_server(
    monkeypatch,
) -> None:
    class FakeBot:
        def close(self) -> None:
            return None

    calls: list[tuple[str, int]] = []

    def fake_server(*, host: str, port: int) -> None:
        calls.append((host, port))

    monkeypatch.setattr("house.cli.HouseBot", FakeBot)
    monkeypatch.setattr("house.cli.serve_dashboard", fake_server)
    monkeypatch.setattr(sys, "argv", ["house", "dashboard", "--host", "0.0.0.0", "--port", "9000"])

    main()

    assert calls == [("0.0.0.0", 9000)]


def test_standby_arms_fresh_filing_mode(
    monkeypatch,
    capsys,
) -> None:
    class FakeBot:
        def standby_for_next_filing(self, *, execute_liquidation: bool) -> dict[str, object]:
            return {
                "awaiting_fresh_filing": True,
                "armed_at": "2026-04-14T12:00:00-04:00",
                "anchor_filing_id": 123,
                "liquidation": {
                    "cancelled_open_orders": 0,
                    "positions_found": 0,
                    "orders": [],
                    "submitted": False,
                    "market_open": False,
                },
                "execute_liquidation": execute_liquidation,
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr("house.cli.HouseBot", FakeBot)
    monkeypatch.setattr(sys, "argv", ["house", "standby", "--no-liquidate"])

    main()

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["awaiting_fresh_filing"] is True
    assert payload["anchor_filing_id"] == 123
    assert payload["execute_liquidation"] is False


def test_ai_brief_prints_summary(
    monkeypatch,
    capsys,
) -> None:
    class FakeBot:
        def ai_brief(self, *, focus: str | None = None) -> dict[str, object]:
            return {
                "created_at": "2026-04-14T20:00:00-04:00",
                "model": "claude-sonnet-4-20250514",
                "report_path": "/tmp/ai-brief.md",
                "brief": f"focus={focus}",
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr("house.cli.HouseBot", FakeBot)
    monkeypatch.setattr(sys, "argv", ["house", "ai-brief", "--focus", "risk"])

    main()

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["model"] == "claude-sonnet-4-20250514"
    assert payload["brief"] == "focus=risk"


def test_sync_fills_prints_ledger_summary(
    monkeypatch,
    capsys,
) -> None:
    class FakeBot:
        def sync_broker_fills(self) -> int:
            return 2

        def _ledger_summary(self) -> dict[str, object]:
            return {
                "fill_count": 2,
                "latest_fill_time": "2026-04-14T15:00:00+00:00",
                "realized_pnl": 40.0,
                "open_positions": [],
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr("house.cli.HouseBot", FakeBot)
    monkeypatch.setattr(sys, "argv", ["house", "sync-fills"])

    main()

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["new_fills"] == 2
    assert payload["ledger"]["realized_pnl"] == 40.0
