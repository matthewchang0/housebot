from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import app as vercel_app
from house.bot import HouseBot
from house.config import Settings
from house.dashboard import DashboardApp
from house.db import Database


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


def _call_app(path: str, method: str = "GET") -> tuple[str, dict[str, str], bytes]:
    response_status = ""
    response_headers: list[tuple[str, str]] = []

    def start_response(status: str, headers: list[tuple[str, str]]) -> None:
        nonlocal response_status, response_headers
        response_status = status
        response_headers = headers

    body = b"".join(
        vercel_app.app(
            {
                "REQUEST_METHOD": method,
                "PATH_INFO": path,
            },
            start_response,
        )
    )
    return response_status, dict(response_headers), body


def test_vercel_entrypoint_serves_dashboard_routes(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    settings = _settings(tmp_path)
    database = Database(settings.db_path)
    database.set_runtime_state("last_ingest_date", "2026-04-14")
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
            json.dumps([]),
        ),
    )
    database._conn.commit()
    database.close()

    settings.log_path.parent.mkdir(parents=True, exist_ok=True)
    settings.log_path.write_text("", encoding="utf-8")
    settings.report_path.mkdir(parents=True, exist_ok=True)

    bot = HouseBot(settings=settings)
    monkeypatch.setattr(vercel_app, "_get_dashboard_app", lambda: DashboardApp(bot=bot))

    try:
        status, headers, body = _call_app("/")
        assert status == "200 OK"
        assert headers["Content-Type"] == "text/html; charset=utf-8"
        assert b"House Control Room" in body

        status, headers, body = _call_app("/api/dashboard")
        assert status == "200 OK"
        assert headers["Content-Type"] == "application/json; charset=utf-8"
        payload = json.loads(body)
        assert payload["status"]["runtime_state"]["last_ingest_date"] == "2026-04-14"
    finally:
        bot.close()


def test_vercel_entrypoint_proxies_to_upstream(monkeypatch) -> None:
    monkeypatch.setenv("DASHBOARD_UPSTREAM_URL", "https://house.example.com")
    monkeypatch.setenv("DASHBOARD_UPSTREAM_TOKEN", "shared-secret")

    def fake_get(url: str, **kwargs: object) -> SimpleNamespace:
        assert url == "https://house.example.com/api/dashboard"
        assert kwargs["headers"] == {"Authorization": "Bearer shared-secret"}
        return SimpleNamespace(
            status_code=200,
            reason_phrase="OK",
            content=json.dumps({"status": {"mode": "PAPER"}}).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
        )

    monkeypatch.setattr(vercel_app.httpx, "get", fake_get)

    status, headers, body = _call_app("/api/dashboard")

    assert status == "200 OK"
    assert headers["Content-Type"] == "application/json; charset=utf-8"
    assert json.loads(body)["status"]["mode"] == "PAPER"
