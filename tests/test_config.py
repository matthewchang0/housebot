from __future__ import annotations

import tempfile
from pathlib import Path

from house.config import Settings


def test_settings_load_reads_local_dotenv(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "ALPACA_API_KEY=test-key",
                "ALPACA_SECRET_KEY=test-secret",
                "MODE=LIVE",
                "USER_AGENT=TestBot/1.0",
            ]
        )
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
    monkeypatch.delenv("MODE", raising=False)
    monkeypatch.delenv("USER_AGENT", raising=False)

    settings = Settings.load()

    assert settings.alpaca_api_key == "test-key"
    assert settings.alpaca_secret_key == "test-secret"
    assert settings.mode == "LIVE"
    assert settings.alpaca_base_url == "https://api.alpaca.markets"
    assert settings.user_agent == "TestBot/1.0"
    assert settings.backend_id == "house"
    assert settings.db_path == Path("./data/house/filings.db")
    assert settings.report_path == Path("./reports/house")


def test_settings_load_prefers_existing_environment_over_dotenv(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("ALPACA_API_KEY=file-key\nALPACA_SECRET_KEY=file-secret\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ALPACA_API_KEY", "exported-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "exported-secret")

    settings = Settings.load()

    assert settings.alpaca_api_key == "exported-key"
    assert settings.alpaca_secret_key == "exported-secret"


def test_settings_load_uses_backend_scoped_defaults(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("BACKEND_ID=house-bot\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BACKEND_ID", raising=False)
    monkeypatch.delenv("DB_PATH", raising=False)
    monkeypatch.delenv("LOG_PATH", raising=False)
    monkeypatch.delenv("REPORT_PATH", raising=False)

    settings = Settings.load()

    assert settings.backend_id == "house-bot"
    assert settings.db_path == Path("./data/house-bot/filings.db")
    assert settings.log_path == Path("./logs/house-bot.jsonl")
    assert settings.report_path == Path("./reports/house-bot")


def test_settings_load_uses_tmp_storage_on_vercel(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("BACKEND_ID", "house-bot")
    monkeypatch.delenv("DB_PATH", raising=False)
    monkeypatch.delenv("LOG_PATH", raising=False)
    monkeypatch.delenv("REPORT_PATH", raising=False)

    settings = Settings.load()

    expected_root = Path(tempfile.gettempdir()) / "house"
    assert settings.db_path == expected_root / "data" / "house-bot" / "filings.db"
    assert settings.log_path == expected_root / "logs" / "house-bot.jsonl"
    assert settings.report_path == expected_root / "reports" / "house-bot"
