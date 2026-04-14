from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw is not None and raw != "" else default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw is not None and raw != "" else default


def _env_csv(name: str) -> tuple[str, ...]:
    raw = os.getenv(name, "")
    if not raw.strip():
        return ()
    return tuple(part.strip() for part in raw.split(",") if part.strip())


@dataclass(frozen=True)
class Settings:
    alpaca_api_key: str
    alpaca_secret_key: str
    alpaca_base_url: str
    alpaca_data_base_url: str
    quiver_api_key: str
    mode: str
    lookback_days: int
    long_exposure: float
    short_exposure: float
    max_position_pct: float
    max_drawdown_soft: float
    max_drawdown_hard: float
    log_path: Path
    db_path: Path
    poll_interval_market: int
    poll_interval_off: int
    max_long_positions: int
    max_short_positions: int
    min_position_size: float
    user_agent: str
    poor_accuracy_members: tuple[str, ...]

    @classmethod
    def load(cls) -> "Settings":
        mode = os.getenv("MODE", "PAPER").strip().upper() or "PAPER"
        default_trade_url = (
            "https://api.alpaca.markets"
            if mode == "LIVE"
            else "https://paper-api.alpaca.markets"
        )
        settings = cls(
            alpaca_api_key=os.getenv("ALPACA_API_KEY", ""),
            alpaca_secret_key=os.getenv("ALPACA_SECRET_KEY", ""),
            alpaca_base_url=os.getenv("ALPACA_BASE_URL", default_trade_url),
            alpaca_data_base_url=os.getenv(
                "ALPACA_DATA_BASE_URL", "https://data.alpaca.markets"
            ),
            quiver_api_key=os.getenv("QUIVER_API_KEY", ""),
            mode=mode,
            lookback_days=_env_int("LOOKBACK_DAYS", 90),
            long_exposure=_env_float("LONG_EXPOSURE", 1.30),
            short_exposure=_env_float("SHORT_EXPOSURE", 0.30),
            max_position_pct=_env_float("MAX_POSITION_PCT", 0.15),
            max_drawdown_soft=_env_float("MAX_DRAWDOWN_SOFT", 0.15),
            max_drawdown_hard=_env_float("MAX_DRAWDOWN_HARD", 0.25),
            log_path=Path(os.getenv("LOG_PATH", "./logs/house.jsonl")),
            db_path=Path(os.getenv("DB_PATH", "./data/filings.db")),
            poll_interval_market=_env_int("POLL_INTERVAL_MARKET", 900),
            poll_interval_off=_env_int("POLL_INTERVAL_OFF", 3600),
            max_long_positions=_env_int("MAX_LONG_POSITIONS", 50),
            max_short_positions=_env_int("MAX_SHORT_POSITIONS", 20),
            min_position_size=_env_float("MIN_POSITION_SIZE", 500.0),
            user_agent=os.getenv("USER_AGENT", "NancyBot/0.1"),
            poor_accuracy_members=_env_csv("POOR_ACCURACY_MEMBERS"),
        )
        settings.log_path.parent.mkdir(parents=True, exist_ok=True)
        settings.db_path.parent.mkdir(parents=True, exist_ok=True)
        return settings

    @property
    def alpaca_headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.alpaca_api_key,
            "APCA-API-SECRET-KEY": self.alpaca_secret_key,
        }

    @property
    def is_live(self) -> bool:
        return self.mode == "LIVE"
