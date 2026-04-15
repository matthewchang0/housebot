from __future__ import annotations

import os
import re
import tempfile
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


def _find_local_env_file() -> Path | None:
    for directory in (Path.cwd(), *Path.cwd().parents):
        candidate = directory / ".env"
        if candidate.is_file():
            return candidate
    return None


def _parse_env_assignment(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if stripped.startswith("export "):
        stripped = stripped[7:].lstrip()
    if "=" not in stripped:
        return None

    key, value = stripped.split("=", 1)
    key = key.strip()
    value = value.strip()
    if not key:
        return None

    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]
    elif " #" in value:
        value = value.split(" #", 1)[0].rstrip()

    return key, value


def _load_local_env() -> None:
    env_file = _find_local_env_file()
    if env_file is None:
        return

    for raw_line in env_file.read_text().splitlines():
        assignment = _parse_env_assignment(raw_line)
        if assignment is None:
            continue
        key, value = assignment
        os.environ.setdefault(key, value)


def _default_storage_root() -> Path:
    if os.getenv("VERCEL"):
        return Path(tempfile.gettempdir()) / "house"
    return Path(".")


def _default_path(env_name: str, fallback: Path) -> Path:
    raw = os.getenv(env_name)
    if raw is not None and raw != "":
        return Path(raw)
    return fallback


@dataclass(frozen=True)
class Settings:
    backend_id: str
    alpaca_api_key: str
    alpaca_secret_key: str
    alpaca_base_url: str
    alpaca_data_base_url: str
    quiver_api_key: str
    anthropic_api_key: str
    anthropic_base_url: str
    anthropic_model: str
    anthropic_version: str
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
    report_path: Path

    @classmethod
    def load(cls) -> "Settings":
        _load_local_env()
        backend_id = _normalize_backend_id(os.getenv("BACKEND_ID", "house"))
        mode = os.getenv("MODE", "PAPER").strip().upper() or "PAPER"
        storage_root = _default_storage_root()
        default_trade_url = (
            "https://api.alpaca.markets"
            if mode == "LIVE"
            else "https://paper-api.alpaca.markets"
        )
        settings = cls(
            backend_id=backend_id,
            alpaca_api_key=os.getenv("ALPACA_API_KEY", ""),
            alpaca_secret_key=os.getenv("ALPACA_SECRET_KEY", ""),
            alpaca_base_url=os.getenv("ALPACA_BASE_URL", default_trade_url),
            alpaca_data_base_url=os.getenv(
                "ALPACA_DATA_BASE_URL", "https://data.alpaca.markets"
            ),
            quiver_api_key=os.getenv("QUIVER_API_KEY", ""),
            anthropic_api_key=os.getenv("ANTHROPIC_API_KEY", ""),
            anthropic_base_url=os.getenv("ANTHROPIC_BASE_URL", "https://api.anthropic.com"),
            anthropic_model=os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514"),
            anthropic_version=os.getenv("ANTHROPIC_VERSION", "2023-06-01"),
            mode=mode,
            lookback_days=_env_int("LOOKBACK_DAYS", 90),
            long_exposure=_env_float("LONG_EXPOSURE", 1.30),
            short_exposure=_env_float("SHORT_EXPOSURE", 0.30),
            max_position_pct=_env_float("MAX_POSITION_PCT", 0.15),
            max_drawdown_soft=_env_float("MAX_DRAWDOWN_SOFT", 0.15),
            max_drawdown_hard=_env_float("MAX_DRAWDOWN_HARD", 0.25),
            log_path=_default_path("LOG_PATH", storage_root / "logs" / f"{backend_id}.jsonl"),
            db_path=_default_path("DB_PATH", storage_root / "data" / backend_id / "filings.db"),
            poll_interval_market=_env_int("POLL_INTERVAL_MARKET", 900),
            poll_interval_off=_env_int("POLL_INTERVAL_OFF", 3600),
            max_long_positions=_env_int("MAX_LONG_POSITIONS", 50),
            max_short_positions=_env_int("MAX_SHORT_POSITIONS", 20),
            min_position_size=_env_float("MIN_POSITION_SIZE", 500.0),
            user_agent=os.getenv("USER_AGENT", "NancyBot/0.1"),
            poor_accuracy_members=_env_csv("POOR_ACCURACY_MEMBERS"),
            report_path=_default_path("REPORT_PATH", storage_root / "reports" / backend_id),
        )
        settings.log_path.parent.mkdir(parents=True, exist_ok=True)
        settings.db_path.parent.mkdir(parents=True, exist_ok=True)
        settings.report_path.mkdir(parents=True, exist_ok=True)
        return settings

    @property
    def alpaca_headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.alpaca_api_key,
            "APCA-API-SECRET-KEY": self.alpaca_secret_key,
        }

    @property
    def anthropic_headers(self) -> dict[str, str]:
        return {
            "x-api-key": self.anthropic_api_key,
            "anthropic-version": self.anthropic_version,
            "Content-Type": "application/json",
        }

    @property
    def order_prefix(self) -> str:
        return self.backend_id

    @property
    def is_live(self) -> bool:
        return self.mode == "LIVE"


def _normalize_backend_id(raw: str) -> str:
    cleaned = re.sub(r"[^a-z0-9_-]+", "-", raw.strip().lower())
    cleaned = cleaned.strip("-_")
    return cleaned or "house"
