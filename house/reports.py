from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .models import DailySummary, RebalanceResult


REPORTS_DIR = Path("./reports")


def _default_path(name: str, run_date: date) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    return REPORTS_DIR / f"{name}-{run_date.isoformat()}.json"


def write_daily_summary(summary: DailySummary) -> Path:
    path = _default_path("daily-summary", summary.as_of.date())
    payload = asdict(summary)
    payload["as_of"] = summary.as_of.isoformat()
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def write_rebalance_report(
    result: RebalanceResult,
    skipped_symbols: list[dict[str, str]],
) -> Path:
    path = _default_path("rebalance", result.rebalance_date)
    payload: dict[str, Any] = {
        "rebalance_date": result.rebalance_date.isoformat(),
        "targets": [asdict(target) for target in result.targets],
        "planned_orders": [asdict(order) for order in result.planned_orders],
        "skipped_symbols": skipped_symbols,
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path
