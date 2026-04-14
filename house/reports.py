from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .models import DailySummary, RebalanceResult


def _default_path(report_dir: Path, name: str, run_date: date) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir / f"{name}-{run_date.isoformat()}.json"


def write_daily_summary(summary: DailySummary, report_dir: Path) -> Path:
    path = _default_path(report_dir, "daily-summary", summary.as_of.date())
    payload = asdict(summary)
    payload["as_of"] = summary.as_of.isoformat()
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def write_rebalance_report(
    result: RebalanceResult,
    skipped_symbols: list[dict[str, str]],
    report_dir: Path,
) -> Path:
    path = _default_path(report_dir, "rebalance", result.rebalance_date)
    payload: dict[str, Any] = {
        "rebalance_date": result.rebalance_date.isoformat(),
        "targets": [asdict(target) for target in result.targets],
        "planned_orders": [asdict(order) for order in result.planned_orders],
        "skipped_symbols": skipped_symbols,
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def write_ai_brief(created_at: datetime, brief: str, report_dir: Path) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = created_at.strftime("%Y-%m-%dT%H-%M-%S")
    path = report_dir / f"ai-brief-{timestamp}.md"
    path.write_text(brief.strip() + "\n", encoding="utf-8")
    return path
