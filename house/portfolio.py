from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import date
from typing import Any

from .config import Settings
from .models import Filing, Signal, TargetPosition
from .utils import linear_decay, redistribute_with_cap


COMMITTEE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "agriculture": ("farm", "agri", "fertilizer", "grain", "seed"),
    "armed services": ("defense", "aero", "weapon", "military", "security"),
    "financial": ("bank", "capital", "finance", "payment", "insurance"),
    "ways and means": ("tax", "insurance", "health", "trade", "payment"),
    "energy": ("energy", "oil", "gas", "utility", "solar", "power"),
    "commerce": ("retail", "consumer", "commerce", "tech", "media"),
    "science": ("tech", "software", "semi", "chip", "ai", "space"),
    "transportation": ("rail", "air", "logistics", "shipping", "transport"),
    "infrastructure": ("rail", "air", "logistics", "shipping", "transport"),
    "veterans": ("medical", "defense", "health"),
    "homeland security": ("security", "cyber", "defense"),
    "intelligence": ("security", "cyber", "defense"),
}


def row_to_filing(row: Any) -> Filing:
    return Filing(
        member_name=str(row["member_name"]),
        relation=str(row["relation"]),
        ticker=str(row["ticker"]),
        direction=str(row["direction"]),
        tx_date=date.fromisoformat(row["tx_date"]) if row["tx_date"] else None,
        filing_date=date.fromisoformat(row["filing_date"]),
        amount_range=str(row["amount_range"]),
        amount_midpoint=float(row["amount_midpoint"]),
        committee=str(row["committee"]) if row["committee"] else None,
        asset_type=str(row["asset_type"]),
        context_score=float(row["context_score"] or 1.0),
        status=str(row["status"]),
        source=str(row["source"]),
        raw_text=str(row["raw_text"]) if row["raw_text"] else None,
    )


def _committee_relevance(filing: Filing) -> bool:
    committee = (filing.committee or "").lower()
    text = (filing.raw_text or filing.ticker).lower()
    for committee_name, keywords in COMMITTEE_KEYWORDS.items():
        if committee_name in committee and any(keyword in text for keyword in keywords):
            return True
    return False


def _routine_signal(filing: Filing, counts: Counter[tuple[str, str]]) -> bool:
    return counts[(filing.member_name, filing.ticker)] >= 3 and filing.amount_midpoint <= 32_500


def score_filings(filings: list[Filing], as_of: date, settings: Settings) -> list[Signal]:
    counts = Counter((filing.member_name, filing.ticker) for filing in filings)
    poor_members = {member.upper() for member in settings.poor_accuracy_members}
    signals: list[Signal] = []
    for filing in filings:
        decay = linear_decay(filing.filing_date, as_of, settings.lookback_days)
        if decay <= 0:
            continue
        score = 1.0
        if _committee_relevance(filing):
            score *= 1.5
        if filing.tx_date:
            filing_delay = (filing.filing_date - filing.tx_date).days
            if filing_delay > 30:
                score *= 0.7
            elif filing_delay < 7:
                score *= 1.3
        if filing.relation.lower() == "self":
            score *= 1.1
        if filing.member_name.upper() in poor_members:
            score *= 0.5
        if _routine_signal(filing, counts):
            score *= 0.6
        filing.context_score = score
        signals.append(
            Signal(
                filing=filing,
                adjusted_midpoint=filing.amount_midpoint * decay * score,
                decay=decay,
            )
        )
    return signals


def _aggregate_signals(signals: list[Signal]) -> dict[tuple[str, str], dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {"signals": [], "value": 0.0, "cluster": 1.0}
    )
    for signal in signals:
        key = (signal.filing.ticker, signal.filing.direction)
        bucket = grouped[key]
        bucket["signals"].append(signal)
        bucket["value"] += signal.adjusted_midpoint
    for key, bucket in grouped.items():
        unique_members = len({signal.filing.member_name for signal in bucket["signals"]})
        cluster = min(2.0, 1.3 ** max(0, unique_members - 1))
        bucket["cluster"] = cluster
        bucket["value"] *= cluster
    return grouped


def _resolve_conflicts(
    aggregates: dict[tuple[str, str], dict[str, Any]]
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    long_book: dict[str, dict[str, Any]] = {}
    short_book: dict[str, dict[str, Any]] = {}
    by_symbol: dict[str, dict[str, Any]] = defaultdict(dict)
    for (symbol, direction), payload in aggregates.items():
        by_symbol[symbol][direction] = payload
    for symbol, payload in by_symbol.items():
        buy_value = payload.get("PURCHASE", {}).get("value", 0.0)
        sale_value = payload.get("SALE", {}).get("value", 0.0)
        net = buy_value - sale_value
        if net > 0:
            long_book[symbol] = {
                "value": net,
                "signals": (
                    payload.get("PURCHASE", {}).get("signals", [])
                    + payload.get("SALE", {}).get("signals", [])
                ),
                "conflicted": sale_value > 0,
            }
        elif net < 0:
            short_book[symbol] = {
                "value": abs(net),
                "signals": (
                    payload.get("PURCHASE", {}).get("signals", [])
                    + payload.get("SALE", {}).get("signals", [])
                ),
                "conflicted": buy_value > 0,
            }
    return long_book, short_book


def _allocate_book(
    raw_book: dict[str, dict[str, Any]],
    total_exposure: float,
    max_positions: int,
    cap: float,
    min_position_size: float,
    side: str,
) -> list[TargetPosition]:
    if not raw_book or total_exposure <= 0:
        return []
    top_items = dict(
        sorted(raw_book.items(), key=lambda item: item[1]["value"], reverse=True)[:max_positions]
    )
    raw_total = sum(entry["value"] for entry in top_items.values())
    if raw_total <= 0:
        return []
    notionals = {
        symbol: total_exposure * (entry["value"] / raw_total)
        for symbol, entry in top_items.items()
    }
    notionals = redistribute_with_cap(notionals, total_exposure, cap)
    notionals = {
        symbol: notional
        for symbol, notional in notionals.items()
        if notional >= min_position_size
    }
    if not notionals:
        return []
    actual_total = sum(notionals.values())
    targets: list[TargetPosition] = []
    for symbol, notional in sorted(notionals.items(), key=lambda item: item[1], reverse=True):
        payload = top_items[symbol]
        source_filings = [signal.filing for signal in payload["signals"]]
        weights = notional / actual_total if actual_total else 0.0
        reasons = []
        reasons.append(f"{len(source_filings)} filings")
        if payload.get("conflicted"):
            reasons.append("conflict-netted")
        if any(filing.context_score > 1.0 for filing in source_filings):
            reasons.append("context-boosted")
        rationale = f"{symbol} {side.lower()} target from {', '.join(reasons)}."
        targets.append(
            TargetPosition(
                symbol=symbol,
                side=side,
                target_notional=notional,
                weight=weights,
                rationale=rationale,
                source_filings=source_filings,
            )
        )
    return targets


def construct_targets(
    rows: list[Any],
    nav: float,
    settings: Settings,
    as_of: date,
) -> list[TargetPosition]:
    filings = [row_to_filing(row) for row in rows if row["ticker"]]
    signals = score_filings(filings, as_of, settings)
    aggregates = _aggregate_signals(signals)
    long_book, short_book = _resolve_conflicts(aggregates)
    cap = settings.max_position_pct * nav
    long_targets = _allocate_book(
        long_book,
        settings.long_exposure * nav,
        settings.max_long_positions,
        cap,
        settings.min_position_size,
        "LONG",
    )
    short_targets = _allocate_book(
        short_book,
        settings.short_exposure * nav,
        settings.max_short_positions,
        cap,
        settings.min_position_size,
        "SHORT",
    )
    return long_targets + short_targets


def targets_as_json(targets: list[TargetPosition]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for target in targets:
        row = asdict(target)
        row["source_filings"] = [filing.as_dict() for filing in target.source_filings]
        payload.append(row)
    return payload
