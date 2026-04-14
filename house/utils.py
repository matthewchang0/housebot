from __future__ import annotations

import math
import re
from collections.abc import Iterable, Iterator
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

MIDPOINT_TABLE: tuple[tuple[re.Pattern[str], float], ...] = (
    (re.compile(r"\$1,001\s*-\s*\$15,000", re.I), 8_000.0),
    (re.compile(r"\$15,001\s*-\s*\$50,000", re.I), 32_500.0),
    (re.compile(r"\$50,001\s*-\s*\$100,000", re.I), 75_000.0),
    (re.compile(r"\$100,001\s*-\s*\$250,000", re.I), 175_000.0),
    (re.compile(r"\$250,001\s*-\s*\$500,000", re.I), 375_000.0),
    (re.compile(r"\$500,001\s*-\s*\$1,000,000", re.I), 750_000.0),
    (re.compile(r"\$1,000,001\s*-\s*\$5,000,000", re.I), 3_000_000.0),
    (re.compile(r"\$5,000,001\s*-\s*\$25,000,000", re.I), 15_000_000.0),
    (re.compile(r"\$25,000,001\s*-\s*\$50,000,000", re.I), 37_500_000.0),
    (re.compile(r"over\s+\$50,000,000", re.I), 50_000_000.0),
)

SYMBOL_ALIASES: dict[str, str] = {
    "BRCM": "AVGO",
    "SQ": "XYZ",
}


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\x00", " ")).strip()


def parse_amount_midpoint(amount_range: str) -> float | None:
    cleaned = normalize_whitespace(amount_range)
    for pattern, midpoint in MIDPOINT_TABLE:
        if pattern.search(cleaned):
            return midpoint
    return None


def parse_us_date(raw: str | None) -> date | None:
    if not raw:
        return None
    text = normalize_whitespace(raw)
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def now_et() -> datetime:
    return datetime.now(tz=EASTERN)


def is_market_session(ts: datetime | None = None) -> bool:
    current = ts.astimezone(EASTERN) if ts else now_et()
    if current.weekday() >= 5:
        return False
    current_time = current.time()
    return MARKET_OPEN <= current_time <= MARKET_CLOSE


def seconds_until_next_poll(market_interval: int, off_interval: int, ts: datetime | None = None) -> int:
    return market_interval if is_market_session(ts) else off_interval


def linear_decay(filing_date: date, as_of: date, lookback_days: int) -> float:
    days_since = max(0, (as_of - filing_date).days)
    return max(0.0, 1.0 - (days_since / lookback_days))


def chunked(items: Iterable[str], size: int) -> Iterator[list[str]]:
    batch: list[str] = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def clamp(value: float, floor: float, ceiling: float) -> float:
    return min(max(value, floor), ceiling)


def round_down_shares(qty: float, fractional: bool) -> float:
    if fractional:
        return math.floor(qty * 1000) / 1000
    return math.floor(qty)


def to_iso_z(ts: datetime) -> str:
    return ts.astimezone(ZoneInfo("UTC")).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def business_days_ago(days: int, start: date | None = None) -> date:
    current = start or now_et().date()
    remaining = days
    while remaining > 0:
        current -= timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current


def normalize_symbol(symbol: str) -> str:
    cleaned = normalize_whitespace(symbol).upper()
    if ":" in cleaned:
        cleaned = cleaned.split(":", 1)[0]
    cleaned = cleaned.replace("/", ".")
    cleaned = re.sub(r"[^A-Z.]", "", cleaned)
    return SYMBOL_ALIASES.get(cleaned, cleaned)


def redistribute_with_cap(
    raw_targets: dict[str, float],
    total_target: float,
    cap: float,
) -> dict[str, float]:
    if not raw_targets or total_target <= 0:
        return {}
    values = raw_targets.copy()
    while True:
        capped = {symbol: min(value, cap) for symbol, value in values.items()}
        excess = total_target - sum(capped.values())
        eligible = {
            symbol: values[symbol]
            for symbol in values
            if capped[symbol] < cap and values[symbol] > 0
        }
        if excess <= 1e-6 or not eligible:
            return capped
        eligible_total = sum(eligible.values())
        if eligible_total <= 0:
            return capped
        values = capped.copy()
        for symbol, value in eligible.items():
            values[symbol] += excess * (value / eligible_total)
