from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any


@dataclass(slots=True)
class Filing:
    member_name: str
    relation: str
    ticker: str
    direction: str
    tx_date: date | None
    filing_date: date
    amount_range: str
    amount_midpoint: float
    committee: str | None
    asset_type: str
    context_score: float = 1.0
    status: str = "NEW"
    source: str = ""
    raw_text: str | None = None

    def dedupe_key(self) -> tuple[str, str, date | None, str]:
        return (
            self.member_name.strip().upper(),
            self.ticker.strip().upper(),
            self.tx_date,
            self.amount_range.strip(),
        )

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["tx_date"] = self.tx_date.isoformat() if self.tx_date else None
        data["filing_date"] = self.filing_date.isoformat()
        return data


@dataclass(slots=True)
class Signal:
    filing: Filing
    adjusted_midpoint: float
    decay: float


@dataclass(slots=True)
class TargetPosition:
    symbol: str
    side: str
    target_notional: float
    weight: float
    rationale: str
    source_filings: list[Filing] = field(default_factory=list)


@dataclass(slots=True)
class PlannedOrder:
    symbol: str
    side: str
    qty: float
    limit_price: float
    rationale: str
    client_order_id: str
    rebalance_date: date
    sequence: int
    intent: str = ""


@dataclass(slots=True)
class BrokerFill:
    activity_id: str
    order_id: str
    symbol: str
    side: str
    qty: float
    price: float
    transaction_time: datetime
    activity_type: str
    fill_type: str


@dataclass(slots=True)
class MarketQuote:
    symbol: str
    bid_price: float
    ask_price: float
    last_price: float


@dataclass(slots=True)
class AccountSnapshot:
    nav: float
    buying_power: float
    equity: float
    cash: float


@dataclass(slots=True)
class Position:
    symbol: str
    qty: float
    market_value: float
    current_price: float
    side: str
    unrealized_plpc: float
    unrealized_pl: float


@dataclass(slots=True)
class RiskAction:
    event_type: str
    details: str
    action_taken: str


@dataclass(slots=True)
class DailySummary:
    as_of: datetime
    nav: float
    daily_pnl: float
    long_exposure: float
    short_exposure: float
    net_exposure: float
    top_longs: list[dict[str, Any]]
    top_shorts: list[dict[str, Any]]
    new_filings: int
    orders: list[dict[str, Any]]
    risk_events: list[dict[str, Any]]
    flagged_filings: list[dict[str, Any]]


@dataclass(slots=True)
class RebalanceResult:
    rebalance_date: date
    targets: list[TargetPosition]
    planned_orders: list[PlannedOrder]
    skipped_symbols: list[dict[str, str]]
