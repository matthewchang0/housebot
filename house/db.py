from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any, Iterator

from .models import Filing, PlannedOrder


SCHEMA = """
CREATE TABLE IF NOT EXISTS filings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    member_name TEXT NOT NULL,
    relation TEXT NOT NULL,
    ticker TEXT NOT NULL,
    direction TEXT NOT NULL,
    tx_date DATE,
    filing_date DATE NOT NULL,
    amount_range TEXT NOT NULL,
    amount_midpoint REAL NOT NULL,
    committee TEXT,
    asset_type TEXT NOT NULL,
    context_score REAL DEFAULT 1.0,
    status TEXT DEFAULT 'ACTIVE',
    source TEXT NOT NULL,
    raw_text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(member_name, ticker, tx_date, amount_range)
);

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date DATE NOT NULL,
    nav REAL NOT NULL,
    long_exposure REAL NOT NULL,
    short_exposure REAL NOT NULL,
    net_exposure REAL NOT NULL,
    positions_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id TEXT UNIQUE NOT NULL,
    alpaca_order_id TEXT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    qty REAL NOT NULL,
    limit_price REAL,
    status TEXT NOT NULL,
    rebalance_date DATE NOT NULL,
    rationale TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    filled_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS risk_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    details TEXT NOT NULL,
    action_taken TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS runtime_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        self._conn.row_factory = sqlite3.Row
        self.initialize()

    def initialize(self) -> None:
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        try:
            yield self._conn
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def insert_filings(self, filings: list[Filing]) -> int:
        inserted = 0
        with self.transaction() as conn:
            for filing in filings:
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO filings (
                        member_name, relation, ticker, direction, tx_date, filing_date,
                        amount_range, amount_midpoint, committee, asset_type,
                        context_score, status, source, raw_text
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        filing.member_name,
                        filing.relation,
                        filing.ticker,
                        filing.direction,
                        filing.tx_date.isoformat() if filing.tx_date else None,
                        filing.filing_date.isoformat(),
                        filing.amount_range,
                        filing.amount_midpoint,
                        filing.committee,
                        filing.asset_type,
                        filing.context_score,
                        filing.status,
                        filing.source,
                        filing.raw_text,
                    ),
                )
                inserted += cursor.rowcount
        return inserted

    def list_active_filings(self, as_of: date, lookback_days: int) -> list[sqlite3.Row]:
        return self._conn.execute(
            """
            SELECT *
            FROM filings
            WHERE status IN ('ACTIVE', 'NEW')
              AND date(filing_date) >= date(?, ?)
              AND direction IN ('PURCHASE', 'SALE')
            ORDER BY filing_date DESC
            """,
            (as_of.isoformat(), f"-{lookback_days} days"),
        ).fetchall()

    def list_flagged_filings(self, on_date: date | None = None) -> list[sqlite3.Row]:
        if on_date:
            return self._conn.execute(
                "SELECT * FROM filings WHERE status = 'FLAGGED' AND date(created_at) = date(?) ORDER BY created_at DESC",
                (on_date.isoformat(),),
            ).fetchall()
        return self._conn.execute(
            "SELECT * FROM filings WHERE status = 'FLAGGED' ORDER BY created_at DESC"
        ).fetchall()

    def count_new_filings(self, on_date: date) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS count FROM filings WHERE date(created_at) = date(?)",
            (on_date.isoformat(),),
        ).fetchone()
        return int(row["count"]) if row else 0

    def record_snapshot(
        self,
        snapshot_date: date,
        nav: float,
        long_exposure: float,
        short_exposure: float,
        net_exposure: float,
        positions: list[dict[str, Any]],
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO portfolio_snapshots (
                    snapshot_date, nav, long_exposure, short_exposure, net_exposure, positions_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_date.isoformat(),
                    nav,
                    long_exposure,
                    short_exposure,
                    net_exposure,
                    json.dumps(positions),
                ),
            )

    def latest_snapshot(self) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM portfolio_snapshots ORDER BY snapshot_date DESC, id DESC LIMIT 1"
        ).fetchone()

    def peak_nav(self) -> float:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(nav), 0) AS peak_nav FROM portfolio_snapshots"
        ).fetchone()
        return float(row["peak_nav"]) if row else 0.0

    def record_order(self, order: PlannedOrder, status: str = "PENDING") -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO orders (
                    client_order_id, symbol, side, qty, limit_price, status, rebalance_date, rationale
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order.client_order_id,
                    order.symbol,
                    order.side,
                    order.qty,
                    order.limit_price,
                    status,
                    order.rebalance_date.isoformat(),
                    order.rationale,
                ),
            )

    def update_order_status(
        self,
        client_order_id: str,
        status: str,
        alpaca_order_id: str | None = None,
        filled_at: str | None = None,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE orders
                SET status = ?, alpaca_order_id = COALESCE(?, alpaca_order_id), filled_at = COALESCE(?, filled_at)
                WHERE client_order_id = ?
                """,
                (status, alpaca_order_id, filled_at, client_order_id),
            )

    def recent_orders(self, on_date: date) -> list[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM orders WHERE date(created_at) = date(?) ORDER BY created_at DESC",
            (on_date.isoformat(),),
        ).fetchall()

    def get_order(self, client_order_id: str) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM orders WHERE client_order_id = ?",
            (client_order_id,),
        ).fetchone()

    def record_risk_event(self, event_type: str, details: str, action_taken: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO risk_events (event_type, details, action_taken) VALUES (?, ?, ?)",
                (event_type, details, action_taken),
            )

    def recent_risk_events(self, on_date: date) -> list[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM risk_events WHERE date(created_at) = date(?) ORDER BY created_at DESC",
            (on_date.isoformat(),),
        ).fetchall()

    def latest_filing_date(self) -> date | None:
        row = self._conn.execute(
            "SELECT MAX(date(filing_date)) AS latest_filing_date FROM filings WHERE status IN ('ACTIVE', 'NEW')"
        ).fetchone()
        if row and row["latest_filing_date"]:
            return date.fromisoformat(str(row["latest_filing_date"]))
        return None

    def get_runtime_state(self, key: str, default: str | None = None) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM runtime_state WHERE key = ?",
            (key,),
        ).fetchone()
        if row:
            return str(row["value"])
        return default

    def set_runtime_state(self, key: str, value: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO runtime_state (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
                """,
                (key, value),
            )

    def close(self) -> None:
        self._conn.close()
