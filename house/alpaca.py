from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from time import sleep
from typing import Any

from .config import Settings
from .http import HttpClient
from .models import AccountSnapshot, MarketQuote, PlannedOrder, Position
from .utils import chunked


@dataclass(slots=True)
class AssetInfo:
    symbol: str
    tradable: bool
    shortable: bool
    easy_to_borrow: bool
    fractionable: bool
    exchange: str
    asset_class: str


class AlpacaClient:
    def __init__(self, settings: Settings, http: HttpClient) -> None:
        self.settings = settings
        self.http = http
        self.trade_base = settings.alpaca_base_url.rstrip("/")
        self.data_base = settings.alpaca_data_base_url.rstrip("/")

    @property
    def configured(self) -> bool:
        return bool(self.settings.alpaca_api_key and self.settings.alpaca_secret_key)

    def _headers(self) -> dict[str, str]:
        return self.settings.alpaca_headers

    def account_snapshot(self) -> AccountSnapshot:
        payload = self.http.get_json(f"{self.trade_base}/v2/account", headers=self._headers())
        return AccountSnapshot(
            nav=float(payload["portfolio_value"]),
            buying_power=float(payload["buying_power"]),
            equity=float(payload["equity"]),
            cash=float(payload["cash"]),
        )

    def clock(self) -> dict[str, Any]:
        return self.http.get_json(f"{self.trade_base}/v2/clock", headers=self._headers())

    def market_is_open(self) -> bool:
        return bool(self.clock().get("is_open"))

    def positions(self) -> list[Position]:
        payload = self.http.get_json(f"{self.trade_base}/v2/positions", headers=self._headers())
        positions: list[Position] = []
        for row in payload:
            qty = float(row["qty"])
            side = row.get("side", "long")
            positions.append(
                Position(
                    symbol=row["symbol"],
                    qty=qty,
                    market_value=float(row["market_value"]),
                    current_price=float(row["current_price"]),
                    side=side,
                    unrealized_plpc=float(row.get("unrealized_plpc") or 0.0),
                    unrealized_pl=float(row.get("unrealized_pl") or 0.0),
                )
            )
        return positions

    def open_orders(self) -> list[dict[str, Any]]:
        return self.http.get_json(
            f"{self.trade_base}/v2/orders",
            headers=self._headers(),
            params={"status": "open"},
        )

    def cancel_order(self, order_id: str) -> None:
        self.http.request("DELETE", f"{self.trade_base}/v2/orders/{order_id}", headers=self._headers())

    def asset(self, symbol: str) -> AssetInfo:
        payload = self.http.get_json(
            f"{self.trade_base}/v2/assets/{symbol}",
            headers=self._headers(),
        )
        return AssetInfo(
            symbol=payload["symbol"],
            tradable=bool(payload.get("tradable")),
            shortable=bool(payload.get("shortable")),
            easy_to_borrow=bool(payload.get("easy_to_borrow")),
            fractionable=bool(payload.get("fractionable")),
            exchange=str(payload.get("exchange") or ""),
            asset_class=str(payload.get("class") or ""),
        )

    def asset_map(self, symbols: list[str]) -> dict[str, AssetInfo]:
        result: dict[str, AssetInfo] = {}
        for symbol in sorted(set(symbols)):
            result[symbol] = self.asset(symbol)
        return result

    def latest_quotes(self, symbols: list[str]) -> dict[str, MarketQuote]:
        quotes: dict[str, MarketQuote] = {}
        for batch in chunked(sorted(set(symbols)), 100):
            payload = self.http.get_json(
                f"{self.data_base}/v2/stocks/quotes/latest",
                headers=self._headers(),
                params={"symbols": ",".join(batch)},
            )
            rows = payload.get("quotes", {})
            for symbol, row in rows.items():
                bid_price = float(row.get("bp") or 0.0)
                ask_price = float(row.get("ap") or 0.0)
                last_price = ask_price or bid_price
                quotes[symbol] = MarketQuote(
                    symbol=symbol,
                    bid_price=bid_price,
                    ask_price=ask_price,
                    last_price=last_price,
                )
        return quotes

    def submit_order(self, order: PlannedOrder) -> dict[str, Any]:
        payload = {
            "symbol": order.symbol,
            "qty": str(order.qty),
            "side": order.side,
            "type": "limit",
            "limit_price": f"{order.limit_price:.2f}",
            "time_in_force": "day",
            "client_order_id": order.client_order_id,
        }
        response = self.http.get_json(
            f"{self.trade_base}/v2/orders",
            method="POST",
            headers=self._headers(),
            json=payload,
        )
        sleep(0.3)
        return response

