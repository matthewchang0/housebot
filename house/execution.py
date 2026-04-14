from __future__ import annotations

from dataclasses import replace
from datetime import date
from typing import Iterable

from .alpaca import AssetInfo
from .models import MarketQuote, PlannedOrder, Position, TargetPosition
from .utils import round_down_shares


def filter_targets_by_assets(
    targets: list[TargetPosition],
    asset_map: dict[str, AssetInfo],
) -> tuple[list[TargetPosition], list[dict[str, str]]]:
    accepted: list[TargetPosition] = []
    skipped: list[dict[str, str]] = []
    skipped_short_value = 0.0
    for target in targets:
        asset = asset_map.get(target.symbol)
        if not asset or not asset.tradable or asset.asset_class != "us_equity":
            skipped.append({"symbol": target.symbol, "reason": "not_tradable_on_alpaca"})
            continue
        if asset.exchange not in {"NYSE", "NASDAQ", "AMEX", "ARCA", "BATS"}:
            skipped.append({"symbol": target.symbol, "reason": f"unsupported_exchange:{asset.exchange}"})
            continue
        if target.side == "SHORT" and not (asset.shortable and asset.easy_to_borrow):
            skipped_short_value += target.target_notional
            skipped.append({"symbol": target.symbol, "reason": "not_easy_to_borrow"})
            continue
        accepted.append(target)
    if skipped_short_value > 0:
        accepted = _redistribute_short_targets(accepted, skipped_short_value)
    return accepted, skipped


def _redistribute_short_targets(
    targets: list[TargetPosition],
    skipped_notional: float,
) -> list[TargetPosition]:
    short_targets = [target for target in targets if target.side == "SHORT"]
    if not short_targets or skipped_notional <= 0:
        return targets
    short_total = sum(target.target_notional for target in short_targets)
    redistributed: list[TargetPosition] = []
    for target in targets:
        if target.side != "SHORT":
            redistributed.append(target)
            continue
        share = target.target_notional / short_total if short_total else 0.0
        redistributed.append(
            replace(target, target_notional=target.target_notional + (skipped_notional * share))
        )
    return redistributed


def plan_orders(
    targets: list[TargetPosition],
    current_positions: list[Position],
    quotes: dict[str, MarketQuote],
    asset_map: dict[str, AssetInfo],
    rebalance_date: date,
    order_prefix: str,
) -> list[PlannedOrder]:
    current_by_symbol = {
        position.symbol: (
            position.market_value if position.side == "long" else -abs(position.market_value)
        )
        for position in current_positions
    }
    target_by_symbol = {
        target.symbol: (target.target_notional if target.side == "LONG" else -target.target_notional)
        for target in targets
    }
    planned: list[PlannedOrder] = []
    for symbol in sorted(set(current_by_symbol) | set(target_by_symbol)):
        current = current_by_symbol.get(symbol, 0.0)
        target = target_by_symbol.get(symbol, 0.0)
        quote = quotes.get(symbol)
        asset = asset_map.get(symbol)
        if not quote or not asset:
            continue
        if current > 0 and target < 0:
            planned.extend(
                [
                    _build_order(symbol, -current, quote, asset, rebalance_date, 10, "CLOSE_LONG", order_prefix),
                    _build_order(symbol, target, quote, asset, rebalance_date, 70, "OPEN_SHORT", order_prefix),
                ]
            )
            continue
        if current < 0 and target > 0:
            planned.extend(
                [
                    _build_order(symbol, abs(current), quote, asset, rebalance_date, 20, "COVER_SHORT", order_prefix),
                    _build_order(symbol, target, quote, asset, rebalance_date, 60, "OPEN_LONG", order_prefix),
                ]
            )
            continue
        delta = target - current
        if abs(delta) < 1.0:
            continue
        if current != 0 and target == 0:
            intent = "CLOSE_LONG" if current > 0 else "COVER_SHORT"
            sequence = 10 if current > 0 else 20
        elif current > 0 and abs(target) < abs(current):
            intent = "REDUCE_LONG"
            sequence = 30
        elif current < 0 and abs(target) < abs(current):
            intent = "REDUCE_SHORT"
            sequence = 30
        elif current > 0 and abs(target) > abs(current):
            intent = "INCREASE_LONG"
            sequence = 40
        elif current < 0 and abs(target) > abs(current):
            intent = "INCREASE_SHORT"
            sequence = 50
        elif current == 0 and target > 0:
            intent = "OPEN_LONG"
            sequence = 60
        else:
            intent = "OPEN_SHORT"
            sequence = 70
        planned.append(
            _build_order(symbol, delta, quote, asset, rebalance_date, sequence, intent, order_prefix)
        )
    return [order for order in planned if order.qty > 0]


def _build_order(
    symbol: str,
    delta_notional: float,
    quote: MarketQuote,
    asset: AssetInfo,
    rebalance_date: date,
    sequence: int,
    intent: str,
    order_prefix: str,
) -> PlannedOrder:
    side = "buy" if delta_notional > 0 else "sell"
    price = quote.ask_price if side == "buy" else quote.bid_price
    if price <= 0:
        price = quote.last_price
    price *= 1.001 if side == "buy" else 0.999
    qty = round_down_shares(abs(delta_notional) / price, asset.fractionable)
    return PlannedOrder(
        symbol=symbol,
        side=side,
        qty=qty,
        limit_price=price,
        rationale=f"{intent} to reach rebalance target.",
        client_order_id=f"{order_prefix}-{rebalance_date:%Y%m%d}-{sequence:02d}-{symbol}-{side}",
        rebalance_date=rebalance_date,
        sequence=sequence,
        intent=intent,
    )


def scale_new_orders_for_buying_power(
    orders: list[PlannedOrder],
    buying_power: float,
) -> list[PlannedOrder]:
    capital_orders = [
        order
        for order in orders
        if order.side == "buy" and order.intent in {"OPEN_LONG", "INCREASE_LONG"}
    ]
    required = sum(order.qty * order.limit_price for order in capital_orders)
    if required <= 0 or required <= buying_power:
        return orders
    scale = buying_power / required
    scaled: list[PlannedOrder] = []
    for order in orders:
        if order in capital_orders:
            scaled.append(replace(order, qty=order.qty * scale))
        else:
            scaled.append(order)
    return [order for order in scaled if order.qty > 0]


def symbols_for_targets_and_positions(
    targets: Iterable[TargetPosition], positions: Iterable[Position]
) -> list[str]:
    symbols = {target.symbol for target in targets}
    symbols.update(position.symbol for position in positions)
    return sorted(symbols)
