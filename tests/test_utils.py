from datetime import date

from house.utils import linear_decay, parse_amount_midpoint, redistribute_with_cap


def test_amount_midpoints_match_strategy_table() -> None:
    assert parse_amount_midpoint("$1,001 - $15,000") == 8_000.0
    assert parse_amount_midpoint("$500,001 - $1,000,000") == 750_000.0
    assert parse_amount_midpoint("Over $50,000,000") == 50_000_000.0




def test_linear_decay_hits_zero_at_window_end() -> None:
    assert linear_decay(date(2026, 1, 1), date(2026, 4, 1), 90) == 0.0
    assert round(linear_decay(date(2026, 3, 1), date(2026, 4, 1), 90), 3) == 0.656


def test_redistribution_respects_cap() -> None:
    redistributed = redistribute_with_cap(
        {"AAPL": 70.0, "MSFT": 20.0, "NVDA": 10.0},
        total_target=100.0,
        cap=40.0,
    )
    assert redistributed["AAPL"] == 40.0
    assert round(sum(redistributed.values()), 5) == 100.0
