from datetime import date
from pathlib import Path

from house.config import Settings
from house.portfolio import construct_targets


def _settings() -> Settings:
    return Settings(
        alpaca_api_key="",
        alpaca_secret_key="",
        alpaca_base_url="https://paper-api.alpaca.markets",
        alpaca_data_base_url="https://data.alpaca.markets",
        quiver_api_key="",
        mode="PAPER",
        lookback_days=90,
        long_exposure=1.30,
        short_exposure=0.30,
        max_position_pct=0.15,
        max_drawdown_soft=0.15,
        max_drawdown_hard=0.25,
        log_path=Path("/tmp/house-test.jsonl"),
        db_path=Path("/tmp/house-test.db"),
        poll_interval_market=900,
        poll_interval_off=3600,
        max_long_positions=50,
        max_short_positions=20,
        min_position_size=500.0,
        user_agent="NancyBot/0.1",
        poor_accuracy_members=(),
    )


def test_construct_targets_nets_conflicts_and_respects_caps() -> None:
    rows = [
        {
            "member_name": "Rep One",
            "relation": "Self",
            "ticker": "AAPL",
            "direction": "PURCHASE",
            "tx_date": "2026-04-01",
            "filing_date": "2026-04-05",
            "amount_range": "$100,001 - $250,000",
            "amount_midpoint": 175_000.0,
            "committee": "Committee on Ways and Means",
            "asset_type": "Stock",
            "context_score": 1.0,
            "status": "ACTIVE",
            "source": "clerk",
            "raw_text": "Apple Inc. Common Stock (AAPL) [ST]",
        },
        {
            "member_name": "Rep Two",
            "relation": "Self",
            "ticker": "AAPL",
            "direction": "SALE",
            "tx_date": "2026-04-01",
            "filing_date": "2026-04-05",
            "amount_range": "$15,001 - $50,000",
            "amount_midpoint": 32_500.0,
            "committee": "Committee on Armed Services",
            "asset_type": "Stock",
            "context_score": 1.0,
            "status": "ACTIVE",
            "source": "quiver",
            "raw_text": "Apple Inc. Common Stock (AAPL) [ST]",
        },
        {
            "member_name": "Rep Three",
            "relation": "Self",
            "ticker": "MSFT",
            "direction": "SALE",
            "tx_date": "2026-04-01",
            "filing_date": "2026-04-05",
            "amount_range": "$100,001 - $250,000",
            "amount_midpoint": 175_000.0,
            "committee": "Committee on Science, Space, and Technology",
            "asset_type": "Stock",
            "context_score": 1.0,
            "status": "ACTIVE",
            "source": "quiver",
            "raw_text": "Microsoft Corporation Common Stock (MSFT) [ST]",
        },
    ]
    targets = construct_targets(rows, nav=100_000.0, settings=_settings(), as_of=date(2026, 4, 14))
    longs = [target for target in targets if target.side == "LONG"]
    shorts = [target for target in targets if target.side == "SHORT"]
    assert len(longs) == 1
    assert longs[0].symbol == "AAPL"
    assert len(shorts) == 1
    assert shorts[0].symbol == "MSFT"
    assert longs[0].target_notional <= 15_000.0
    assert shorts[0].target_notional <= 15_000.0
