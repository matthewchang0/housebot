from __future__ import annotations

from pathlib import Path

from house.ai import AnthropicClient
from house.config import Settings


class FakeHttp:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get_json(self, url: str, **kwargs: object) -> object:
        self.calls.append({"url": url, **kwargs})
        return {
            "content": [
                {"type": "text", "text": "Summary\nAll clear."},
                {"type": "text", "text": "Risks\nNone."},
            ]
        }


def _settings() -> Settings:
    return Settings(
        backend_id="house",
        alpaca_api_key="",
        alpaca_secret_key="",
        alpaca_base_url="https://paper-api.alpaca.markets",
        alpaca_data_base_url="https://data.alpaca.markets",
        quiver_api_key="",
        anthropic_api_key="test-anthropic-key",
        anthropic_base_url="https://api.anthropic.com",
        anthropic_model="claude-sonnet-4-20250514",
        anthropic_version="2023-06-01",
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
        report_path=Path("/tmp/house-reports"),
    )


def test_anthropic_client_builds_messages_request() -> None:
    http = FakeHttp()
    client = AnthropicClient(_settings(), http)  # type: ignore[arg-type]

    brief = client.operator_brief({"status": {"mode": "PAPER"}}, focus="risk")

    assert brief.startswith("Summary")
    assert len(http.calls) == 1
    call = http.calls[0]
    assert call["url"] == "https://api.anthropic.com/v1/messages"
    assert call["method"] == "POST"
    headers = call["headers"]
    assert isinstance(headers, dict)
    assert headers["x-api-key"] == "test-anthropic-key"
    payload = call["json"]
    assert isinstance(payload, dict)
    assert payload["model"] == "claude-sonnet-4-20250514"
