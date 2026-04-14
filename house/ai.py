from __future__ import annotations

import json
from typing import Any

from .config import Settings
from .http import HttpClient


class AnthropicClient:
    def __init__(self, settings: Settings, http: HttpClient) -> None:
        self.settings = settings
        self.http = http

    @property
    def configured(self) -> bool:
        return bool(self.settings.anthropic_api_key)

    def operator_brief(self, payload: dict[str, Any], focus: str | None = None) -> str:
        if not self.configured:
            raise RuntimeError("Missing Anthropic credentials. Set ANTHROPIC_API_KEY.")

        response = self.http.get_json(
            f"{self.settings.anthropic_base_url.rstrip('/')}/v1/messages",
            method="POST",
            headers=self.settings.anthropic_headers,
            json={
                "model": self.settings.anthropic_model,
                "max_tokens": 450,
                "system": (
                    "You are an operations assistant for a deterministic trading bot. "
                    "Summarize only the facts present in the provided JSON. "
                    "Do not invent missing values or claim trades were executed unless the data says so. "
                    "Respond in plain text with three short sections: Summary, Risks, Next checks. "
                    "Keep the total response under 300 words."
                ),
                "messages": [
                    {
                        "role": "user",
                        "content": self._prompt_text(payload, focus),
                    }
                ],
            },
        )
        text = self._extract_text(response)
        if not text:
            raise RuntimeError("Anthropic response did not include any text output.")
        return text.strip()

    def _prompt_text(self, payload: dict[str, Any], focus: str | None) -> str:
        lines = [
            "Create a concise operator brief for this bot state.",
            "Mention stale data, risk flags, skipped symbols, or missing configuration if present.",
            "Keep it under 300 words.",
        ]
        if focus:
            lines.append(f"User focus: {focus.strip()}")
        lines.append("Bot state JSON:")
        lines.append(json.dumps(payload, indent=2, sort_keys=True, default=str))
        return "\n".join(lines)

    def _extract_text(self, payload: Any) -> str:
        if not isinstance(payload, dict):
            return ""
        content = payload.get("content")
        if not isinstance(content, list):
            return ""

        chunks: list[str] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            text = block.get("text")
            if isinstance(text, str):
                chunks.append(text)
        return "\n".join(part.strip() for part in chunks if part and part.strip())
