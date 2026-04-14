from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(slots=True)
class HttpClient:
    user_agent: str
    timeout: float = 20.0
    retries: int = 3

    def __post_init__(self) -> None:
        self._client = httpx.Client(
            follow_redirects=True,
            timeout=self.timeout,
            headers={"User-Agent": self.user_agent},
        )

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        delay = 1.0
        last_error: Exception | None = None
        for attempt in range(self.retries):
            try:
                response = self._client.request(method, url, **kwargs)
                if response.status_code >= 500 and attempt < self.retries - 1:
                    time.sleep(delay)
                    delay *= 3
                    continue
                response.raise_for_status()
                return response
            except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError) as exc:
                last_error = exc
                if attempt == self.retries - 1:
                    raise
                time.sleep(delay)
                delay *= 3
        if last_error:
            raise last_error
        raise RuntimeError("HTTP request failed without raising a concrete error.")

    def get_json(self, url: str, **kwargs: Any) -> Any:
        method = kwargs.pop("method", "GET")
        return self.request(method, url, **kwargs).json()

    def get_text(self, url: str, **kwargs: Any) -> str:
        return self.request("GET", url, **kwargs).text

    def get_bytes(self, url: str, **kwargs: Any) -> bytes:
        return self.request("GET", url, **kwargs).content

    def close(self) -> None:
        self._client.close()
