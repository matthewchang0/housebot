from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from threading import Lock
from typing import Any

from house.bot import HouseBot
from house.dashboard import DASHBOARD_HTML, DashboardApp

StartResponse = Callable[[str, list[tuple[str, str]]], Any]

_dashboard_lock = Lock()
_dashboard_app: DashboardApp | None = None


def _get_dashboard_app() -> DashboardApp:
    global _dashboard_app
    with _dashboard_lock:
        if _dashboard_app is None:
            _dashboard_app = DashboardApp(bot=HouseBot())
    return _dashboard_app


def _response(
    start_response: StartResponse,
    status: str,
    body: bytes,
    *,
    content_type: str,
    extra_headers: Iterable[tuple[str, str]] = (),
) -> list[bytes]:
    headers = [
        ("Content-Type", content_type),
        ("Content-Length", str(len(body))),
        *extra_headers,
    ]
    start_response(status, headers)
    return [body]


def app(environ: dict[str, Any], start_response: StartResponse) -> list[bytes]:
    method = environ.get("REQUEST_METHOD", "GET").upper()
    path = environ.get("PATH_INFO", "/") or "/"
    is_head = method == "HEAD"

    if method not in {"GET", "HEAD"}:
        body = json.dumps({"error": "Method not allowed"}).encode("utf-8")
        return _response(
            start_response,
            "405 Method Not Allowed",
            b"" if is_head else body,
            content_type="application/json; charset=utf-8",
            extra_headers=(("Allow", "GET, HEAD"),),
        )

    if path in {"/", "/index.html"}:
        body = DASHBOARD_HTML.encode("utf-8")
        return _response(
            start_response,
            "200 OK",
            b"" if is_head else body,
            content_type="text/html; charset=utf-8",
        )

    if path == "/api/dashboard":
        try:
            payload = _get_dashboard_app().dashboard_payload()
            status = "200 OK"
        except Exception as exc:
            payload = {"error": str(exc)}
            status = "500 Internal Server Error"
        body = json.dumps(payload, default=str).encode("utf-8")
        return _response(
            start_response,
            status,
            b"" if is_head else body,
            content_type="application/json; charset=utf-8",
            extra_headers=(("Cache-Control", "no-store"),),
        )

    if path == "/api/health":
        body = json.dumps({"ok": True}).encode("utf-8")
        return _response(
            start_response,
            "200 OK",
            b"" if is_head else body,
            content_type="application/json; charset=utf-8",
            extra_headers=(("Cache-Control", "no-store"),),
        )

    body = json.dumps({"error": "Not found"}).encode("utf-8")
    return _response(
        start_response,
        "404 Not Found",
        b"" if is_head else body,
        content_type="application/json; charset=utf-8",
    )
