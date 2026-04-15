from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterable
from threading import Lock
from typing import Any

import httpx

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


def _upstream_base_url() -> str | None:
    raw = os.getenv("DASHBOARD_UPSTREAM_URL", "").strip()
    if not raw:
        return None
    return raw.rstrip("/")


def _upstream_headers() -> dict[str, str]:
    token = os.getenv("DASHBOARD_UPSTREAM_TOKEN", "").strip()
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}"}


def _fetch_upstream(path: str) -> tuple[str, bytes, str]:
    base_url = _upstream_base_url()
    if not base_url:
        raise RuntimeError("Dashboard upstream is not configured.")

    response = httpx.get(
        f"{base_url}{path}",
        headers=_upstream_headers(),
        timeout=15.0,
        follow_redirects=True,
    )
    content_type = response.headers.get("Content-Type", "application/json; charset=utf-8")
    return f"{response.status_code} {response.reason_phrase}", response.content, content_type


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
            if _upstream_base_url():
                status, body, content_type = _fetch_upstream(path)
                return _response(
                    start_response,
                    status,
                    b"" if is_head else body,
                    content_type=content_type,
                    extra_headers=(("Cache-Control", "no-store"),),
                )

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
        if _upstream_base_url():
            try:
                status, body, content_type = _fetch_upstream(path)
                return _response(
                    start_response,
                    status,
                    b"" if is_head else body,
                    content_type=content_type,
                    extra_headers=(("Cache-Control", "no-store"),),
                )
            except Exception as exc:
                body = json.dumps({"error": str(exc)}).encode("utf-8")
                return _response(
                    start_response,
                    "502 Bad Gateway",
                    b"" if is_head else body,
                    content_type="application/json; charset=utf-8",
                    extra_headers=(("Cache-Control", "no-store"),),
                )

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
