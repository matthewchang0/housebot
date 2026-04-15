from __future__ import annotations

import hmac
import json
import os
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer

from .bot import HouseBot
from .config import Settings


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>House Control Room</title>
  <style>
    :root {
      --bg: #f4efe6;
      --bg-2: #efe6d7;
      --panel: rgba(255, 251, 245, 0.84);
      --panel-strong: rgba(255, 248, 239, 0.94);
      --ink: #1d2621;
      --muted: #59655e;
      --line: rgba(62, 72, 66, 0.12);
      --accent: #1f6b4f;
      --accent-soft: #d7efe2;
      --warn: #8f5e1e;
      --danger: #8a2f2d;
      --shadow: 0 22px 70px rgba(61, 49, 31, 0.10);
      --radius: 22px;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      font-family: "Avenir Next", "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(33, 108, 80, 0.14), transparent 30%),
        radial-gradient(circle at top right, rgba(214, 150, 59, 0.12), transparent 28%),
        linear-gradient(180deg, var(--bg), var(--bg-2));
    }

    .shell {
      width: min(1380px, calc(100vw - 32px));
      margin: 24px auto 48px;
    }

    .hero {
      padding: 28px;
      border: 1px solid var(--line);
      border-radius: 30px;
      background: linear-gradient(135deg, rgba(255,255,255,0.78), rgba(245,236,224,0.92));
      box-shadow: var(--shadow);
      overflow: hidden;
      position: relative;
    }

    .hero::after {
      content: "";
      position: absolute;
      inset: auto -40px -70px auto;
      width: 220px;
      height: 220px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(31,107,79,0.18), transparent 68%);
    }

    .hero h1 {
      margin: 0 0 10px;
      font-size: clamp(2rem, 4vw, 3.4rem);
      line-height: 0.95;
      letter-spacing: -0.04em;
    }

    .hero p {
      margin: 0;
      max-width: 760px;
      color: var(--muted);
      font-size: 1rem;
    }

    .hero-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 18px;
    }

    .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(255,255,255,0.8);
      border: 1px solid var(--line);
      color: var(--muted);
      font-size: 0.9rem;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(12, minmax(0, 1fr));
      gap: 18px;
      margin-top: 18px;
    }

    .card {
      grid-column: span 12;
      padding: 20px;
      border-radius: var(--radius);
      border: 1px solid var(--line);
      background: var(--panel);
      backdrop-filter: blur(8px);
      box-shadow: var(--shadow);
      animation: rise 0.45s ease both;
    }

    .card h2, .card h3 {
      margin: 0 0 14px;
      letter-spacing: -0.03em;
    }

    .stats {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }

    .stat {
      padding: 16px;
      border-radius: 18px;
      background: var(--panel-strong);
      border: 1px solid var(--line);
    }

    .label {
      color: var(--muted);
      font-size: 0.84rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    .value {
      margin-top: 8px;
      font-size: clamp(1.2rem, 3vw, 2rem);
      letter-spacing: -0.04em;
    }

    .subvalue {
      margin-top: 8px;
      color: var(--muted);
      font-size: 0.92rem;
    }

    .wide-8 { grid-column: span 8; }
    .wide-7 { grid-column: span 7; }
    .wide-6 { grid-column: span 6; }
    .wide-5 { grid-column: span 5; }
    .wide-4 { grid-column: span 4; }

    .row {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      padding: 10px 0;
      border-bottom: 1px solid var(--line);
    }

    .row:last-child {
      border-bottom: 0;
      padding-bottom: 0;
    }

    .row-title {
      font-weight: 600;
    }

    .row-copy {
      color: var(--muted);
      max-width: 70%;
      text-align: right;
      overflow-wrap: anywhere;
    }

    .table-wrap {
      overflow-x: auto;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.95rem;
    }

    th, td {
      padding: 10px 12px;
      text-align: left;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }

    th {
      color: var(--muted);
      font-weight: 600;
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    .log-list {
      display: grid;
      gap: 10px;
      max-height: 500px;
      overflow: auto;
      padding-right: 2px;
    }

    .log {
      padding: 14px;
      border-radius: 16px;
      background: rgba(255,255,255,0.66);
      border: 1px solid var(--line);
    }

    .event {
      display: inline-block;
      padding: 4px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }

    .muted {
      color: var(--muted);
    }

    .spark {
      display: flex;
      align-items: end;
      gap: 8px;
      height: 180px;
      margin-top: 12px;
      padding-top: 12px;
    }

    .bar {
      flex: 1;
      border-radius: 12px 12px 4px 4px;
      background: linear-gradient(180deg, rgba(31,107,79,0.95), rgba(31,107,79,0.40));
      min-width: 14px;
      position: relative;
    }

    .bar-label {
      position: absolute;
      left: 50%;
      bottom: -24px;
      transform: translateX(-50%) rotate(-35deg);
      transform-origin: center;
      color: var(--muted);
      font-size: 0.72rem;
      white-space: nowrap;
    }

    .empty {
      color: var(--muted);
      font-style: italic;
    }

    .footer {
      margin-top: 18px;
      text-align: center;
      color: var(--muted);
      font-size: 0.9rem;
    }

    @keyframes rise {
      from { opacity: 0; transform: translateY(8px); }
      to { opacity: 1; transform: translateY(0); }
    }

    @media (max-width: 1024px) {
      .wide-8, .wide-7, .wide-6, .wide-5, .wide-4 {
        grid-column: span 12;
      }

      .stats {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }

    @media (max-width: 680px) {
      .shell {
        width: min(100vw - 18px, 100%);
        margin-top: 10px;
      }

      .hero, .card {
        padding: 16px;
      }

      .stats {
        grid-template-columns: 1fr;
      }

      .row {
        flex-direction: column;
      }

      .row-copy {
        max-width: 100%;
        text-align: left;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <h1>House Control Room</h1>
      <p>Live visibility into the trading bot's local state, recent events, orders, filings, and reports. This dashboard refreshes every 5 seconds and never places trades.</p>
      <div class="hero-meta">
        <div class="pill" id="generated-at">Loading...</div>
        <div class="pill" id="mode-pill">Mode</div>
        <div class="pill" id="alpaca-pill">Alpaca</div>
        <div class="pill" id="latest-event-pill">Latest event</div>
      </div>
    </section>

    <div class="grid">
      <section class="card wide-8">
        <h2>Pulse</h2>
        <div class="stats" id="pulse-stats"></div>
      </section>

      <section class="card wide-4">
        <h2>Runtime Markers</h2>
        <div id="runtime-state"></div>
      </section>

      <section class="card wide-7">
        <h2>Net Asset Value Trend</h2>
        <div class="muted">Recent portfolio snapshots from the local SQLite database.</div>
        <div class="spark" id="snapshot-chart"></div>
      </section>

      <section class="card wide-5">
        <h2>Latest Reports</h2>
        <div id="reports"></div>
      </section>

      <section class="card wide-6">
        <h2>Recent Orders</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Created</th>
                <th>Symbol</th>
                <th>Side</th>
                <th>Status</th>
                <th>Qty</th>
                <th>Limit</th>
              </tr>
            </thead>
            <tbody id="orders-body"></tbody>
          </table>
        </div>
      </section>

      <section class="card wide-6">
        <h2>Recent Filings</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Filing Date</th>
                <th>Ticker</th>
                <th>Direction</th>
                <th>Status</th>
                <th>Member</th>
              </tr>
            </thead>
            <tbody id="filings-body"></tbody>
          </table>
        </div>
      </section>

      <section class="card wide-5">
        <h2>Top Positions</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Side</th>
                <th>Market Value</th>
                <th>P/L</th>
              </tr>
            </thead>
            <tbody id="positions-body"></tbody>
          </table>
        </div>
      </section>

      <section class="card wide-7">
        <h2>Recent Logs</h2>
        <div class="log-list" id="logs"></div>
      </section>

      <section class="card wide-12">
        <h2>Risk Events</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Created</th>
                <th>Event</th>
                <th>Details</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody id="risk-body"></tbody>
          </table>
        </div>
      </section>
    </div>

    <div class="footer">Refreshes every 5 seconds. Source of truth: local SQLite, reports, and JSONL logs.</div>
  </div>

  <script>
    const currency = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 });
    const decimal = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });

    function fmtMoney(value) {
      if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
      return currency.format(Number(value));
    }

    function fmtPct(value) {
      if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
      return `${decimal.format(Number(value) * 100)}%`;
    }

    function fmtText(value) {
      if (value === null || value === undefined || value === "") return "—";
      return String(value);
    }

    function setRows(targetId, rows, emptyMessage, renderer) {
      const target = document.getElementById(targetId);
      if (!rows || rows.length === 0) {
        target.innerHTML = `<tr><td colspan="8" class="empty">${emptyMessage}</td></tr>`;
        return;
      }
      target.innerHTML = rows.map(renderer).join("");
    }

    function setList(targetId, rows, emptyMessage, renderer) {
      const target = document.getElementById(targetId);
      if (!rows || rows.length === 0) {
        target.innerHTML = `<div class="empty">${emptyMessage}</div>`;
        return;
      }
      target.innerHTML = rows.map(renderer).join("");
    }

    function renderPulse(data) {
      const status = data.status || {};
      const snapshot = status.latest_snapshot || {};
      const cards = [
        { label: "Net Asset Value", value: fmtMoney(snapshot.nav), subvalue: `Net exposure ${fmtPct(snapshot.net_exposure)}` },
        { label: "Latest Filing Date", value: fmtText(status.latest_filing_date), subvalue: `${Object.values(data.filing_counts || {}).reduce((sum, count) => sum + count, 0)} filings tracked` },
        { label: "Latest Order", value: fmtText((data.latest_order || {}).symbol), subvalue: `${fmtText((data.latest_order || {}).status)} • ${fmtText((data.latest_order || {}).side)}` },
        { label: "Latest Event", value: fmtText((status.latest_log || {}).event), subvalue: fmtText((status.latest_log || {}).timestamp) },
      ];
      document.getElementById("pulse-stats").innerHTML = cards.map((card) => `
        <div class="stat">
          <div class="label">${card.label}</div>
          <div class="value">${card.value}</div>
          <div class="subvalue">${card.subvalue}</div>
        </div>
      `).join("");
    }

    function renderRuntime(data) {
      const state = (data.status || {}).runtime_state || {};
      const rows = Object.entries(state).map(([key, value]) => `
        <div class="row">
          <div class="row-title">${key.replaceAll("_", " ")}</div>
          <div class="row-copy">${fmtText(value)}</div>
        </div>
      `);
      document.getElementById("runtime-state").innerHTML = rows.join("") || `<div class="empty">No runtime markers yet.</div>`;
    }

    function renderReports(data) {
      const rebalance = data.latest_rebalance_report;
      const daily = data.latest_daily_report;
      const entries = [
        rebalance ? { title: "Rebalance", path: rebalance._path, summary: `${(rebalance.targets || []).length} targets • ${(rebalance.planned_orders || []).length} planned orders` } : null,
        daily ? { title: "Daily Summary", path: daily._path, summary: `${fmtMoney(daily.nav)} NAV • ${fmtPct(daily.net_exposure)} net exposure` } : null,
      ].filter(Boolean);

      setList("reports", entries, "No reports generated yet.", (entry) => `
        <div class="row">
          <div>
            <div class="row-title">${entry.title}</div>
            <div class="muted">${entry.summary}</div>
          </div>
          <div class="row-copy">${fmtText(entry.path)}</div>
        </div>
      `);
    }

    function renderSnapshots(data) {
      const snapshots = data.recent_snapshots || [];
      const target = document.getElementById("snapshot-chart");
      if (snapshots.length === 0) {
        target.innerHTML = `<div class="empty">No portfolio snapshots yet.</div>`;
        return;
      }
      const maxNav = Math.max(...snapshots.map((snapshot) => Number(snapshot.nav || 0)), 1);
      target.innerHTML = snapshots.map((snapshot) => {
        const height = Math.max(18, Math.round((Number(snapshot.nav || 0) / maxNav) * 150));
        return `
          <div class="bar" style="height:${height}px">
            <div class="bar-label">${fmtText(snapshot.snapshot_date).slice(5)}</div>
          </div>
        `;
      }).join("");
    }

    function renderTables(data) {
      setRows("orders-body", data.recent_orders, "No orders yet.", (row) => `
        <tr>
          <td>${fmtText(row.created_at)}</td>
          <td>${fmtText(row.symbol)}</td>
          <td>${fmtText(row.side)}</td>
          <td>${fmtText(row.status)}</td>
          <td>${fmtText(row.qty)}</td>
          <td>${fmtMoney(row.limit_price)}</td>
        </tr>
      `);

      setRows("filings-body", data.recent_filings, "No filings yet.", (row) => `
        <tr>
          <td>${fmtText(row.filing_date)}</td>
          <td>${fmtText(row.ticker)}</td>
          <td>${fmtText(row.direction)}</td>
          <td>${fmtText(row.status)}</td>
          <td>${fmtText(row.member_name)}</td>
        </tr>
      `);

      setRows("positions-body", data.latest_positions, "No positions captured in the latest snapshot.", (row) => `
        <tr>
          <td>${fmtText(row.symbol)}</td>
          <td>${fmtText(row.side)}</td>
          <td>${fmtMoney(row.market_value)}</td>
          <td>${fmtMoney(row.unrealized_pl)}</td>
        </tr>
      `);

      setRows("risk-body", data.recent_risk_events, "No risk events recorded.", (row) => `
        <tr>
          <td>${fmtText(row.created_at)}</td>
          <td>${fmtText(row.event_type)}</td>
          <td>${fmtText(row.details)}</td>
          <td>${fmtText(row.action_taken)}</td>
        </tr>
      `);
    }

    function renderLogs(data) {
      setList("logs", data.recent_logs, "No log events yet.", (row) => `
        <div class="log">
          <div class="event">${fmtText(row.event || "log")}</div>
          <div style="margin-top:10px; font-weight:600;">${fmtText(row.timestamp)}</div>
          <div class="muted" style="margin-top:8px;">${fmtText(row.rationale || row.error || row.details || row.raw || "")}</div>
        </div>
      `);
    }

    function renderMeta(data) {
      const status = data.status || {};
      document.getElementById("generated-at").textContent = `Updated ${fmtText(data.generated_at)}`;
      document.getElementById("mode-pill").textContent = `Mode ${fmtText(status.mode)}`;
      document.getElementById("alpaca-pill").textContent = status.alpaca_configured ? "Alpaca configured" : "Alpaca missing";
      document.getElementById("latest-event-pill").textContent = `Latest ${fmtText((status.latest_log || {}).event)}`;
    }

    async function refresh() {
      const response = await fetch("/api/dashboard", { cache: "no-store" });
      if (!response.ok) throw new Error(`dashboard request failed with ${response.status}`);
      const data = await response.json();
      renderMeta(data);
      renderPulse(data);
      renderRuntime(data);
      renderReports(data);
      renderSnapshots(data);
      renderTables(data);
      renderLogs(data);
    }

    refresh().catch((error) => {
      document.getElementById("generated-at").textContent = error.message;
    });
    setInterval(() => refresh().catch(() => {}), 5000);
  </script>
</body>
</html>
"""


@dataclass
class DashboardApp:
    bot: HouseBot

    def dashboard_payload(self) -> dict[str, object]:
        return self.bot.dashboard_data()


def _dashboard_bearer_token() -> str:
    return os.getenv("DASHBOARD_BEARER_TOKEN", "").strip()


def _is_authorized(auth_header: str | None) -> bool:
    expected = _dashboard_bearer_token()
    if not expected:
        return True
    if not auth_header:
        return False
    scheme, _, token = auth_header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return False
    return hmac.compare_digest(token, expected)


class DashboardRequestHandler(BaseHTTPRequestHandler):
    server: "DashboardHTTPServer"

    def do_GET(self) -> None:
        if not _is_authorized(self.headers.get("Authorization")):
            self._write_unauthorized()
            return
        if self.path in {"/", "/index.html"}:
            self._write_html(DASHBOARD_HTML)
            return
        if self.path == "/api/dashboard":
            self._write_json(self.server.app.dashboard_payload())
            return
        if self.path == "/api/health":
            self._write_json({"ok": True})
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def log_message(self, format: str, *args: object) -> None:
        return

    def _write_html(self, content: str) -> None:
        encoded = content.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _write_json(self, payload: dict[str, object]) -> None:
        encoded = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _write_unauthorized(self) -> None:
        encoded = json.dumps({"error": "Unauthorized"}).encode("utf-8")
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("WWW-Authenticate", "Bearer")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


class DashboardHTTPServer(HTTPServer):
    def __init__(self, server_address: tuple[str, int], app: DashboardApp) -> None:
        super().__init__(server_address, DashboardRequestHandler)
        self.app = app


def serve_dashboard(host: str = "127.0.0.1", port: int = 8765, settings: Settings | None = None) -> None:
    bot = HouseBot(settings=settings)
    app = DashboardApp(bot=bot)
    server = DashboardHTTPServer((host, port), app)
    print(f"House dashboard running at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        bot.close()
