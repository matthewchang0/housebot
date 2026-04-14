from __future__ import annotations

import io
import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date
from typing import Any

from pypdf import PdfReader

from .http import HttpClient
from .models import Filing
from .utils import normalize_symbol, normalize_whitespace, parse_amount_midpoint, parse_us_date


CLERK_BASE_URL = "https://disclosures-clerk.house.gov"
QUIVER_URL = "https://api.quiverquant.com/beta/live/housetrading"
CAPITOL_TRADES_URL = "https://www.capitoltrades.com/trades"
MEMBER_DATA_URL = "https://clerk.house.gov/xml/lists/MemberData.xml"

PTR_TABLE_RE = re.compile(
    r"(?P<owner>[A-Z]{1,3})\s+"
    r"(?P<asset>.+?)\s+"
    r"(?P<tx_type>[PSERX])\s+"
    r"(?P<tx_date>\d{2}/\d{2}/\d{4})\s*"
    r"(?P<filing_date>\d{2}/\d{2}/\d{4})\s*"
    r"(?P<amount>(?:\$[\d,]+\s*-\s*\$[\d,]+)|(?:Over\s+\$50,000,000))",
    re.S,
)
TICKER_RE = re.compile(r"\(([A-Z.\-]+)\)\s+\[(?P<asset_code>[A-Z]{2,4})\]")


def _safe_text(value: Any) -> str:
    return normalize_whitespace("" if value is None else str(value))


def _member_name(first: str, last: str) -> str:
    return normalize_whitespace(f"{first} {last}")


def _normalize_direction(raw: str | None) -> str | None:
    text = _safe_text(raw).upper()
    mapping = {
        "P": "PURCHASE",
        "PURCHASE": "PURCHASE",
        "BUY": "PURCHASE",
        "S": "SALE",
        "SALE": "SALE",
        "SELL": "SALE",
    }
    return mapping.get(text)


def _relation_from_owner(owner: str | None) -> str:
    code = _safe_text(owner).upper()
    if code == "SP":
        return "Spouse"
    if code == "DC":
        return "Dependent Child"
    return "Self"


def _looks_like_tradeable_equity(asset_name: str, asset_type: str) -> bool:
    joined = f"{asset_name} {asset_type}".upper()
    if any(token in joined for token in ("OPTION", "BOND", "WARRANT", "NOTE", "CRYPTO", "MUTUAL FUND")):
        return False
    return "[ST]" in joined or "ETF" in joined or "COMMON STOCK" in joined or asset_type.upper() in {"STOCK", "ETF"}


@dataclass(slots=True)
class ClerkIndexEntry:
    doc_id: str
    filing_year: int
    filing_date: date
    member_name: str
    state_dst: str


class ClerkClient:
    def __init__(self, http: HttpClient) -> None:
        self.http = http
        self._committee_map: dict[str, tuple[str, str]] | None = None

    def committee_map(self) -> dict[str, tuple[str, str]]:
        if self._committee_map is not None:
            return self._committee_map
        xml_text = self.http.get_text(MEMBER_DATA_URL)
        root = ET.fromstring(xml_text)
        committee_names: dict[str, str] = {}
        for committee in root.findall(".//committees/committee"):
            code = committee.attrib.get("comcode", "")
            name = _safe_text(committee.findtext("committee-fullname"))
            if code and name:
                committee_names[code] = name
        result: dict[str, tuple[str, str]] = {}
        for member in root.findall(".//members/member"):
            state_dst = _safe_text(member.findtext("statedistrict"))
            official_name = _safe_text(member.findtext("./member-info/official-name"))
            codes = [
                committee.attrib.get("comcode", "")
                for committee in member.findall("./committee-assignments/committee")
            ]
            committees = ", ".join(
                committee_names[code] for code in codes if code in committee_names
            )
            if state_dst:
                result[state_dst] = (official_name, committees)
        self._committee_map = result
        return result

    def list_recent_ptr_index_entries(self, years: list[int], since: date) -> list[ClerkIndexEntry]:
        entries: list[ClerkIndexEntry] = []
        for year in sorted(set(years)):
            url = f"{CLERK_BASE_URL}/public_disc/financial-pdfs/{year}FD.zip"
            archive = self.http.get_bytes(url)
            with io.BytesIO(archive) as handle:
                import zipfile

                with zipfile.ZipFile(handle) as bundle:
                    xml_name = f"{year}FD.xml"
                    root = ET.fromstring(bundle.read(xml_name))
            for member in root.findall(".//Member"):
                if _safe_text(member.findtext("FilingType")) != "P":
                    continue
                filing_date = parse_us_date(member.findtext("FilingDate"))
                if not filing_date or filing_date < since:
                    continue
                doc_id = _safe_text(member.findtext("DocID"))
                if not doc_id:
                    continue
                entries.append(
                    ClerkIndexEntry(
                        doc_id=doc_id,
                        filing_year=year,
                        filing_date=filing_date,
                        member_name=_member_name(
                            _safe_text(member.findtext("First")),
                            _safe_text(member.findtext("Last")),
                        ),
                        state_dst=_safe_text(member.findtext("StateDst")),
                    )
                )
        return entries

    def fetch_ptr_filings(self, entry: ClerkIndexEntry) -> list[Filing]:
        committee_name, committees = self.committee_map().get(entry.state_dst, ("", ""))
        member_name = committee_name or entry.member_name
        url = f"{CLERK_BASE_URL}/public_disc/ptr-pdfs/{entry.filing_year}/{entry.doc_id}.pdf"
        pdf_bytes = self.http.get_bytes(url)
        reader = PdfReader(io.BytesIO(pdf_bytes))
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
        clean_text = normalize_whitespace(text)
        if "* For the complete list of asset type abbreviations" in clean_text:
            clean_text = clean_text.split("* For the complete list of asset type abbreviations", 1)[0]
        filings: list[Filing] = []
        for match in PTR_TABLE_RE.finditer(clean_text):
            tx_type = _safe_text(match.group("tx_type"))
            direction = _normalize_direction(tx_type)
            if direction not in {"PURCHASE", "SALE"}:
                continue
            asset = _safe_text(match.group("asset"))
            ticker_match = TICKER_RE.search(asset)
            if not ticker_match:
                filings.append(
                    Filing(
                        member_name=member_name,
                        relation=_relation_from_owner(match.group("owner")),
                        ticker="",
                        direction=direction,
                        tx_date=parse_us_date(match.group("tx_date")),
                        filing_date=entry.filing_date,
                        amount_range=_safe_text(match.group("amount")),
                        amount_midpoint=0.0,
                        committee=committees or None,
                        asset_type="UNKNOWN",
                        status="FLAGGED",
                        source="clerk",
                        raw_text=asset,
                    )
                )
                continue
            asset_code = ticker_match.group("asset_code")
            amount_range = _safe_text(match.group("amount"))
            midpoint = parse_amount_midpoint(amount_range)
            ticker = normalize_symbol(ticker_match.group(1))
            if midpoint is None or not ticker or not _looks_like_tradeable_equity(asset, asset_code):
                filings.append(
                    Filing(
                        member_name=member_name,
                        relation=_relation_from_owner(match.group("owner")),
                        ticker=ticker,
                        direction=direction,
                        tx_date=parse_us_date(match.group("tx_date")),
                        filing_date=entry.filing_date,
                        amount_range=amount_range,
                        amount_midpoint=midpoint or 0.0,
                        committee=committees or None,
                        asset_type=asset_code,
                        status="FLAGGED",
                        source="clerk",
                        raw_text=asset,
                    )
                )
                continue
            filings.append(
                Filing(
                    member_name=member_name,
                    relation=_relation_from_owner(match.group("owner")),
                    ticker=ticker,
                    direction=direction,
                    tx_date=parse_us_date(match.group("tx_date")),
                    filing_date=entry.filing_date,
                    amount_range=amount_range,
                    amount_midpoint=midpoint,
                    committee=committees or None,
                    asset_type="Stock",
                    source="clerk",
                    raw_text=asset,
                )
            )
        return filings


class QuiverClient:
    def __init__(self, http: HttpClient, api_key: str) -> None:
        self.http = http
        self.api_key = api_key

    def fetch(self) -> list[Filing]:
        if not self.api_key:
            return []
        payload = self.http.get_json(
            QUIVER_URL,
            headers={"Authorization": f"Bearer {self.api_key}"},
        )
        if not isinstance(payload, list):
            return []
        filings: list[Filing] = []
        for row in payload:
            filing = _normalize_aggregator_row(row, source="quiver")
            if filing:
                filings.append(filing)
        return filings


class CapitolTradesClient:
    def __init__(self, http: HttpClient) -> None:
        self.http = http

    def fetch(self, since: date | None = None) -> list[Filing]:
        filings: list[Filing] = []
        page = 1
        while True:
            url = CAPITOL_TRADES_URL if page == 1 else f"{CAPITOL_TRADES_URL}?page={page}"
            rows = _extract_capitol_trades_rows(self.http.get_text(url))
            if not rows:
                break
            page_dates: list[date] = []
            for row in rows:
                published = _parse_row_date(row.get("pubDate"))
                if published is not None:
                    page_dates.append(published)
                filing = _normalize_aggregator_row(row, source="capitoltrades")
                if filing:
                    filings.append(filing)
            if since and page_dates and min(page_dates) < since:
                break
            page += 1
        return filings


def _pick(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if "." in key:
            current: Any = row
            ok = True
            for part in key.split("."):
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    ok = False
                    break
            if ok and current not in (None, ""):
                return current
        elif key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _normalize_aggregator_row(row: Any, source: str) -> Filing | None:
    if not isinstance(row, dict):
        return None
    chamber = _safe_text(_pick(row, "chamber", "Chamber", "office", "Office")).lower()
    if chamber and "house" not in chamber and "representative" not in chamber:
        return None
    direction = _normalize_direction(_pick(row, "transaction", "Transaction", "type", "Type", "txType"))
    if direction not in {"PURCHASE", "SALE"}:
        return None
    ticker = normalize_symbol(
        _safe_text(_pick(row, "ticker", "Ticker", "asset.ticker", "assetTicker", "issuer.issuerTicker"))
    )
    amount_range = _safe_text(
        _pick(row, "amount_range", "range", "Range", "amount", "Amount", "amounts")
    )
    midpoint = parse_amount_midpoint(amount_range)
    if midpoint is None:
        amount_value = _pick(row, "value", "amountValue")
        if isinstance(amount_value, (int, float)):
            midpoint = float(amount_value)
            amount_range = f"${midpoint:,.0f}"
    filing_date = _parse_row_date(_pick(row, "filing_date", "Date", "reportDate", "disclosureDate", "pubDate"))
    tx_date = _parse_row_date(_pick(row, "tx_date", "TransactionDate", "transactionDate", "txDate"))
    member_name = _safe_text(
        _pick(row, "member_name", "Representative", "representative", "politician.name", "member")
    )
    if not member_name:
        member_name = _member_name(
            _safe_text(_pick(row, "politician.firstName")),
            _safe_text(_pick(row, "politician.lastName")),
        )
    relation = _safe_text(_pick(row, "relation", "owner", "Owner")) or "Self"
    asset_type = _safe_text(_pick(row, "asset_type", "asset.type", "asset", "AssetType")) or "Stock"
    if not filing_date or not member_name or not ticker or midpoint is None:
        return None
    if not _looks_like_tradeable_equity(ticker, asset_type):
        return None
    committee = _safe_text(_pick(row, "committee", "committees"))
    return Filing(
        member_name=member_name,
        relation=relation.title(),
        ticker=ticker,
        direction=direction,
        tx_date=tx_date,
        filing_date=filing_date,
        amount_range=amount_range,
        amount_midpoint=midpoint,
        committee=committee or None,
        asset_type=asset_type,
        source=source,
        raw_text=_safe_text(row),
    )


def _extract_capitol_trades_rows(html: str) -> list[dict[str, Any]]:
    marker = r"\"data\":["
    start = html.find(marker)
    if start < 0:
        return []
    array_start = start + len(r"\"data\":")
    depth = 0
    array_end: int | None = None
    for index, char in enumerate(html[array_start:], start=array_start):
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                array_end = index + 1
                break
    if array_end is None:
        return []
    raw_array = html[array_start:array_end]
    decoded = raw_array.encode("utf-8").decode("unicode_escape")
    payload = json.loads(decoded)
    return [row for row in payload if isinstance(row, dict)]


def _parse_row_date(raw: Any) -> date | None:
    if raw is None:
        return None
    text = _safe_text(raw)
    if "T" in text:
        text = text.split("T", 1)[0]
    return parse_us_date(text)
