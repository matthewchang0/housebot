from datetime import date

from house.sources import _extract_capitol_trades_rows, _normalize_aggregator_row


def test_extract_capitol_trades_rows_reads_embedded_next_payload() -> None:
    html = """
    <script>
    self.__next_f.push([1,"stub \\\"data\\\":[{\\\"_txId\\\":10000064896,\\\"chamber\\\":\\\"house\\\",\\\"issuer\\\":{\\\"issuerTicker\\\":\\\"MSFT:US\\\",\\\"issuerName\\\":\\\"Microsoft Corp\\\"},\\\"owner\\\":\\\"self\\\",\\\"politician\\\":{\\\"firstName\\\":\\\"Nancy\\\",\\\"lastName\\\":\\\"Pelosi\\\"},\\\"pubDate\\\":\\\"2026-04-14T15:15:08Z\\\",\\\"txDate\\\":\\\"2026-03-20\\\",\\\"txType\\\":\\\"buy\\\",\\\"value\\\":8000}] tail"]);
    </script>
    """

    rows = _extract_capitol_trades_rows(html)

    assert len(rows) == 1
    assert rows[0]["_txId"] == 10000064896
    assert rows[0]["issuer"]["issuerTicker"] == "MSFT:US"


def test_normalize_aggregator_row_handles_capitol_trades_shape_and_aliases() -> None:
    filing = _normalize_aggregator_row(
        {
            "chamber": "house",
            "issuer": {
                "issuerName": "Block Inc",
                "issuerTicker": "SQ:US",
            },
            "owner": "self",
            "politician": {
                "firstName": "Gilbert",
                "lastName": "Cisneros, Jr.",
            },
            "pubDate": "2026-04-14T15:15:08Z",
            "txDate": "2026-03-20",
            "txType": "buy",
            "value": 8000,
        },
        source="capitoltrades",
    )

    assert filing is not None
    assert filing.member_name == "Gilbert Cisneros, Jr."
    assert filing.ticker == "XYZ"
    assert filing.direction == "PURCHASE"
    assert filing.tx_date == date(2026, 3, 20)
    assert filing.filing_date == date(2026, 4, 14)
    assert filing.amount_midpoint == 8000.0
