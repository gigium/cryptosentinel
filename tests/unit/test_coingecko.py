from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.coingecko import add_ingestion_metadata, fetch_markets


SAMPLE_MARKET_DATA = [
    {"id": "bitcoin", "symbol": "btc", "current_price": 60000},
    {"id": "ethereum", "symbol": "eth", "current_price": 3000},
]


class TestFetchMarkets:
    def test_returns_list_of_dicts(self):
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_MARKET_DATA

        with patch("src.ingestion.coingecko.requests.get", return_value=mock_response):
            result = fetch_markets()

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["id"] == "bitcoin"

    def test_raises_on_http_error(self):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("404")

        with patch("src.ingestion.coingecko.requests.get", return_value=mock_response):
            with pytest.raises(Exception, match="404"):
                fetch_markets()

    def test_calls_correct_url_and_params(self):
        mock_response = MagicMock()
        mock_response.json.return_value = []

        with patch("src.ingestion.coingecko.requests.get", return_value=mock_response) as mock_get:
            fetch_markets()

        mock_get.assert_called_once_with(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": 10, "page": 1},
            timeout=10,
        )


class TestAddIngestionMetadata:
    def test_adds_ingested_at_and_ingestion_date(self):
        data = [{"id": "bitcoin"}, {"id": "ethereum"}]
        result = add_ingestion_metadata(data)

        for row in result:
            assert "ingested_at" in row
            assert "ingestion_date" in row

    def test_ingested_at_is_iso_format(self):
        data = [{"id": "bitcoin"}]
        result = add_ingestion_metadata(data)

        from datetime import datetime
        datetime.fromisoformat(result[0]["ingested_at"])

    def test_ingestion_date_is_date_string(self):
        data = [{"id": "bitcoin"}]
        result = add_ingestion_metadata(data)

        from datetime import date
        date.fromisoformat(result[0]["ingestion_date"])

    def test_mutates_and_returns_input_list(self):
        data = [{"id": "bitcoin"}]
        result = add_ingestion_metadata(data)

        assert result is data

    def test_all_rows_get_same_timestamp(self):
        data = [{"id": "bitcoin"}, {"id": "ethereum"}]
        result = add_ingestion_metadata(data)

        assert result[0]["ingested_at"] == result[1]["ingested_at"]
