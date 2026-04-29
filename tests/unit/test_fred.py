from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.fred import add_ingestion_metadata, fetch_series, fetch_all

SAMPLE_OBSERVATIONS = {
    "observations": [
        {"date": "2026-04-01", "value": "5.33"},
        {"date": "2026-03-01", "value": "5.33"},
    ]
}

MISSING_VALUE_OBSERVATIONS = {
    "observations": [
        {"date": "2026-04-01", "value": "."},
    ]
}


class TestFetchSeries:
    def test_returns_list_of_dicts(self):
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_OBSERVATIONS

        with patch("src.ingestion.fred.requests.get", return_value=mock_response):
            result = fetch_series("FEDFUNDS", "test_key")

        assert len(result) == 2
        assert result[0]["series_id"] == "FEDFUNDS"
        assert result[0]["indicator"] == "fed_funds_rate"

    def test_parses_value_as_float(self):
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_OBSERVATIONS

        with patch("src.ingestion.fred.requests.get", return_value=mock_response):
            result = fetch_series("FEDFUNDS", "test_key")

        assert isinstance(result[0]["value"], float)

    def test_missing_value_becomes_none(self):
        mock_response = MagicMock()
        mock_response.json.return_value = MISSING_VALUE_OBSERVATIONS

        with patch("src.ingestion.fred.requests.get", return_value=mock_response):
            result = fetch_series("FEDFUNDS", "test_key")

        assert result[0]["value"] is None

    def test_raises_on_http_error(self):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("403")

        with patch("src.ingestion.fred.requests.get", return_value=mock_response):
            with pytest.raises(Exception, match="403"):
                fetch_series("FEDFUNDS", "test_key")

    def test_calls_correct_params(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"observations": []}

        with patch("src.ingestion.fred.requests.get", return_value=mock_response) as mock_get:
            fetch_series("CPIAUCSL", "my_key", limit=5)

        _, kwargs = mock_get.call_args
        assert kwargs["params"]["series_id"] == "CPIAUCSL"
        assert kwargs["params"]["api_key"] == "my_key"
        assert kwargs["params"]["limit"] == 5


class TestFetchAll:
    def test_fetches_all_three_series(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"observations": []}

        with patch("src.ingestion.fred.requests.get", return_value=mock_response) as mock_get:
            fetch_all("test_key")

        assert mock_get.call_count == 3


class TestAddIngestionMetadata:
    def test_adds_metadata_fields(self):
        data = [{"series_id": "FEDFUNDS", "value": 5.33}]
        result = add_ingestion_metadata(data)
        assert "ingested_at" in result[0]
        assert "ingestion_date" in result[0]
