from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.binance import add_ingestion_metadata, fetch_klines, _parse_kline, fetch_all

SAMPLE_KLINE = [
    1714000000000, "60000.0", "61000.0", "59500.0", "60500.0", "120.5",
    1714003599999, "7260000.0", 4200, "60.2", "3628440.0", "0"
]


class TestParseKline:
    def test_returns_expected_keys(self):
        result = _parse_kline(SAMPLE_KLINE, "BTCUSDT", "1h")
        for key in ("symbol", "interval", "open_time", "open", "high", "low", "close", "volume", "num_trades"):
            assert key in result

    def test_casts_to_correct_types(self):
        result = _parse_kline(SAMPLE_KLINE, "BTCUSDT", "1h")
        assert isinstance(result["open"], float)
        assert isinstance(result["volume"], float)
        assert isinstance(result["num_trades"], int)

    def test_symbol_and_interval_preserved(self):
        result = _parse_kline(SAMPLE_KLINE, "ETHUSDT", "1m")
        assert result["symbol"] == "ETHUSDT"
        assert result["interval"] == "1m"


class TestFetchKlines:
    def test_returns_parsed_list(self):
        mock_response = MagicMock()
        mock_response.json.return_value = [SAMPLE_KLINE, SAMPLE_KLINE]

        with patch("src.ingestion.binance.requests.get", return_value=mock_response):
            result = fetch_klines("BTCUSDT", "1h")

        assert len(result) == 2
        assert result[0]["symbol"] == "BTCUSDT"

    def test_raises_on_http_error(self):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("500")

        with patch("src.ingestion.binance.requests.get", return_value=mock_response):
            with pytest.raises(Exception, match="500"):
                fetch_klines("BTCUSDT", "1h")

    def test_calls_correct_url(self):
        mock_response = MagicMock()
        mock_response.json.return_value = []

        with patch("src.ingestion.binance.requests.get", return_value=mock_response) as mock_get:
            fetch_klines("BTCUSDT", "1m", limit=5)

        mock_get.assert_called_once_with(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": 5},
            timeout=10,
        )


class TestFetchAll:
    def test_fetches_all_symbol_interval_combinations(self):
        mock_response = MagicMock()
        mock_response.json.return_value = [SAMPLE_KLINE]

        with patch("src.ingestion.binance.requests.get", return_value=mock_response) as mock_get:
            result = fetch_all()

        # 2 symbols × 2 intervals = 4 calls
        assert mock_get.call_count == 4
        assert len(result) == 4


class TestAddIngestionMetadata:
    def test_adds_metadata_fields(self):
        data = [{"symbol": "BTCUSDT"}]
        result = add_ingestion_metadata(data)
        assert "ingested_at" in result[0]
        assert "ingestion_date" in result[0]

    def test_all_rows_share_timestamp(self):
        data = [{"symbol": "BTCUSDT"}, {"symbol": "ETHUSDT"}]
        result = add_ingestion_metadata(data)
        assert result[0]["ingested_at"] == result[1]["ingested_at"]
