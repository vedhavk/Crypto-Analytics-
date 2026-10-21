"""
test_currency_converter.py — Unit tests for CurrencyConverter.

All external HTTP calls are mocked using unittest.mock to ensure fast, deterministic tests.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from currency_converter import CurrencyConverter


@pytest.fixture
def mock_open_er_api_response():
    return {
        "result": "success",
        "provider": "https://www.exchangerate-api.com",
        "time_last_update_unix": 1729468951,
        "rates": {
            "INR": 94.96,
            "EUR": 0.92,
        },
    }


@pytest.fixture
def mock_frankfurter_response():
    return {
        "amount": 1.0,
        "base": "USD",
        "date": "2026-10-21",
        "rates": {
            "INR": 94.85,
        },
    }


class TestCurrencyConverterFetchAndCache:
    def test_fetch_primary_success(self, mock_open_er_api_response):
        converter = CurrencyConverter(refresh_interval=60.0)

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = json.dumps(mock_open_er_api_response).encode("utf-8")
            mock_resp.__enter__.return_value = mock_resp
            mock_urlopen.return_value = mock_resp

            rate, timestamp = converter.get_rate()

            assert rate == 94.96
            assert timestamp == "2024-10-21T00:02:31Z" or timestamp.endswith("Z")
            assert mock_urlopen.call_count == 1

    def test_caching_within_refresh_interval(self, mock_open_er_api_response):
        converter = CurrencyConverter(refresh_interval=300.0)

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = json.dumps(mock_open_er_api_response).encode("utf-8")
            mock_resp.__enter__.return_value = mock_resp
            mock_urlopen.return_value = mock_resp

            # First call fetches from API
            rate1, ts1 = converter.get_rate()
            assert rate1 == 94.96
            assert mock_urlopen.call_count == 1

            # Second call immediately after uses cache
            rate2, ts2 = converter.get_rate()
            assert rate2 == rate1
            assert ts2 == ts1
            assert mock_urlopen.call_count == 1  # No additional network request

    def test_refresh_after_interval_expires(self, mock_open_er_api_response):
        converter = CurrencyConverter(refresh_interval=10.0)

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = json.dumps(mock_open_er_api_response).encode("utf-8")
            mock_resp.__enter__.return_value = mock_resp
            mock_urlopen.return_value = mock_resp

            with patch("time.monotonic") as mock_time:
                mock_time.return_value = 100.0
                converter.get_rate()
                assert mock_urlopen.call_count == 1

                # Advance time past refresh interval
                mock_time.return_value = 115.0
                converter.get_rate()
                assert mock_urlopen.call_count == 2

    def test_fallback_api_used_when_primary_fails(self, mock_frankfurter_response):
        converter = CurrencyConverter()

        def fake_urlopen(req, *args, **kwargs):
            url = req.full_url if hasattr(req, "full_url") else str(req)
            if "open.er-api.com" in url:
                raise OSError("Primary connection timeout")
            mock_resp = MagicMock()
            mock_resp.read.return_value = json.dumps(mock_frankfurter_response).encode("utf-8")
            mock_resp.__enter__.return_value = mock_resp
            return mock_resp

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            rate, timestamp = converter.get_rate()
            assert rate == 94.85
            assert timestamp == "2026-10-21T00:00:00Z"

    def test_reuse_cached_rate_on_failure(self, mock_open_er_api_response):
        converter = CurrencyConverter(refresh_interval=1.0)

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = json.dumps(mock_open_er_api_response).encode("utf-8")
            mock_resp.__enter__.return_value = mock_resp
            mock_urlopen.return_value = mock_resp

            # Successful initial fetch
            rate, ts = converter.get_rate()
            assert rate == 94.96

        # Now simulate network failure after expiry
        with patch("urllib.request.urlopen", side_effect=OSError("Network down")):
            with patch("time.monotonic", return_value=100000.0):
                cached_rate, cached_ts = converter.get_rate()
                # Stale cached rate must be reused
                assert cached_rate == 94.96
                assert cached_ts == ts

    def test_return_none_when_no_cache_and_api_fails(self):
        converter = CurrencyConverter()

        with patch("urllib.request.urlopen", side_effect=OSError("DNS failure")):
            rate, timestamp = converter.get_rate()
            assert rate is None
            assert timestamp is None


class TestCurrencyConverterEnrichment:
    def test_enrich_event_with_valid_rate(self, mock_open_er_api_response):
        converter = CurrencyConverter()

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = json.dumps(mock_open_er_api_response).encode("utf-8")
            mock_resp.__enter__.return_value = mock_resp
            mock_urlopen.return_value = mock_resp

            trade = {
                "event_id": "1108688107",
                "symbol": "BTC-USD",
                "price": 80000.0,
                "volume": 0.05,
                "timestamp": "2026-10-21T05:00:00Z",
            }

            enriched = converter.enrich_event(trade)

            assert enriched["price_usd"] == 80000.0
            assert enriched["exchange_rate"] == 94.96
            assert enriched["rate_timestamp"] is not None
            assert enriched["price_inr"] == round(80000.0 * 94.96, 2)
            assert enriched["price_inr"] == 7596800.0

    def test_enrich_event_with_failed_rate_sets_none(self):
        converter = CurrencyConverter()

        with patch("urllib.request.urlopen", side_effect=OSError("Offline")):
            trade = {
                "event_id": "1108688107",
                "symbol": "BTC-USD",
                "price": 50000.0,
                "volume": 0.1,
                "timestamp": "2026-10-21T05:00:00Z",
            }

            enriched = converter.enrich_event(trade)

            assert enriched["price_usd"] == 50000.0
            assert enriched["exchange_rate"] is None
            assert enriched["rate_timestamp"] is None
            assert enriched["price_inr"] is None
