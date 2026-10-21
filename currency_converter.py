"""
currency_converter.py — Handles USD to INR conversion with caching and periodic refresh.

Integrates free, open exchange-rate APIs without requiring an API key:
- Primary API: Open ExchangeRate API (https://open.er-api.com/v6/latest/USD)
- Fallback API: Frankfurter API (https://api.frankfurter.app/latest?from=USD&to=INR)

Features:
- In-memory caching: Caches the latest valid exchange rate and timestamp.
- Periodic refresh: Only contacts external APIs when the cache TTL has expired
  (default: 300 seconds / 5 minutes), preventing unnecessary network calls on live trades.
- Resilient failure handling:
  * If an API request fails, reuses the last valid cached rate if available.
  * If no valid cached rate is available (e.g. during an initial network outage),
    gracefully sets price_inr, exchange_rate, and rate_timestamp to None (null).
- Thread-safe: Uses threading.Lock to support concurrent event processing.
"""

import json
import logging
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)

PRIMARY_API_URL = "https://open.er-api.com/v6/latest/USD"
FALLBACK_API_URL = "https://api.frankfurter.app/latest?from=USD&to=INR"
DEFAULT_REFRESH_INTERVAL_SECONDS = 300.0  # 5 minutes


class CurrencyConverter:
    """Manages USD/INR exchange rate retrieval, caching, and trade enrichment."""

    def __init__(
        self,
        refresh_interval: float = DEFAULT_REFRESH_INTERVAL_SECONDS,
        primary_url: str = PRIMARY_API_URL,
        fallback_url: str = FALLBACK_API_URL,
        timeout: float = 5.0,
    ):
        self.refresh_interval = refresh_interval
        self.primary_url = primary_url
        self.fallback_url = fallback_url
        self.timeout = timeout

        self._cached_rate: Optional[float] = None
        self._cached_timestamp: Optional[str] = None
        self._last_refresh_time: float = 0.0
        self._lock = threading.Lock()

    def _fetch_from_primary(self) -> Tuple[float, str]:
        """Fetch USD-to-INR rate from open.er-api.com."""
        req = urllib.request.Request(
            self.primary_url,
            headers={"User-Agent": "CryptoAnalytics/1.0"},
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        if data.get("result") != "success" and "rates" not in data:
            raise ValueError(f"Primary API returned unexpected format: {data}")

        rates = data.get("rates", {})
        if "INR" not in rates:
            raise ValueError("Primary API response does not contain 'INR'")

        rate = float(rates["INR"])

        # Format rate timestamp to UTC ISO-8601 ending with Z
        if "time_last_update_unix" in data:
            dt = datetime.fromtimestamp(data["time_last_update_unix"], tz=timezone.utc)
            rate_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif "time_last_update_utc" in data:
            rate_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            rate_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        return rate, rate_timestamp

    def _fetch_from_fallback(self) -> Tuple[float, str]:
        """Fetch USD-to-INR rate from frankfurter.app."""
        req = urllib.request.Request(
            self.fallback_url,
            headers={"User-Agent": "CryptoAnalytics/1.0"},
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        rates = data.get("rates", {})
        if "INR" not in rates:
            raise ValueError("Fallback API response does not contain 'INR'")

        rate = float(rates["INR"])
        date_str = data.get("date")
        if date_str:
            rate_timestamp = f"{date_str}T00:00:00Z"
        else:
            rate_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        return rate, rate_timestamp

    def _fetch_fresh_rate(self) -> Tuple[float, str]:
        """Attempt primary API first, falling back to secondary if needed."""
        try:
            return self._fetch_from_primary()
        except Exception as primary_exc:
            logger.warning(
                "Primary exchange rate API failed (%s); trying fallback...",
                primary_exc,
            )
            try:
                return self._fetch_from_fallback()
            except Exception as fallback_exc:
                raise RuntimeError(
                    f"All exchange rate APIs failed. Primary: {primary_exc}; Fallback: {fallback_exc}"
                ) from fallback_exc

    def get_rate(self) -> Tuple[Optional[float], Optional[str]]:
        """
        Get the current USD/INR exchange rate and timestamp.

        Returns (rate, timestamp).
        If the cache is valid, returns cached data.
        If cache has expired, attempts to refresh.
        If refresh fails and cache exists, returns stale cached rate.
        If refresh fails and no cache exists, returns (None, None).
        """
        now = time.monotonic()
        with self._lock:
            # Check if cache is still fresh
            if self._cached_rate is not None and (now - self._last_refresh_time) < self.refresh_interval:
                return self._cached_rate, self._cached_timestamp

            # Attempt to refresh
            try:
                rate, ts = self._fetch_fresh_rate()
                self._cached_rate = rate
                self._cached_timestamp = ts
                self._last_refresh_time = now
                logger.info("Updated USD/INR exchange rate: %.4f (%s)", rate, ts)
                return self._cached_rate, self._cached_timestamp
            except Exception as exc:
                if self._cached_rate is not None:
                    logger.warning(
                        "Exchange rate refresh failed (%s). Reusing cached rate: %.4f",
                        exc,
                        self._cached_rate,
                    )
                    return self._cached_rate, self._cached_timestamp
                logger.error(
                    "Exchange rate API request failed and no cached rate is available: %s",
                    exc,
                )
                return None, None

    def enrich_event(self, event: dict) -> dict:
        """
        Enrich a trade event dictionary with USD and INR fields.

        Formula:
            price_inr = round(price_usd * exchange_rate, 2)

        If exchange_rate is unavailable (None), price_inr, exchange_rate,
        and rate_timestamp are set to None.
        """
        price_usd = float(event.get("price_usd") or event["price"])
        rate, rate_timestamp = self.get_rate()

        enriched = event.copy()
        enriched["price_usd"] = price_usd

        if rate is not None:
            enriched["exchange_rate"] = rate
            enriched["rate_timestamp"] = rate_timestamp
            enriched["price_inr"] = round(price_usd * rate, 2)
        else:
            enriched["exchange_rate"] = None
            enriched["rate_timestamp"] = None
            enriched["price_inr"] = None

        return enriched
