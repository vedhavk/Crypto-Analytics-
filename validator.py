"""
validator.py — Validates and normalizes market trade events.

A trade event must contain all nine required fields. Numeric fields must be
positive, finite numbers (booleans are explicitly rejected).
price_inr, exchange_rate, and rate_timestamp are allowed to be None (null)
when conversion data is unavailable.
Timestamp fields must be parseable ISO-8601 strings that include timezone
information. The normalized result preserves numeric values as JSON numbers
(floats), uppercases symbol, formats event_id as a string, and normalizes
timestamps to UTC in ISO-8601 format ending with 'Z'.
"""

import math
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional


# Fields every event must carry in its schema.
REQUIRED_FIELDS = [
    "event_id",
    "symbol",
    "price",
    "volume",
    "timestamp",
    "price_usd",
    "price_inr",
    "exchange_rate",
    "rate_timestamp",
]

# Fields that cannot be null/None.
MANDATORY_NON_NULL_FIELDS = [
    "event_id",
    "symbol",
    "price",
    "volume",
    "timestamp",
    "price_usd",
]

# Numeric fields that cannot be null.
NON_NULL_NUMERIC_FIELDS = ["price", "volume", "price_usd"]

# Numeric fields that can be null if conversion data is unavailable.
NULLABLE_NUMERIC_FIELDS = ["price_inr", "exchange_rate"]

# Timestamp fields.
MANDATORY_TIMESTAMP_FIELDS = ["timestamp"]
NULLABLE_TIMESTAMP_FIELDS = ["rate_timestamp"]


def _validate_numeric(field: str, value: Any) -> float:
    """
    Validate that *value* is a positive, finite number.
    Booleans are rejected even though Python treats them as ints.
    Returns float value.
    Raises ValueError with a descriptive message on any problem.
    """
    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric, not a boolean")

    try:
        fval = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field} must be numeric")

    if math.isnan(fval) or math.isinf(fval) or fval <= 0:
        raise ValueError(f"{field} must be positive and finite")

    # Verify decimal parsing doesn't fail
    try:
        Decimal(str(value))
    except InvalidOperation:
        raise ValueError(f"{field} must be numeric")

    return fval


def _parse_timestamp(field: str, value: Any) -> datetime:
    """
    Parse *value* as an ISO-8601 timestamp and return a timezone-aware
    datetime. Raises ValueError with a descriptive message on failure.
    """
    if not isinstance(value, str):
        raise ValueError(f"Invalid {field}: must be a string")

    # Replace the trailing 'Z' shorthand so fromisoformat can parse it across Python versions.
    normalised = value.strip().replace("Z", "+00:00")

    try:
        dt = datetime.fromisoformat(normalised)
    except ValueError:
        raise ValueError(f"Invalid {field}: '{value}' is not a valid ISO-8601 timestamp")

    if dt.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")

    return dt


def validate_trade(event: dict) -> dict:
    """
    Validate and normalize a market trade event.

    Parameters
    ----------
    event : dict
        Raw event containing the market-data fields.

    Returns
    -------
    dict
        A normalized copy of the event:
        - event_id is a non-empty string.
        - symbol is stripped and upper-cased.
        - numeric values are preserved as JSON numbers (floats).
        - price_inr, exchange_rate, and rate_timestamp may be None.
        - timestamps are converted to UTC ISO-8601 strings ending with 'Z'.

    Raises
    ------
    ValueError
        On any validation failure. The message identifies the failing field
        and reason.
    """
    if not isinstance(event, dict):
        raise ValueError("Event must be a dictionary")

    # --- Check field presence --------------------------------------------------
    for field in REQUIRED_FIELDS:
        if field not in event:
            raise ValueError(f"Missing required field: {field}")

    # Check non-null mandatory fields
    for field in MANDATORY_NON_NULL_FIELDS:
        if event[field] is None:
            raise ValueError(f"Missing required field: {field}")

    # --- event_id --------------------------------------------------------------
    event_id_val = event["event_id"]
    if isinstance(event_id_val, bool):
        raise ValueError("event_id must be a string or integer, not a boolean")
    if not isinstance(event_id_val, (str, int)):
        raise ValueError("event_id must be a string or integer")
    event_id_str = str(event_id_val).strip()
    if not event_id_str:
        raise ValueError("event_id must be a non-empty string")

    # --- Symbol ----------------------------------------------------------------
    if not isinstance(event["symbol"], str) or not event["symbol"].strip():
        raise ValueError("symbol must be a non-empty string")

    # --- Non-null Numeric fields -----------------------------------------------
    validated_numerics: dict[str, float] = {}
    for field in NON_NULL_NUMERIC_FIELDS:
        validated_numerics[field] = _validate_numeric(field, event[field])

    # --- Nullable Numeric fields -----------------------------------------------
    nullable_numerics: dict[str, Optional[float]] = {}
    for field in NULLABLE_NUMERIC_FIELDS:
        val = event[field]
        if val is None:
            nullable_numerics[field] = None
        else:
            nullable_numerics[field] = _validate_numeric(field, val)

    # --- Mandatory Timestamp field ---------------------------------------------
    dt_timestamp = _parse_timestamp("timestamp", event["timestamp"])

    # --- Nullable Timestamp field ----------------------------------------------
    rate_dt: Optional[datetime] = None
    if event["rate_timestamp"] is not None:
        rate_dt = _parse_timestamp("rate_timestamp", event["rate_timestamp"])

    # --- Build normalized output -----------------------------------------------
    normalized = event.copy()

    normalized["event_id"] = event_id_str
    normalized["symbol"] = event["symbol"].strip().upper()

    for field, num in validated_numerics.items():
        normalized[field] = num

    for field, num in nullable_numerics.items():
        normalized[field] = num

    utc_dt = dt_timestamp.astimezone(timezone.utc)
    normalized["timestamp"] = utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    if rate_dt is not None:
        utc_rate_dt = rate_dt.astimezone(timezone.utc)
        normalized["rate_timestamp"] = utc_rate_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        normalized["rate_timestamp"] = None

    return normalized
