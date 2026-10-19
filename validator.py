"""
validator.py — Validates and normalizes market trade events.

A trade event must contain all eight required fields. Numeric fields must be
positive, finite numbers (booleans are explicitly rejected). Timestamp fields
must be parseable ISO-8601 strings that include timezone information. The
normalized result has the symbol in upper-case, numeric values as strings, and
timestamps converted to UTC in ISO-8601 format ending with 'Z'.
"""

import math
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation


# Fields every event must carry.
REQUIRED_FIELDS = [
    "symbol",
    "price",
    "volume",
    "timestamp",
    "price_usd",
    "price_inr",
    "exchange_rate",
    "rate_timestamp",
]

# Fields that must be positive, finite numbers.
NUMERIC_FIELDS = ["price", "volume", "price_usd", "price_inr", "exchange_rate"]

# Fields that must be timezone-aware ISO-8601 timestamps.
TIMESTAMP_FIELDS = ["timestamp", "rate_timestamp"]


def _validate_numeric(field: str, value) -> Decimal:
    """
    Return *value* as a Decimal if it is a positive, finite number.
    Booleans are rejected even though Python treats them as ints.
    Raises ValueError with a descriptive message on any problem.
    """
    # Booleans are a subclass of int — reject them explicitly.
    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric, not a boolean")

    # First check via float for nan/inf (Decimal doesn't parse float nan/inf from str).
    try:
        fval = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field} must be numeric")

    if math.isnan(fval) or math.isinf(fval) or fval <= 0:
        raise ValueError(f"{field} must be positive and finite")

    # Use Decimal(str(...)) to preserve the original decimal precision.
    try:
        return Decimal(str(value))
    except InvalidOperation:
        raise ValueError(f"{field} must be numeric")


def _parse_timestamp(field: str, value) -> datetime:
    """
    Parse *value* as an ISO-8601 timestamp and return a timezone-aware
    datetime. Raises ValueError with a descriptive message on failure.
    """
    if not isinstance(value, str):
        raise ValueError(f"Invalid {field}: must be a string")

    # Replace the trailing 'Z' shorthand so fromisoformat can parse it.
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
        Raw event containing the eight required market-data fields.

    Returns
    -------
    dict
        A normalized copy of the event:
        - symbol is stripped and upper-cased.
        - numeric values are stored as strings (preserving precision).
        - timestamps are converted to UTC ISO-8601 strings ending with 'Z'.

    Raises
    ------
    ValueError
        On any validation failure. The message identifies the failing field
        and reason.
    """
    if not isinstance(event, dict):
        raise ValueError("Event must be a dictionary")

    # --- Required fields -------------------------------------------------------
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            raise ValueError(f"Missing required field: {field}")

    # --- Symbol ----------------------------------------------------------------
    if not isinstance(event["symbol"], str) or not event["symbol"].strip():
        raise ValueError("symbol must be a non-empty string")

    # --- Numeric fields --------------------------------------------------------
    validated_numerics: dict[str, float] = {}
    for field in NUMERIC_FIELDS:
        validated_numerics[field] = _validate_numeric(field, event[field])

    # --- Timestamp fields ------------------------------------------------------
    validated_timestamps: dict[str, datetime] = {}
    for field in TIMESTAMP_FIELDS:
        validated_timestamps[field] = _parse_timestamp(field, event[field])

    # --- Build normalized output -----------------------------------------------
    normalized = event.copy()

    normalized["symbol"] = event["symbol"].strip().upper()

    for field, num in validated_numerics.items():
        # Convert via Decimal to preserve decimal precision without scientific notation.
        normalized[field] = str(num)

    for field, dt in validated_timestamps.items():
        utc_dt = dt.astimezone(timezone.utc)
        normalized[field] = utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return normalized
