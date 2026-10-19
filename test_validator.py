"""
test_validator.py — Unit tests for validate_trade().

All tests are pure Python and do not require a running Kafka broker.
The fixture valid_trade provides a correct baseline event; individual tests
mutate a copy of it to exercise each validation and normalisation path.
"""

import math

import pytest

from validator import validate_trade


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def valid_trade():
    """Return a complete, valid trade event matching the actual event schema."""
    return {
        "symbol": "BTC-USD",
        "price": 81562.81,
        "volume": 0.025,
        "timestamp": "2026-10-16T10:30:15Z",
        "price_usd": 81562.81,
        "price_inr": 7745204.44,
        "exchange_rate": 94.96,
        "rate_timestamp": "2026-10-16T00:00:00Z",
    }


# ---------------------------------------------------------------------------
# Valid event — smoke test and normalisation checks
# ---------------------------------------------------------------------------

class TestValidEvent:
    def test_returns_dict(self, valid_trade):
        result = validate_trade(valid_trade)
        assert isinstance(result, dict)

    def test_symbol_preserved(self, valid_trade):
        result = validate_trade(valid_trade)
        assert result["symbol"] == "BTC-USD"

    def test_timestamp_ends_with_z(self, valid_trade):
        result = validate_trade(valid_trade)
        assert result["timestamp"].endswith("Z")

    def test_rate_timestamp_ends_with_z(self, valid_trade):
        result = validate_trade(valid_trade)
        assert result["rate_timestamp"].endswith("Z")

    def test_numeric_fields_are_strings(self, valid_trade):
        result = validate_trade(valid_trade)
        for field in ("price", "volume", "price_usd", "price_inr", "exchange_rate"):
            assert isinstance(result[field], str), f"{field} should be a string after normalisation"

    def test_original_event_not_mutated(self, valid_trade):
        original = valid_trade.copy()
        validate_trade(valid_trade)
        assert valid_trade == original

    def test_all_eight_fields_present(self, valid_trade):
        result = validate_trade(valid_trade)
        for field in ("symbol", "price", "volume", "timestamp",
                      "price_usd", "price_inr", "exchange_rate", "rate_timestamp"):
            assert field in result


# ---------------------------------------------------------------------------
# Missing required fields
# ---------------------------------------------------------------------------

class TestMissingFields:
    @pytest.mark.parametrize("field", [
        "symbol", "price", "volume", "timestamp",
        "price_usd", "price_inr", "exchange_rate", "rate_timestamp",
    ])
    def test_missing_field_raises(self, valid_trade, field):
        del valid_trade[field]
        with pytest.raises(ValueError, match=f"Missing required field: {field}"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", [
        "symbol", "price", "volume", "timestamp",
        "price_usd", "price_inr", "exchange_rate", "rate_timestamp",
    ])
    def test_none_field_raises(self, valid_trade, field):
        valid_trade[field] = None
        with pytest.raises(ValueError, match=f"Missing required field: {field}"):
            validate_trade(valid_trade)

    def test_non_dict_event_raises(self):
        with pytest.raises(ValueError, match="Event must be a dictionary"):
            validate_trade("not a dict")

    def test_empty_dict_raises(self):
        with pytest.raises(ValueError, match="Missing required field: symbol"):
            validate_trade({})


# ---------------------------------------------------------------------------
# Symbol validation
# ---------------------------------------------------------------------------

class TestSymbol:
    def test_empty_string_raises(self, valid_trade):
        valid_trade["symbol"] = ""
        with pytest.raises(ValueError, match="symbol"):
            validate_trade(valid_trade)

    def test_whitespace_only_raises(self, valid_trade):
        valid_trade["symbol"] = "   "
        with pytest.raises(ValueError, match="symbol"):
            validate_trade(valid_trade)

    def test_non_string_raises(self, valid_trade):
        valid_trade["symbol"] = 12345
        with pytest.raises(ValueError, match="symbol"):
            validate_trade(valid_trade)

    def test_symbol_uppercased(self, valid_trade):
        valid_trade["symbol"] = "btc-usd"
        result = validate_trade(valid_trade)
        assert result["symbol"] == "BTC-USD"

    def test_symbol_stripped(self, valid_trade):
        valid_trade["symbol"] = "  BTC-USD  "
        result = validate_trade(valid_trade)
        assert result["symbol"] == "BTC-USD"


# ---------------------------------------------------------------------------
# Numeric field validation
# ---------------------------------------------------------------------------

NUMERIC_FIELDS = ["price", "volume", "price_usd", "price_inr", "exchange_rate"]


class TestNumericFields:
    @pytest.mark.parametrize("field", NUMERIC_FIELDS)
    def test_string_non_numeric_raises(self, valid_trade, field):
        valid_trade[field] = "abc"
        with pytest.raises(ValueError, match=f"{field} must be numeric"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", NUMERIC_FIELDS)
    def test_negative_raises(self, valid_trade, field):
        valid_trade[field] = -1.0
        with pytest.raises(ValueError, match=f"{field} must be positive and finite"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", NUMERIC_FIELDS)
    def test_zero_raises(self, valid_trade, field):
        valid_trade[field] = 0
        with pytest.raises(ValueError, match=f"{field} must be positive and finite"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", NUMERIC_FIELDS)
    def test_nan_raises(self, valid_trade, field):
        valid_trade[field] = math.nan
        with pytest.raises(ValueError, match=f"{field} must be positive and finite"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", NUMERIC_FIELDS)
    def test_positive_infinity_raises(self, valid_trade, field):
        valid_trade[field] = math.inf
        with pytest.raises(ValueError, match=f"{field} must be positive and finite"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", NUMERIC_FIELDS)
    def test_negative_infinity_raises(self, valid_trade, field):
        valid_trade[field] = -math.inf
        with pytest.raises(ValueError, match=f"{field} must be positive and finite"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", NUMERIC_FIELDS)
    def test_boolean_true_raises(self, valid_trade, field):
        valid_trade[field] = True
        with pytest.raises(ValueError, match="must be numeric, not a boolean"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", NUMERIC_FIELDS)
    def test_boolean_false_raises(self, valid_trade, field):
        valid_trade[field] = False
        with pytest.raises(ValueError, match="must be numeric, not a boolean"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", NUMERIC_FIELDS)
    def test_list_value_raises(self, valid_trade, field):
        valid_trade[field] = [100]
        with pytest.raises(ValueError, match=f"{field} must be numeric"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", NUMERIC_FIELDS)
    def test_numeric_string_accepted(self, valid_trade, field):
        valid_trade[field] = "100.5"
        result = validate_trade(valid_trade)
        assert result[field] == "100.5"

    def test_price_small_positive_accepted(self, valid_trade):
        valid_trade["price"] = 0.0001
        result = validate_trade(valid_trade)
        assert float(result["price"]) == pytest.approx(0.0001)


# ---------------------------------------------------------------------------
# Timestamp validation
# ---------------------------------------------------------------------------

TIMESTAMP_FIELDS = ["timestamp", "rate_timestamp"]


class TestTimestamps:
    @pytest.mark.parametrize("field", TIMESTAMP_FIELDS)
    def test_invalid_string_raises(self, valid_trade, field):
        valid_trade[field] = "not-a-timestamp"
        with pytest.raises(ValueError, match=f"Invalid {field}"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", TIMESTAMP_FIELDS)
    def test_date_only_raises(self, valid_trade, field):
        valid_trade[field] = "2026-10-16"
        with pytest.raises(ValueError, match=f"{field} must include a timezone"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", TIMESTAMP_FIELDS)
    def test_datetime_without_tz_raises(self, valid_trade, field):
        valid_trade[field] = "2026-10-16T10:30:15"
        with pytest.raises(ValueError, match=f"{field} must include a timezone"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", TIMESTAMP_FIELDS)
    def test_non_string_raises(self, valid_trade, field):
        valid_trade[field] = 1729072215
        with pytest.raises(ValueError, match=f"Invalid {field}"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", TIMESTAMP_FIELDS)
    def test_z_suffix_accepted(self, valid_trade, field):
        valid_trade[field] = "2026-10-16T10:30:15Z"
        result = validate_trade(valid_trade)
        assert result[field].endswith("Z")

    @pytest.mark.parametrize("field", TIMESTAMP_FIELDS)
    def test_offset_tz_accepted_and_converted_to_utc(self, valid_trade, field):
        # +05:30 is IST; the result should be in UTC.
        valid_trade[field] = "2026-10-16T16:00:15+05:30"
        result = validate_trade(valid_trade)
        assert result[field] == "2026-10-16T10:30:15Z"

    @pytest.mark.parametrize("field", TIMESTAMP_FIELDS)
    def test_normalised_timestamp_format(self, valid_trade, field):
        result = validate_trade(valid_trade)
        # Should match YYYY-MM-DDTHH:MM:SSZ exactly.
        import re
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", result[field])
