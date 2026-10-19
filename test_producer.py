"""
test_producer.py — Unit tests for producer.py.

Kafka is fully mocked here using unittest.mock so these tests never open a
network connection and do not require a running broker.
"""

from unittest.mock import MagicMock, patch

import pytest

from producer import publish_trade


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def valid_event():
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


def _make_mock_producer(topic="stock-ticks", partition=0, offset=1):
    """Build a minimal mock KafkaProducer whose .send() returns a resolved future."""
    metadata = MagicMock()
    metadata.topic = topic
    metadata.partition = partition
    metadata.offset = offset

    future = MagicMock()
    future.get.return_value = metadata

    producer = MagicMock()
    producer.send.return_value = future
    return producer


# ---------------------------------------------------------------------------
# publish_trade — happy path
# ---------------------------------------------------------------------------

class TestPublishTrade:
    def test_send_called_once(self, valid_event):
        producer = _make_mock_producer()
        publish_trade(producer, valid_event)
        producer.send.assert_called_once()

    def test_send_uses_correct_topic(self, valid_event):
        producer = _make_mock_producer()
        publish_trade(producer, valid_event)
        topic_used = producer.send.call_args[0][0]
        assert topic_used == "stock-ticks"

    def test_send_value_is_dict(self, valid_event):
        producer = _make_mock_producer()
        publish_trade(producer, valid_event)
        kwargs = producer.send.call_args[1]
        assert isinstance(kwargs["value"], dict)

    def test_send_value_symbol_normalised(self, valid_event):
        valid_event["symbol"] = "btc-usd"
        producer = _make_mock_producer()
        publish_trade(producer, valid_event)
        sent_value = producer.send.call_args[1]["value"]
        assert sent_value["symbol"] == "BTC-USD"

    def test_future_get_called(self, valid_event):
        producer = _make_mock_producer()
        publish_trade(producer, valid_event)
        future = producer.send.return_value
        future.get.assert_called_once()

    def test_prints_confirmation(self, valid_event, capsys):
        producer = _make_mock_producer()
        publish_trade(producer, valid_event)
        captured = capsys.readouterr()
        assert "BTC-USD" in captured.out
        assert "stock-ticks" in captured.out


# ---------------------------------------------------------------------------
# publish_trade — validation failures (Kafka should not be called)
# ---------------------------------------------------------------------------

class TestPublishTradeValidationErrors:
    def test_missing_field_does_not_call_send(self, valid_event):
        del valid_event["price"]
        producer = _make_mock_producer()
        with pytest.raises(ValueError):
            publish_trade(producer, valid_event)
        producer.send.assert_not_called()

    def test_invalid_symbol_does_not_call_send(self, valid_event):
        valid_event["symbol"] = ""
        producer = _make_mock_producer()
        with pytest.raises(ValueError):
            publish_trade(producer, valid_event)
        producer.send.assert_not_called()

    def test_negative_price_does_not_call_send(self, valid_event):
        valid_event["price"] = -1
        producer = _make_mock_producer()
        with pytest.raises(ValueError):
            publish_trade(producer, valid_event)
        producer.send.assert_not_called()

    def test_boolean_price_does_not_call_send(self, valid_event):
        valid_event["price"] = True
        producer = _make_mock_producer()
        with pytest.raises(ValueError):
            publish_trade(producer, valid_event)
        producer.send.assert_not_called()


# ---------------------------------------------------------------------------
# publish_trade — Kafka delivery failure
# ---------------------------------------------------------------------------

class TestPublishTradeKafkaError:
    def test_kafka_error_propagates(self, valid_event):
        from kafka.errors import KafkaError

        producer = _make_mock_producer()
        producer.send.return_value.get.side_effect = KafkaError("broker unavailable")

        with pytest.raises(KafkaError):
            publish_trade(producer, valid_event)
