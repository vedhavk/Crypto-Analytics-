"""
test_coinbase_client.py — Unit tests for Coinbase WebSocket client and message parsing.
"""

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from coinbase_client import (
    CoinbaseWebSocketClient,
    parse_coinbase_message,
)


# ---------------------------------------------------------------------------
# Message Parsing Tests
# ---------------------------------------------------------------------------

class TestParseCoinbaseMessage:
    def test_parse_match_message(self):
        msg = json.dumps({
            "type": "match",
            "trade_id": 1108688107,
            "maker_order_id": "8ee3bf46-c711-4a81-bba4-d8ba21b5b8b2",
            "taker_order_id": "a4a3afd7-0e94-40fa-8ad5-82321092fa35",
            "side": "buy",
            "size": "0.025",
            "price": "81562.81",
            "product_id": "BTC-USD",
            "sequence": 138070906749,
            "time": "2026-10-21T05:15:01.708863Z",
        })

        event = parse_coinbase_message(msg)
        assert event is not None
        assert event["event_id"] == "1108688107"
        assert event["symbol"] == "BTC-USD"
        assert event["price"] == 81562.81
        assert event["volume"] == 0.025
        assert event["price_usd"] == 81562.81
        assert event["timestamp"] == "2026-10-21T05:15:01.708863Z"
        assert event["channel_type"] == "match"

    def test_parse_last_match_message(self):
        msg = json.dumps({
            "type": "last_match",
            "trade_id": 1108688106,
            "maker_order_id": "8ee3bf46-c711-4a81-bba4-d8ba21b5b8b2",
            "taker_order_id": "bb804dcd-84ca-44ea-820d-bea1dbd73e8b",
            "side": "buy",
            "size": "0.00000004",
            "price": "82797.39",
            "product_id": "BTC-USD",
            "sequence": 138070906685,
            "time": "2026-10-21T05:15:01.451522Z",
        })

        event = parse_coinbase_message(msg)
        assert event is not None
        assert event["event_id"] == "1108688106"
        assert event["symbol"] == "BTC-USD"
        assert event["price"] == 82797.39
        assert event["channel_type"] == "match"

    def test_parse_ticker_message(self):
        msg = json.dumps({
            "type": "ticker",
            "sequence": 138070906685,
            "product_id": "ETH-USD",
            "price": "2442.84",
            "open_24h": "2400.00",
            "volume_24h": "15000.5",
            "low_24h": "2390.00",
            "high_24h": "2460.00",
            "trade_id": 849588837,
            "last_size": "0.55",
            "time": "2026-10-21T05:15:01.451522Z",
        })

        event = parse_coinbase_message(msg)
        assert event is not None
        assert event["event_id"] == "849588837"
        assert event["symbol"] == "ETH-USD"
        assert event["price"] == 2442.84
        assert event["volume"] == 0.55
        assert event["channel_type"] == "ticker"

    def test_ticker_uses_sequence_when_trade_id_missing(self):
        msg = json.dumps({
            "type": "ticker",
            "sequence": 999888777,
            "product_id": "BTC-USD",
            "price": "80000.00",
            "last_size": "0.1",
            "time": "2026-10-21T05:15:01Z",
        })

        event = parse_coinbase_message(msg)
        assert event is not None
        assert event["event_id"] == "999888777"

    def test_distinguishes_ticker_from_match(self):
        ticker_msg = json.dumps({
            "type": "ticker",
            "trade_id": 1,
            "product_id": "BTC-USD",
            "price": "100.0",
            "last_size": "1.0",
            "time": "2026-10-21T00:00:00Z",
        })
        match_msg = json.dumps({
            "type": "match",
            "trade_id": 2,
            "product_id": "BTC-USD",
            "price": "100.0",
            "size": "1.0",
            "time": "2026-10-21T00:00:00Z",
        })

        e1 = parse_coinbase_message(ticker_msg)
        e2 = parse_coinbase_message(match_msg)
        assert e1["channel_type"] == "ticker"
        assert e2["channel_type"] == "match"

    def test_system_messages_return_none(self):
        subs_msg = json.dumps({
            "type": "subscriptions",
            "channels": [{"name": "matches", "product_ids": ["BTC-USD"]}],
        })
        heartbeat_msg = json.dumps({
            "type": "heartbeat",
            "sequence": 12345,
            "product_id": "BTC-USD",
        })
        error_msg = json.dumps({
            "type": "error",
            "message": "Failed to subscribe",
        })

        assert parse_coinbase_message(subs_msg) is None
        assert parse_coinbase_message(heartbeat_msg) is None
        assert parse_coinbase_message(error_msg) is None

    def test_malformed_json_returns_none(self):
        assert parse_coinbase_message("invalid json content") is None
        assert parse_coinbase_message("") is None
        assert parse_coinbase_message("123") is None

    def test_missing_or_invalid_fields_returns_none(self):
        # Missing price
        assert parse_coinbase_message(json.dumps({
            "type": "match",
            "trade_id": 1,
            "product_id": "BTC-USD",
            "size": "0.1",
            "time": "2026-10-21T00:00:00Z",
        })) is None

        # Non-numeric price
        assert parse_coinbase_message(json.dumps({
            "type": "match",
            "trade_id": 1,
            "product_id": "BTC-USD",
            "price": "abc",
            "size": "0.1",
            "time": "2026-10-21T00:00:00Z",
        })) is None

        # Non-positive price
        assert parse_coinbase_message(json.dumps({
            "type": "match",
            "trade_id": 1,
            "product_id": "BTC-USD",
            "price": "-50.0",
            "size": "0.1",
            "time": "2026-10-21T00:00:00Z",
        })) is None


# ---------------------------------------------------------------------------
# Client Reconnection & Resubscription Tests
# ---------------------------------------------------------------------------

class TestCoinbaseWebSocketClient:
    def test_build_subscribe_message(self):
        client = CoinbaseWebSocketClient(
            product_ids=["BTC-USD", "ETH-USD"],
            channels=["matches"],
        )
        msg_str = client.build_subscribe_message()
        msg = json.loads(msg_str)
        assert msg["type"] == "subscribe"
        assert msg["product_ids"] == ["BTC-USD", "ETH-USD"]
        assert msg["channels"] == ["matches"]

    def test_reconnect_and_resubscribe(self):
        async def _run_test():
            received_trades = []

            sample_match = json.dumps({
                "type": "match",
                "trade_id": 1001,
                "product_id": "BTC-USD",
                "price": "81000.00",
                "size": "0.05",
                "time": "2026-10-21T00:00:00Z",
            })

            # Mock websocket connection that disconnects once, then yields a trade
            connect_count = 0
            sent_messages = []

            class MockWs:
                def __init__(self, should_fail):
                    self.should_fail = should_fail

                async def __aenter__(self):
                    nonlocal connect_count
                    connect_count += 1
                    if self.should_fail:
                        from websockets.exceptions import ConnectionClosed
                        raise ConnectionClosed(None, None)
                    return self

                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    pass

                async def send(self, msg):
                    sent_messages.append(msg)

                def __aiter__(self):
                    return self

                async def __anext__(self):
                    if not hasattr(self, "_yielded"):
                        self._yielded = True
                        return sample_match
                    raise StopAsyncIteration

            def fake_connect(*args, **kwargs):
                nonlocal connect_count
                # First attempt fails, second attempt succeeds
                if connect_count == 0:
                    return MockWs(should_fail=True)
                return MockWs(should_fail=False)

            client = CoinbaseWebSocketClient(
                product_ids=["BTC-USD"],
                channels=["matches"],
                on_trade=lambda t: received_trades.append(t),
                initial_backoff=0.01,
                max_backoff=0.05,
            )

            with patch("websockets.connect", side_effect=fake_connect):
                # Run with max_messages=1 so it terminates after 1 event
                await client.connect_and_listen(max_messages=1)

            # Verified reconnection occurred (at least 2 connect calls)
            assert connect_count >= 2
            # Verified resubscription was sent on successful connection
            assert len(sent_messages) >= 1
            sub = json.loads(sent_messages[0])
            assert sub["type"] == "subscribe"
            assert sub["product_ids"] == ["BTC-USD"]
            # Verified trade event was parsed and delivered to on_trade
            assert len(received_trades) == 1
            assert received_trades[0]["event_id"] == "1001"
            assert received_trades[0]["symbol"] == "BTC-USD"

        asyncio.run(_run_test())
