"""
coinbase_client.py — WebSocket client for Coinbase Exchange Market Data.

Connects to wss://ws-feed.exchange.coinbase.com to receive real-time market data.
Supports channels:
  - 'matches': Individual trade execution events (default)
  - 'ticker': Real-time price and 24h ticker updates

Handles:
  - Automatic reconnection with exponential backoff on disconnect or failure
  - Automatic resubscription upon reconnection
  - Parsing and distinguishing 'matches' vs 'ticker' messages
  - Graceful discarding of system messages (subscriptions, heartbeat) and malformed data
"""

import asyncio
import json
import logging
from typing import Any, Callable, Dict, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

DEFAULT_COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
DEFAULT_PRODUCT_IDS = ["BTC-USD"]
DEFAULT_CHANNELS = ["matches"]


def parse_coinbase_message(raw_msg: str) -> Optional[Dict[str, Any]]:
    """
    Parse a Coinbase WebSocket JSON message into an un-enriched trade dict.

    Distinguishes:
      - 'match' / 'last_match': Individual trade executions from 'matches' channel
      - 'ticker': Price updates from 'ticker' channel
      - 'subscriptions' / 'heartbeat': Administrative/liveness messages (returns None)
      - 'error': Coinbase error responses (logged and returns None)
      - Malformed or unexpected messages: Logged and returns None

    Returns
    -------
    dict or None
        Dict containing raw trade fields (event_id, symbol, price, volume, timestamp, price_usd)
        or None if not a trade event.
    """
    try:
        data = json.loads(raw_msg)
    except (json.JSONDecodeError, TypeError) as exc:
        logger.warning("Received invalid JSON from Coinbase: %s", exc)
        return None

    if not isinstance(data, dict):
        logger.warning("Received non-dict message from Coinbase: %s", data)
        return None

    msg_type = data.get("type")

    # Informational / Administrative messages
    if msg_type == "subscriptions":
        logger.info("Coinbase subscriptions confirmed: %s", data.get("channels"))
        return None

    if msg_type == "heartbeat":
        logger.debug("Coinbase heartbeat received for %s", data.get("product_id"))
        return None

    if msg_type == "error":
        logger.error("Coinbase returned error message: %s", data.get("message"))
        return None

    # Handle individual trade events ('matches' channel)
    if msg_type in ("match", "last_match"):
        try:
            trade_id = data.get("trade_id")
            product_id = data.get("product_id")
            price_str = data.get("price")
            size_str = data.get("size")
            time_str = data.get("time")

            if not all((trade_id, product_id, price_str, size_str, time_str)):
                logger.warning("Missing required fields in match message: %s", data)
                return None

            price = float(price_str)
            volume = float(size_str)
            if price <= 0 or volume <= 0:
                logger.warning("Non-positive price or volume in match message: %s", data)
                return None

            return {
                "event_id": str(trade_id),
                "symbol": product_id,
                "price": price,
                "volume": volume,
                "timestamp": time_str,
                "price_usd": price,
                "channel_type": "match",
            }
        except (ValueError, TypeError) as exc:
            logger.warning("Failed to parse match message (%s): %s", exc, data)
            return None

    # Handle ticker updates ('ticker' channel)
    if msg_type == "ticker":
        try:
            product_id = data.get("product_id")
            price_str = data.get("price")
            time_str = data.get("time")
            last_size_str = data.get("last_size") or data.get("volume_24h")
            trade_id = data.get("trade_id") or data.get("sequence")

            if not all((product_id, price_str, time_str, trade_id)):
                logger.warning("Missing required fields in ticker message: %s", data)
                return None

            price = float(price_str)
            volume = float(last_size_str) if last_size_str else 0.001
            if price <= 0 or volume <= 0:
                logger.warning("Non-positive price or volume in ticker message: %s", data)
                return None

            return {
                "event_id": str(trade_id),
                "symbol": product_id,
                "price": price,
                "volume": volume,
                "timestamp": time_str,
                "price_usd": price,
                "channel_type": "ticker",
            }
        except (ValueError, TypeError) as exc:
            logger.warning("Failed to parse ticker message (%s): %s", exc, data)
            return None

    logger.debug("Ignoring unhandled Coinbase message type: %s", msg_type)
    return None


class CoinbaseWebSocketClient:
    """Manages connection, subscription, and reconnection to Coinbase WebSocket feed."""

    def __init__(
        self,
        product_ids: Optional[List[str]] = None,
        channels: Optional[List[str]] = None,
        ws_url: str = DEFAULT_COINBASE_WS_URL,
        on_trade: Optional[Callable[[Dict[str, Any]], None]] = None,
        initial_backoff: float = 1.0,
        max_backoff: float = 30.0,
    ):
        self.product_ids = product_ids or DEFAULT_PRODUCT_IDS
        self.channels = channels or DEFAULT_CHANNELS
        self.ws_url = ws_url
        self.on_trade = on_trade
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff

        self._running = False
        self._current_ws: Optional[Any] = None

    def build_subscribe_message(self) -> str:
        """Construct JSON subscribe message for Coinbase."""
        return json.dumps({
            "type": "subscribe",
            "product_ids": self.product_ids,
            "channels": self.channels,
        })

    async def connect_and_listen(self, max_messages: Optional[int] = None) -> None:
        """
        Connect to Coinbase WebSocket and process messages indefinitely.

        Parameters
        ----------
        max_messages : Optional[int]
            If provided, stops after receiving this many valid trade events (useful for tests).
        """
        self._running = True
        backoff = self.initial_backoff
        received_count = 0

        while self._running:
            try:
                logger.info("Connecting to Coinbase WebSocket: %s", self.ws_url)
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    self._current_ws = ws
                    backoff = self.initial_backoff  # Reset backoff on successful connection

                    # Resubscribe upon connection
                    sub_msg = self.build_subscribe_message()
                    await ws.send(sub_msg)
                    logger.info(
                        "Subscribed to %s on channels %s",
                        self.product_ids,
                        self.channels,
                    )

                    async for raw_msg in ws:
                        if not self._running:
                            break

                        trade_event = parse_coinbase_message(raw_msg)
                        if trade_event and self.on_trade:
                            # Remove internal tracking field before dispatching
                            trade_event.pop("channel_type", None)
                            self.on_trade(trade_event)
                            received_count += 1
                            if max_messages and received_count >= max_messages:
                                self._running = False
                                break

            except (ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                if not self._running:
                    break
                logger.warning(
                    "Coinbase WebSocket disconnected (%s). Reconnecting in %.1fs...",
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.max_backoff)
            except Exception as exc:
                if not self._running:
                    break
                logger.error("Unexpected error in WebSocket loop: %s", exc, exc_info=True)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.max_backoff)

        logger.info("Coinbase WebSocket listener stopped.")

    def stop(self) -> None:
        """Signal the listener to stop gracefully."""
        self._running = False
        if self._current_ws:
            asyncio.create_task(self._current_ws.close())
