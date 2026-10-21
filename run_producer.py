"""
run_producer.py — Market Data Ingestion Pipeline.

Supports two operational modes:
1. Live Streaming Mode (Default):
   Connects continuously to Coinbase WebSocket (wss://ws-feed.exchange.coinbase.com),
   subscribes to crypto pairs, enriches each trade with USD-to-INR conversion
   via cached exchange rates, validates the schema, and continuously publishes
   to Kafka topic 'stock-ticks'.

2. File Mode:
   Reads an individual JSON event from disk (defaults to sample_trade.json),
   validates it, and publishes it once.

Usage:
  # Continuous live ingestion from Coinbase WebSocket (default)
  python run_producer.py

  # Live ingestion for specific pairs and channel
  python run_producer.py --symbols BTC-USD,ETH-USD --channel matches

  # Test live WebSocket without requiring Kafka broker (--dry-run)
  python run_producer.py --dry-run --max-events 5

  # Publish single event from file (sample_trade.json)
  python run_producer.py --file sample_trade.json
  python run_producer.py sample_trade.json
"""

import argparse
import asyncio
import json
import logging
import pathlib
import signal
import sys
from typing import Optional

from kafka.errors import KafkaError

from coinbase_client import CoinbaseWebSocketClient
from currency_converter import CurrencyConverter
from producer import create_producer, publish_trade

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("market_producer")

DEFAULT_DATA_FILE = pathlib.Path(__file__).parent / "sample_trade.json"


def load_event(path: pathlib.Path) -> dict:
    """Read and parse a JSON trade event from *path*."""
    if not path.exists():
        sys.exit(f"Error: data file not found: {path}")

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        sys.exit(f"Error: {path} contains invalid JSON — {exc}")


def run_file_mode(file_path: pathlib.Path, broker: Optional[str] = None) -> None:
    """Publish a single trade event from a JSON file."""
    logger.info("Running in file mode with %s", file_path)
    event = load_event(file_path)

    try:
        producer = create_producer(broker=broker)
        try:
            publish_trade(producer, event)
        finally:
            producer.close()
    except ValueError as exc:
        sys.exit(f"Validation error: {exc}")
    except KafkaError as exc:
        sys.exit(f"Kafka error: {exc}")


async def run_live_mode(
    symbols: list[str],
    channels: list[str],
    broker: Optional[str] = None,
    dry_run: bool = False,
    max_events: Optional[int] = None,
) -> None:
    """Run continuous Coinbase WebSocket stream, enriching and publishing to Kafka."""
    converter = CurrencyConverter()
    producer = None if dry_run else create_producer(broker=broker)

    events_published = 0

    def on_trade_received(raw_trade: dict) -> None:
        nonlocal events_published
        try:
            # 1. Enrich with USD-to-INR conversion
            enriched = converter.enrich_event(raw_trade)

            # 2. Publish to Kafka (or log if dry-run)
            if dry_run:
                # Still run validation to ensure schema correctness
                from validator import validate_trade
                validated = validate_trade(enriched)
                logger.info(
                    "[DRY-RUN] Validated trade: symbol=%s, price_usd=%.2f, price_inr=%s, exchange_rate=%s, event_id=%s",
                    validated["symbol"],
                    validated["price_usd"],
                    validated["price_inr"],
                    validated["exchange_rate"],
                    validated["event_id"],
                )
            else:
                publish_trade(producer, enriched)

            events_published += 1

        except Exception as exc:
            logger.error("Error processing trade event: %s (event: %s)", exc, raw_trade)

    client = CoinbaseWebSocketClient(
        product_ids=symbols,
        channels=channels,
        on_trade=on_trade_received,
    )

    # Set up graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, client.stop)
        except NotImplementedError:
            # Signal handlers not implemented on Windows event loop for add_signal_handler
            pass

    logger.info(
        "Starting continuous market ingestion for %s on channel(s) %s (dry_run=%s)",
        symbols,
        channels,
        dry_run,
    )

    try:
        await client.connect_and_listen(max_messages=max_events)
    finally:
        if producer is not None:
            logger.info("Closing Kafka producer...")
            producer.close()
        logger.info("Pipeline stopped. Total events processed: %d", events_published)


def parse_args():
    parser = argparse.ArgumentParser(description="Market Data Ingestion Producer")
    parser.add_argument(
        "positional_file",
        nargs="?",
        type=str,
        help="Optional path to a JSON file to publish a single event",
    )
    parser.add_argument(
        "--file",
        "-f",
        type=str,
        help="Path to JSON file to run in single-event file mode",
    )
    parser.add_argument(
        "--symbols",
        "-s",
        type=str,
        default="BTC-USD",
        help="Comma-separated list of Coinbase product IDs (default: BTC-USD)",
    )
    parser.add_argument(
        "--channel",
        "-c",
        type=str,
        default="matches",
        choices=["matches", "ticker"],
        help="Coinbase WebSocket channel to subscribe to (default: matches)",
    )
    parser.add_argument(
        "--broker",
        "-b",
        type=str,
        default=None,
        help="Kafka bootstrap broker address (default: localhost:9092 or KAFKA_BROKER env)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without connecting to Kafka; validates and logs events to console",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after publishing this many events (useful for verification)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # If a file was passed as positional or named flag, run file mode
    target_file = args.file or args.positional_file
    if target_file:
        run_file_mode(pathlib.Path(target_file), broker=args.broker)
        return

    # Otherwise run continuous live mode
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    channels = [args.channel]

    try:
        asyncio.run(
            run_live_mode(
                symbols=symbols,
                channels=channels,
                broker=args.broker,
                dry_run=args.dry_run,
                max_events=args.max_events,
            )
        )
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")


if __name__ == "__main__":
    main()
