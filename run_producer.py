"""
run_producer.py — Entry point that loads a trade event from JSON and publishes it.

Usage:
    python run_producer.py
    python run_producer.py path/to/custom_event.json

The file defaults to sample_trade.json in the same directory as this script.
"""

import json
import pathlib
import sys

from kafka.errors import KafkaError

from producer import create_producer, publish_trade

# Default to sample_trade.json next to this script.
DEFAULT_DATA_FILE = pathlib.Path(__file__).parent / "sample_trade.json"


def load_event(path: pathlib.Path) -> dict:
    """
    Read and parse a JSON trade event from *path*.

    Raises
    ------
    SystemExit
        With a descriptive message if the file is missing or contains
        invalid JSON.
    """
    if not path.exists():
        sys.exit(f"Error: data file not found: {path}")

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        sys.exit(f"Error: {path} contains invalid JSON — {exc}")


def main() -> None:
    # Allow an optional command-line argument to override the data file.
    data_file = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DATA_FILE

    event = load_event(data_file)

    producer = create_producer()
    try:
        publish_trade(producer, event)
    except ValueError as exc:
        sys.exit(f"Validation error: {exc}")
    except KafkaError as exc:
        sys.exit(f"Kafka error: {exc}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
