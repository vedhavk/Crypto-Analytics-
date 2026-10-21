"""
producer.py — Creates a Kafka producer and publishes validated trade events.

Uses a proper Serializer subclass to avoid the kafka-python 3.x deprecation
warning that occurs when a plain callable is passed as value_serializer.
Serializes events to JSON while preserving numbers as JSON numbers.
"""

import json
import logging
import os
from typing import Any, List, Optional, Tuple

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.serializer import Serializer

from validator import validate_trade

logger = logging.getLogger(__name__)

TOPIC = os.getenv("KAFKA_TOPIC", "stock-ticks")
BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")


class JsonSerializer(Serializer):
    """Serialize a Python dict to UTF-8 encoded JSON bytes preserving numeric types."""

    def serialize(self, topic: str, headers: List[Tuple[str, bytes]], data: Any) -> bytes:
        return json.dumps(data).encode("utf-8")


def create_producer(broker: Optional[str] = None) -> KafkaProducer:
    """
    Create and return a KafkaProducer configured for reliable delivery.

    The producer uses acks='all' so the broker confirms that all in-sync
    replicas have received the message before acknowledging it, and retries
    up to five times on transient network errors.
    """
    bootstrap_broker = broker or BROKER
    return KafkaProducer(
        bootstrap_servers=[bootstrap_broker],
        value_serializer=JsonSerializer(),
        acks="all",
        retries=5,
    )


def publish_trade(
    producer: KafkaProducer,
    event: dict,
    topic: Optional[str] = None,
    timeout: float = 10.0,
) -> dict:
    """
    Validate *event* and publish it to the stock-ticks Kafka topic.

    Parameters
    ----------
    producer : KafkaProducer
        An active producer returned by create_producer().
    event : dict
        Raw trade event. Must pass validate_trade() before sending.
    topic : Optional[str]
        Topic name to publish to (defaults to TOPIC).
    timeout : float
        Delivery confirmation timeout in seconds.

    Returns
    -------
    dict
        The validated and normalized event that was published.

    Raises
    ------
    ValueError
        If the event fails validation.
    KafkaError
        If the broker cannot be reached or delivery fails after retries.
    """
    target_topic = topic or TOPIC

    # Validate and normalise before touching the network.
    normalized_event = validate_trade(event)

    try:
        future = producer.send(target_topic, value=normalized_event)
        # Block until the broker confirms delivery (or the timeout expires).
        metadata = future.get(timeout=timeout)
    except KafkaError as exc:
        logger.error(
            "Failed to publish trade %s to %s: %s",
            normalized_event.get("event_id", "?"),
            target_topic,
            exc,
        )
        raise KafkaError(
            f"Failed to publish {normalized_event.get('symbol', '?')} "
            f"to {target_topic}: {exc}"
        ) from exc

    print(
        f"Published {normalized_event['symbol']} "
        f"to {metadata.topic}, partition {metadata.partition}, "
        f"offset {metadata.offset}"
    )
    return normalized_event
