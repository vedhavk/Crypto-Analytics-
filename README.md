# Market Data Ingestion — Member 2: Ingestion and Data Quality

This component connects to the **Coinbase Exchange WebSocket**, receives live cryptocurrency market-data events, enriches them with a **USD-to-INR exchange rate**, validates and normalises every event, and publishes them continuously to a **Kafka topic** (`stock-ticks`).

---

## Project structure

```
market-ingestion/
├── docker-compose.yml   # Kafka broker (KRaft) with host + Docker listener
├── sample_trade.json    # Fixed event for manual / CI testing
├── coinbase_ws.py       # Coinbase WebSocket client (reconnect, resubscribe)
├── inr_converter.py     # USD→INR rate fetcher with caching
├── validator.py         # Validates and normalises raw trade events
├── producer.py          # Kafka producer — wraps validation and publishing
├── run_producer.py      # Entry point: live WebSocket mode or single-file mode
├── test_validator.py    # Unit tests for the validator (no Kafka required)
├── test_producer.py     # Unit tests for the producer (Kafka is mocked)
├── test_ws_parser.py    # Unit tests for WS parsing, INR conversion, caching
└── requirements.txt     # Python dependencies
```

---

## Event schema

Every event published to `stock-ticks` carries the following fields:

| Field            | Type            | Nullable | Description                                     |
|------------------|-----------------|----------|-------------------------------------------------|
| `symbol`         | string          | No       | Trading pair, e.g. `"BTC-USD"`                 |
| `price`          | number (float)  | No       | Last traded price in USD                        |
| `volume`         | number (float)  | No       | Trade volume                                    |
| `timestamp`      | string          | No       | UTC ISO-8601 trade time: `YYYY-MM-DDTHH:MM:SSZ` |
| `price_usd`      | number (float)  | No       | Price in US dollars                             |
| `price_inr`      | number \| null  | Yes      | Price in Indian rupees (`null` when unavailable)|
| `exchange_rate`  | number \| null  | Yes      | USD→INR rate used (`null` when unavailable)     |
| `rate_timestamp` | string \| null  | Yes      | Rate fetch time (`null` when unavailable)       |

**Numeric values are JSON numbers** (not strings). Timestamps are UTC ISO-8601 strings ending with `Z`.

---

## Channels subscribed on Coinbase

| Channel   | Message type      | Fields used                             |
|-----------|-------------------|-----------------------------------------|
| `ticker`  | `ticker`          | `product_id`, `price`, `volume_24h`, `time` |
| `matches` | `match` / `last_match` | `product_id`, `price`, `size`, `time` |

Trading pairs: `BTC-USD`, `ETH-USD`, `SOL-USD` (configurable in `coinbase_ws.py`).

---

## USD-to-INR conversion

| Property           | Value                                             |
|--------------------|---------------------------------------------------|
| **API source**     | `https://open.er-api.com/v6/latest/USD`           |
| **Provider**       | ExchangeRate-API free public endpoint (no key)    |
| **Refresh interval** | Every **5 minutes** (`REFRESH_INTERVAL_S = 300`) |
| **Caching**        | Module-level thread-safe cache; every crypto event reuses the latest valid rate |
| **Failure handling** | On HTTP error / bad JSON: stale cache is reused; if never fetched, `price_inr`, `exchange_rate`, and `rate_timestamp` are published as `null` |

Formula: `price_inr = price_usd × exchange_rate`

---

## Reconnection and resubscription

| Behaviour          | Detail                                                         |
|--------------------|----------------------------------------------------------------|
| Reconnect strategy | Exponential back-off: 1 s → 2 s → 4 s … capped at **60 s**   |
| Resubscription     | Subscription message resent on every `on_open` callback        |
| Error types caught | Socket errors, broker-sent `error` frames, malformed JSON      |

---

## Kafka listener configuration

| Listener name | Address            | For whom                                       |
|---------------|--------------------|------------------------------------------------|
| `PLAINTEXT`   | `localhost:9092`   | Python producer running on the host            |
| `DOCKER`      | `kafka:29092`      | Spark / ClickHouse services inside Docker network |

Spark should use `kafka:29092` as the bootstrap server. The Python producer uses `localhost:9092`.

---

## Prerequisites

- Python 3.12
- Docker Desktop (for local Kafka)
- Windows 11 / PowerShell

---

## Setup

### 1. Create and activate the virtual environment

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

```powershell
pip install -r requirements.txt
```

---

## Running Kafka locally

### Start the broker

```powershell
docker compose up -d
```

Kafka runs in KRaft mode (no ZooKeeper) and listens on `localhost:9092` (host) and `kafka:29092` (Docker network).

### Create the topic (first time only)

```powershell
docker exec market-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --create --topic stock-ticks `
  --partitions 1 --replication-factor 1
```

### Stop the broker

```powershell
docker compose down
```

---

## Running the producer

### Live mode (default) — connect to Coinbase and publish continuously

```powershell
python run_producer.py
```

The producer:
1. Fetches the USD→INR rate from the free API.
2. Connects to `wss://ws-feed.exchange.coinbase.com`.
3. Subscribes to `ticker` and `matches` channels.
4. Parses every incoming message and publishes validated events to `stock-ticks`.
5. Reconnects automatically with back-off on any failure.
6. Refreshes the exchange rate every 5 minutes.

### Dry-run mode — test live WebSocket & INR conversion without Kafka

```powershell
# Validate and print live events to console without requiring Kafka:
python run_producer.py --dry-run

# Test live pipeline and automatically stop after 5 events:
python run_producer.py --dry-run --max-events 5
```

### Single-event test mode — publish one JSON file and exit

```powershell
python run_producer.py --file sample_trade.json
python run_producer.py --file path\to\my_event.json
```

### Logging

```powershell
python run_producer.py --log-level DEBUG
```

---

## Consuming messages (verification)

Stream all messages from the beginning:

```powershell
docker exec market-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic stock-ticks `
  --from-beginning
```

---

## Running the tests

```powershell
python -m pytest -v
```

All tests are unit tests. They do not require Kafka or network access. All HTTP and WebSocket calls are mocked.

| Test file            | What it covers                                                  |
|----------------------|-----------------------------------------------------------------|
| `test_validator.py`  | Schema validation, nullable INR fields, symbol normalisation, timestamps |
| `test_producer.py`   | Kafka publish path, null-INR events, validation errors, Kafka errors |
| `test_ws_parser.py`  | WS message parsing (ticker/match), INR caching, failure handling, subscribe message |

---

## Schema decision: `event_id`

An `event_id` field is **not included** in the current schema. The Spark consumer and ClickHouse team should use the combination of `(symbol, timestamp)` as a logical primary key, or rely on Kafka partition + offset for deduplication. If `event_id` is required, it should be added as a UUID generated at publish time and coordinated with all downstream consumers before schema changes are made.

---

## Validation error examples

```python
from validator import validate_trade

# Missing non-nullable field
validate_trade({"symbol": "BTC-USD"})
# ValueError: Missing required field: price

# Boolean used as a number
validate_trade({..., "price": True})
# ValueError: price must be numeric, not a boolean

# Timestamp without timezone
validate_trade({..., "timestamp": "2026-10-16T10:30:15"})
# ValueError: timestamp must include a timezone

# Null price_inr is allowed (INR conversion unavailable)
validate_trade({..., "price_inr": None, "exchange_rate": None, "rate_timestamp": None})
# → valid; INR fields are null in the published event
```
