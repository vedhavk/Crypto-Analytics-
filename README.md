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

| Test file                   | What it covers                                                  |
|-----------------------------|-----------------------------------------------------------------|
| `test_validator.py`         | Schema validation, nullable INR fields, symbol normalisation, timestamps |
| `test_producer.py`          | Kafka publish path, null-INR events, validation errors, Kafka errors |
| `test_ws_parser.py`         | WS message parsing, INR caching & fallback API, failure handling, dry-run mode |
| `test_clickhouse_schema.py` | ClickHouse table schemas, engine types, HTTP API execution      |
| `spark/test_spark_pipeline.py` | Spark schema definition, ClickHouse writer, retry logic       |

---

## Spark Structured Streaming & ClickHouse (Member 3)

The streaming pipeline consumes continuously from Kafka topic `stock-ticks`, parses and normalizes JSON payloads, and executes dual micro-batch streaming queries:
1. **Raw Ticks**: Writes validated ticks to ClickHouse table `stocks.ticks`.
2. **1-Minute Aggregates**: Watermarked windowed stream computing `count(*)`, `avg(price)`, `min(price)`, `max(price)`, `sum(volume)`, and `avg(price_inr)` into `stocks.ticks_1m_agg`.

### ClickHouse Tables & Deduplicated Views

* `stocks.ticks`: `MergeTree()` partitioned by month, ordered by `(symbol, timestamp, price, volume)`.
* `stocks.ticks_1m_agg`: `ReplacingMergeTree(calculated_at)` partitioned by month, ordered by `(symbol, window_start)`.
* `stocks.market_alerts`: `MergeTree()` table for Member 4 anomaly alerts.
* `stocks.v_ticks_1m_agg`: **Recommended View for Member 4 & FastAPI**. Reads `stocks.ticks_1m_agg FINAL` to guarantee only the latest merged version of each window is returned without duplicates.
* `stocks.v_ticks_dedup`: View filtering Kafka replay duplicates across raw ticks.

### Running the Complete Stack with Docker

```powershell
# 1. Start Kafka, ClickHouse, and Spark Structured Streaming
docker compose up -d

# 2. In another terminal, run the Python WebSocket producer
python run_producer.py
```

### Running Spark Streaming on Host Machine

```powershell
python spark/spark_consumer.py --kafka-bootstrap localhost:9092 --clickhouse-host localhost
```

---

## Schema decision: `event_id`

An `event_id` field is not emitted by the producer. ClickHouse generates a unique UUID on insert via `event_id UUID DEFAULT generateUUIDv4()`, while preserving `(symbol, timestamp, price, volume)` as the ordering key to guarantee sub-second trades are never collapsed.

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
