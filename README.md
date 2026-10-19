# Market Data Ingestion — Member 2: Ingestion and Data Quality

This component receives raw market trade events, validates and normalises them, and publishes them to a Kafka topic (`stock-ticks`). It is one module in a larger group project.

---

## Project structure

```
market-ingestion/
├── docker-compose.yml   # Starts a local Kafka broker (KRaft, no ZooKeeper)
├── sample_trade.json    # Example event used for manual integration testing
├── validator.py         # Validates and normalises raw trade events
├── producer.py          # Kafka producer — wraps validation and publishing
├── run_producer.py      # Entry point: load JSON → validate → publish
├── test_validator.py    # Unit tests for the validator (no Kafka required)
├── test_producer.py     # Unit tests for the producer (Kafka is mocked)
└── requirements.txt     # Python dependencies
```

---

## Event schema

Every trade event must contain these eight fields:

| Field           | Type   | Description                        |
|-----------------|--------|------------------------------------|
| `symbol`        | string | Trading pair, e.g. `"BTC-USD"`    |
| `price`         | number | Last traded price                  |
| `volume`        | number | Trade volume                       |
| `timestamp`     | string | ISO-8601 with timezone             |
| `price_usd`     | number | Price in US dollars                |
| `price_inr`     | number | Price in Indian rupees             |
| `exchange_rate` | number | USD/INR exchange rate used         |
| `rate_timestamp`| string | When the exchange rate was fetched |

Validation rules:
- All eight fields are required and must not be `None`.
- `symbol` must be a non-empty string.
- Numeric fields must be positive, finite numbers. Booleans are rejected.
- Timestamp fields must be valid ISO-8601 strings that include a timezone.

Normalisation applied before publishing:
- `symbol` is stripped and upper-cased.
- Numeric values are stored as strings (e.g. `"81562.81"`).
- Timestamps are converted to UTC and formatted as `YYYY-MM-DDTHH:MM:SSZ`.

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

Kafka runs in KRaft mode (no ZooKeeper) and listens on `localhost:9092`.

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

## Publishing a sample event

```powershell
python run_producer.py
```

This reads `sample_trade.json`, validates it, and publishes it to `stock-ticks`. You should see:

```
Published BTC-USD to stock-ticks, partition 0, offset N
```

You can also pass a custom JSON file:

```powershell
python run_producer.py path\to\my_event.json
```

---

## Consuming messages (verification)

Read the last message published to the topic:

```powershell
docker exec market-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic stock-ticks `
  --from-beginning --max-messages 1
```

---

## Running the tests

```powershell
python -m pytest -v
```

All tests are unit tests. They do not require Kafka to be running. Producer tests use `unittest.mock` so no messages are sent during testing.

---

## Validation error examples

```python
from validator import validate_trade

# Missing field
validate_trade({"symbol": "BTC-USD"})
# ValueError: Missing required field: price

# Boolean used as a number
validate_trade({..., "price": True})
# ValueError: price must be numeric, not a boolean

# Timestamp without timezone
validate_trade({..., "timestamp": "2026-10-16T10:30:15"})
# ValueError: timestamp must include a timezone
```
