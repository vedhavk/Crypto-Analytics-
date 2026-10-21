# Market Data Ingestion — Member 2: Ingestion and Data Quality

This component continuously ingests real-time cryptocurrency market data from Coinbase Exchange, enriches each trade with live USD-to-INR currency conversion, validates and normalises events according to a strict data-quality schema, and publishes them to an Apache Kafka topic (`stock-ticks`). It forms the ingestion layer of a streaming analytics platform.

---

## Project Structure

```
market-ingestion/
├── docker-compose.yml          # Kafka broker with dual listeners (host + Docker Spark)
├── sample_trade.json           # Example event with event_id for individual testing
├── validator.py                # Schema validation & normalisation (JSON numbers, nullable INR)
├── currency_converter.py       # Free exchange-rate API, caching & failure handling
├── coinbase_client.py          # Coinbase WebSocket client (matches/ticker, auto-reconnect)
├── producer.py                 # Reliable Kafka producer with native JSON serialization
├── run_producer.py             # Entry point: continuous live streaming or single-file mode
├── test_validator.py           # Unit tests for schema validation and normalisation
├── test_producer.py            # Unit tests for Kafka producer (mocked broker)
├── test_currency_converter.py  # Unit tests for rate fetching, caching, and fallback
├── test_coinbase_client.py     # Unit tests for WebSocket message parsing & reconnection
└── requirements.txt            # Python dependencies
```

---

## Event Schema

Every event published to `stock-ticks` adheres to the following agreed schema:

| Field            | JSON Type        | Description                                           | Nullable |
|------------------|------------------|-------------------------------------------------------|----------|
| `event_id`       | string           | Unique trade or sequence identifier                   | No       |
| `symbol`         | string           | Normalized trading pair, e.g. `"BTC-USD"`             | No       |
| `price`          | number (float)   | Last traded price                                     | No       |
| `volume`         | number (float)   | Trade volume / size                                   | No       |
| `timestamp`      | string           | UTC ISO-8601 timestamp ending with `'Z'`              | No       |
| `price_usd`      | number (float)   | Price in US dollars                                   | No       |
| `price_inr`      | number or null   | Price in Indian rupees (`price_usd * exchange_rate`)  | Yes      |
| `exchange_rate`  | number or null   | USD/INR exchange rate used                            | Yes      |
| `rate_timestamp` | string or null   | UTC ISO-8601 timestamp when exchange rate was fetched | Yes      |

### Validation and Normalisation Rules

- **Strict Event ID**: `event_id` is required, non-empty, and normalized as a string.
- **Symbol**: Normalized to upper-case and stripped of whitespace (e.g. `"BTC-USD"`).
- **Numeric Preservation**: Numeric values are preserved as **JSON numbers** (`float`), NOT converted into strings.
- **Positive and Finite**: Non-null numeric fields must be strictly positive and finite (`> 0`). Booleans (`True`/`False`) and NaN/Infinity are rejected.
- **Nullable INR Fields**: If external exchange-rate APIs are unreachable and no cached rate exists, `price_inr`, `exchange_rate`, and `rate_timestamp` are set to `null` (`None` in Python), allowing market data publishing to proceed uninterrupted.
- **Timezone Awareness**: All timestamps must include timezone information and are converted to UTC formatted as `YYYY-MM-DDTHH:MM:SSZ`.

---

## Coinbase WebSocket Integration

- **Endpoint**: `wss://ws-feed.exchange.coinbase.com`
- **Supported Channels**:
  - `matches` (*Default*): Streams every individual trade execution (`type: "match"`, `type: "last_match"`), providing granular trade IDs and executed sizes.
  - `ticker`: Streams real-time price changes, 24h volume, and best bid/ask updates.
- **Message Parsing & Distinction**:
  - `match` events extract `trade_id`, `product_id`, `price`, `size`, and `time`.
  - `ticker` updates extract `trade_id`/`sequence`, `product_id`, `price`, `last_size`, and `time`.
  - Administrative messages (`subscriptions`, `heartbeat`) and malformed messages are handled gracefully without terminating the stream.
- **Resilience & Reconnection**:
  - If the socket closes or network errors occur, the client automatically reconnects using exponential backoff (1s up to 30s).
  - Subscriptions are automatically re-sent upon every reconnection.

---

## USD-to-INR Conversion

- **Exchange-Rate Sources** (No API keys or paid tiers required):
  - **Primary**: Open ExchangeRate API (`https://open.er-api.com/v6/latest/USD`)
  - **Fallback**: Frankfurter API (`https://api.frankfurter.app/latest?from=USD&to=INR`)
- **Refresh Interval**: Configurable (default: 300 seconds / 5 minutes) to avoid redundant network overhead on live trade ticks.
- **Caching Strategy**:
  - In-memory thread-safe cache (`CurrencyConverter`) stores the most recent valid rate and its UTC timestamp.
  - Subsequent trade events read the cached rate in O(1) time without blocking.
- **Failure-Handling Mechanism**:
  - If a refresh request fails, the converter reuses the last valid cached rate and logs a warning.
  - If the service boots without any internet connectivity and cannot retrieve an initial rate, it sets `price_inr`, `exchange_rate`, and `rate_timestamp` to `null`, ensuring core USD trades continue streaming.

---

## Kafka Configuration & Dual Listeners

The broker configuration in `docker-compose.yml` supports both the Python producer running directly on the host and the downstream Spark consumer running inside Docker:

```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,INTERNAL://kafka:29092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
```

- **Host Python Producer**: Connects to `localhost:9092`.
- **Dockerized Spark / ClickHouse**: Connects to `kafka:29092` via Docker network `market-network`.

---

## Setup & Prerequisites

- Python 3.10+ (tested on Python 3.12 and 3.14)
- Docker Desktop (for Kafka broker)

### 1. Install dependencies

```powershell
pip install -r requirements.txt
```

### 2. Start Kafka broker

```powershell
docker compose up -d
```

### 3. Create Kafka topic (first time only)

```powershell
docker exec market-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --create --topic stock-ticks `
  --partitions 1 --replication-factor 1
```

---

## Running the Ingestion Pipeline

### 1. Live Continuous Streaming (Default)

Streams live BTC-USD trades from Coinbase, enriches them with INR rates, validates them, and publishes to Kafka:

```powershell
python run_producer.py
```

Customize cryptocurrency symbols or switch channel:

```powershell
# Ingest multiple crypto pairs
python run_producer.py --symbols BTC-USD,ETH-USD,SOL-USD

# Ingest from ticker channel instead of matches
python run_producer.py --channel ticker --symbols BTC-USD
```

### 2. Verification Mode without Kafka (`--dry-run`)

Test live Coinbase WebSocket ingestion and USD-to-INR enrichment without needing Docker/Kafka running:

```powershell
python run_producer.py --dry-run --max-events 5
```

### 3. Single-Event File Mode

Publish or validate a single event file (e.g. `sample_trade.json`):

```powershell
python run_producer.py sample_trade.json
```

Or specify a custom file:

```powershell
python run_producer.py --file path\to\trade.json
```

---

## Consuming Messages (Verification)

Consume published messages from the `stock-ticks` Kafka topic:

```powershell
docker exec market-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic stock-ticks `
  --from-beginning
```

---

## Running the Tests

Execute the comprehensive test suite (132 unit tests):

```powershell
python -m pytest -v
```

The test suite covers:
- Schema validation, required `event_id`, numeric preservation as numbers, and nullable INR fields (`test_validator.py`).
- Kafka publishing, JSON serialization, and error handling (`test_producer.py`).
- Exchange rate API fetching, caching TTL, fallback API, and failure degradation (`test_currency_converter.py`).
- Coinbase message parsing, ticker vs matches distinction, and reconnection/resubscription logic (`test_coinbase_client.py`).
