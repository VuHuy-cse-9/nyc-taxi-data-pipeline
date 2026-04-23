# NYC Taxi Online Feature Processing — Context

## Overview

This module implements the **online (streaming) pipeline** for real-time feature computation. It consumes CDC change events from source databases via Debezium, parses telemetry, enriches with derived features, and publishes to a Kafka topic for Kafka Connect to sink to the PostgreSQL datamart.

### Features Computed

1. **Demand per Zone**: Sliding window count of requests by pickup zone
2. **Fleet Composition per Zone**: Proportion of vehicle types (green/yellow/FHV) active in a zone
3. **Net Flow of Vehicles**: Directional movement signal (outgoing − incoming trips per zone pair)
4. **Congestion Proxy via Trip Speeds**: Aggregated speed statistics (avg, min, max, stddev, percentiles)

---

## Directory Structure

```
stream_processing/
├── parsers/              # CDC parsing modules (Debezium → Flink table)
│   ├── __init__.py       # Re-exports parse_data functions
│   ├── green_taxi_parsing.py     # Parse raw.public.green_taxi (Avro + Debezium)
│   ├── yellow_taxi_parsing.py    # Parse raw.default.yellow_taxi (JSON + Debezium)
│   └── fhvhv_parsing.py          # Parse raw.datasource3.fhvhv_taxi (JSON + Debezium)
├── preprocess/           # Row-level transformation modules
│   ├── __init__.py       # Re-exports preprocess functions
│   ├── common.py         # Shared helpers (ensure_boolean_type, process_trip_type, process_payment_type)
│   ├── green_taxi_preprocess.py  # Green taxi preprocessing
│   ├── yellow_taxi_preprocess.py # Yellow taxi preprocessing
│   └── fhvhv_preprocess.py       # FHV preprocessing
├── schemas.py            # Flink schema definitions, DDLs, type helpers
├── online_feat.py        # Feature computation (windowing, joins)
├── main.py               # Entry point — orchestrates the pipeline
├── deps.zip              # PyFiles distribution (for Flink run)
└── README.md             # Quick start notes for job submission
```

---

## Pipeline Architecture

```
Source DB (PostgreSQL/MongoDB/Cassandra)
    ↓ (Debezium CDC)
Kafka topic (raw.public.green_taxi, raw.default.yellow_taxi, raw.datasource3.fhvhv_taxi)
    ↓ (Flink parsing)
Kafka topic (preprocessed_green, preprocessed_yellow, preprocessed_fhvhv)
    ↓ (Flink preprocessing, union green+yellow)
Kafka topic (preprocessed_traditional)
    ↓ (Flink online_feat.py)
Kafka topic (online-feature)
    ↓ (Kafka Connect)
PostgreSQL datamart (online features table)
```

---

## Data Flow & Transformation

### Stage 1: Parsing (`parsers/`)

Each taxi type has its own parser that consumes Debezium CDC messages:

| Source | Topic Format | Parsing Method |
|--------|-------------|----------------|
| Green Taxi | `raw.public.green_taxi` | Avro + Debezium envelope + `TO_TIMESTAMP()` |
| Yellow Taxi | `raw.default.yellow_taxi` | JSON nested `payload.after.<field>.value` |
| FHV | `raw.datasource3.fhvhv_taxi` | JSON + `JSON_VALUE()` extraction |

**Green Taxi Schema**:
```sql
CREATE TABLE raw_green_taxi (
    id STRING, vendorid INT, ..., lpep_pickup_datetime STRING,
    pickup_datetime AS TO_TIMESTAMP(lpep_pickup_datetime),
    WATERMARK FOR pickup_datetime AS pickup_datetime - INTERVAL '1' SECOND
) WITH ('format' = 'debezium-avro-confluent')
```

**Yellow Taxi Schema**:
```sql
CREATE TABLE raw_yellow_taxi (
    payload ROW(after ROW(
        vendorid ROW(value INT, deletion_ts BOOLEAN),
        tpep_pickup_datetime ROW(value STRING, ...)
    )),
    pickup_datetime AS TO_TIMESTAMP(payload.after.tpep_pickup_datetime.`value`, 'fmt'),
    WATERMARK FOR pickup_datetime AS ...
) WITH ('format' = 'json')
```

**FHV Schema**:
```sql
CREATE TABLE raw_for_hire_vehicle (
    payload ROW(after STRING),
    data AS payload.after,
    pickup_datetime AS TO_TIMESTAMP(JSON_VALUE(payload.after, '$.pickup_datetime'), 'fmt'),
    ...
) WITH ('format' = 'json')
```

---

### Stage 2: Preprocessing (`preprocess/`)

Row-level transformations applied to each record:

| Transformation | Green | Yellow | FHV |
|----------------|-------|--------|-----|
| `store_and_fwd_flag` → boolean | ✅ | ✅ | N/A |
| `trip_type` label mapping | ✅ (1/2 → Street-hail/Dispatch) | Hardcoded `Street-hail` | N/A |
| `payment_type` → enum string | ✅ (1-6 → enum) | ✅ | N/A |
| Rename `trip_distance` → `trip_miles` | ✅ | ✅ | N/A |
| Add `taxi_type` column | ✅ | ✅ | ✅ |
| Compute `trip_duration` (seconds) | ✅ (`timestamp_diff`) | ✅ (`timestamp_diff`) | ✅ (from `trip_time`) |
| Green/Yellow union compatibility | Drop `ehail_fee`, add `airport_fee=0` | Add `total_amount` sum, `trip_type` | Rename `trip_time` → `trip_duration`, `base_passenger_fare` → `fare_amount` |

**Key helpers in `preprocess/common.py`**:
- `ensure_boolean_type(col, true_value)`: `col.lower.trim() LIKE true_value` → boolean
- `process_trip_type(col)`: `IF col=1 THEN Street-hail ELSE Dispatch`
- `process_payment_type(col)`: `COALESCE(IF col=1 THEN CreditCard, IF col=2 THEN Cash, ...)`

---

### Stage 3: Feature Computation (`online_feat.py`)

**Sliding window configuration** (for all features):
```python
WINDOW_SIZE = 10    # 10 seconds (!!! production should be minutes)
SLIDE_EVERY = 5     # Slide every 5 seconds
```

**Four online features**:

1. **Demand per Zone** (`compute_window_count`):
   - Group by: `pulocationid`
   - Count: `id`
   - Window: Slide(10s, 5s) on `pickup_datetime`

2. **Fleet Composition per Zone** (`compute_window_count`):
   - Group by: `pulocationid`, `taxi_type`
   - Count: `id`
   - Window: Slide(10s, 5s) on `pickup_datetime`

3. **Net Flow per Zone** (`compute_netflow_per_zone`):
   - Group by: `pulocationid`, `taxi_type`
   - `netflow_in`: COUNT WHERE `dolocationid = pulocationid`
   - `netflow_out`: COUNT WHERE `dolocationid != pulocationid`
   - Window: Slide(10s, 5s) on `pickup_datetime`

4. **Congestion Proxy** (`compute_congestion_proxy_via_trip_speed`):
   - Speed: `trip_miles / trip_duration * 3600` (mph)
   - Aggregates: avg, min, max, stddev_pop, percentile(0.25), percentile(0.75), percentile(0.5)
   - Group by: `pulocationid`
   - Window: Slide(10s, 5s) on `pickup_datetime`

**Joining Features**:
```python
stateful_features = join_table(
    join_table(
        join_table(demand, fleet_composition, ['pulocationid', 'window_start', 'window_end']),
        netflow, ['pulocationid', 'window_start', 'window_end', 'taxi_type']),
    speed_features, ['pulocationid', 'window_start', 'window_end'])
```

---

### Stage 4: Sink (`main.py`)

The sink uses **JSON format with debezium-like schema/payload envelope** for Kafka Connect compatibility:

**Schema Row** (14 fields):
```
pulocationid (int32), window_start (int64, epoch ms), window_end (int64, epoch ms),
demand_per_zone (int64), taxi_type (string), fleet_composition_per_zone (int64),
netflow_in (int32), netflow_out (int32),
trip_avg_speed_mph (float), trip_min_speed_mph (float), trip_max_speed_mph (float),
trip_stddev_speed_mph (float), trip_percentile_25_speed_mph (double), trip_percentile_75_speed_mph (double)
```

**Payload Row** (15 values):
```
Same as schema + trip_median_speed_mph (double)
```

**⚠️ Bug**: Schema has 14 fields but payload has 15 — including `trip_median_speed_mph` in payload will cause cast error.

**Time Conversion**: `to_unix_timestamp()` converts Flink `TIMESTAMP(3)` to epoch milliseconds.

**Kafka Connect Configuration**: Uses `debezium-json` format with `key.fields=id`.

---

## Configuration

### Environment (.env)

| Variable | Description |
|----------|-------------|
| `bootstrap.servers` | Kafka broker (broker:29092 in Docker) |
| `table.local-time-zone` | America/New_York (set in main.py) |

### Flink Configuration (inline in `main.py`)

```python
t_env.get_config().set("table.local-time-zone", "America/New_York")
t_env.get_config().set("taskmanager.memory.network.max", "1gb")
t_env.get_config().set("taskmanager.memory.network.fraction", "0.5")
```

### JAR Dependencies

Required connectors:
- `flink-connector-kafka-4.0.0-2.0.jar`
- `kafka-clients-3.9.0.jar`
- `postgresql-42.7.7.jar`
- `flink-connector-jdbc-3.3.0-1.20.jar`
- `flink-avro-2.1.0.jar`
- Debezium Avro converters + Jackson

---

## Running

### Local Development

```bash
# 1. Zip dependencies
zip -r deps.zip parsers preprocess schemas.py online_feat.py

# 2. Submit to Flink cluster
flink run -m localhost:8090 -py main.py --pyFiles deps.zip
```

### Production (via Docker)

The job is submitted to the Flink cluster from outside the Docker container. The `flink-jobmanager` image includes `/opt/flink/usrlib` for JARs.

---

## Schema Reference

### Input Topics

| Topic | Format | Debezium? |
|-------|--------|-----------|
| `raw.public.green_taxi` | Avro (Confluent Schema Registry) | ✅ |
| `raw.default.yellow_taxi` | JSON | ✅ (nested payload) |
| `raw.datasource3.fhvhv_taxi` | JSON | ✅ (string payload) |

### Output Topic

- **Topic**: `online-feature`
- **Format**: `debezium-json` (key: `id`, value: schema + payload)
- **Key Fields**: `pulocationid`, `window_start`, `window_end`, `taxi_type`
- **Watermark Strategy**: Slide(10s, 5s) on `pickup_datetime`

---

## Key Files Reference

### **main.py** — Pipeline Orchestrator

**High-level flow**:
1. Create Flink streaming environment
2. Parse raw topics → tables
3. Preprocess tables
4. Union green+yellow → `traditional_taxi_table`
5. Compute online features for each union
6. Join features
7. Cast to schema/payload format
8. Execute insert to `online_feature` sink

**Critical**: `create_sink_table()` converts timestamps to epoch milliseconds via `to_unix_timestamp()`.

---

### **online_feat.py** — Feature Computation Engine

**Modular design**:
- `compute_window_count()`: Generic windowed aggregation template
- `compute_netflow_per_zone()`: Net flow calculation
- `compute_congestion_proxy_via_trip_speed()`: Speed statistics
- `join_table()`: Joins with column renaming to avoid name collision
- `compute_online_feature()`: Orchestrates all features and joins

**⚠️ Bug**: No division-by-zero guard in speed calculation (`trip_miles / trip_duration * 3600`).

---

### **schemas.py** — Schema Definitions

**DDL Constants** (5 tables):
- `PREPROCESSED_TRADITIONAL_TAXI_TABLE_WITH_KAFKA_CONNECTOR`
- `PREPROCESSED_FHVHV_TABLE_WITH_KAFKA_CONNECTOR`
- `STREAM_PREPROCESSED_TRADITIONAL_TAXI_TABLE_WITH_KAFKA_CONNECTOR`
- `STREAM_PREPROCESSED_FHVHV_TABLE_WITH_KAFKA_CONNECTOR`
- `STREAM_ONLINE_FEATURE_TABLE_WITH_KAFKA_CONNECTOR` (defined twice — first overwritten)

**TypeHotions**:
- `SINK_SCHEMA_TYPE`: ROW with schema/fields ARRAY
- `SINK_PAYLOAD_TYPE`: ROW with payload fields (including `trip_median_speed_mph`)

**Enums**:
- `TripType`: STREET_HAIL, DISPATCH
- `TaxiType`: GREEN, YELLOW, FHVH (⚠️ typo: `"fore_hire_vehicle"`)
- `PaymentType`: CREDIT_CARD, CASH, NO_CHARGE, etc.

---

### **preprocess/common.py** — Shared Helpers

- `ensure_boolean_type(col, true_value)`: Similarity match on trimmed/lowercased string
- `process_trip_type(col)`: IF ELSE chain mapping 1/2 to enum strings
- `process_payment_type(col)`: COALESCE of IF ELSE branches for 1-6 mapping

**⚠️ Inefficiency**: `process_payment_type` evaluates all 6 branches before `coalesce` picks one.

---

### **preprocess/green_taxi_preprocess.py** — Green Preprocess

**Transformations**:
1. Add boolean flags for `store_and_fwd_flag`
2. Map `trip_type` (1/2 → Street-hail/Dispatch)
3. Map `payment_type` (1-6 → enum)
4. Add `airport_fee=0`, `taxi_type='green_taxi'`
5. Drop: `ehail_fee`, `trip_type`, `payment_type`, `store_and_fwd_flag`
6. Rename: `trip_distance` → `trip_miles`, `p_*` → `*`
7. Compute `trip_duration = timestamp_diff(SECOND, pickup, dropoff)`

---

### **preprocess/yellow_taxi_preprocess.py** — Yellow Preprocess

**Transformations**:
1. Add boolean flags for `store_and_fwd_flag`
2. Map `payment_type` (1-6 → enum)
3. Handle null `airport_fee` → 0.0
4. Add `taxi_type='yellow_taxi'`, `trip_type='Street-hail'`
5. Drop: `payment_type`, `store_and_fwd_flag`, `airport_fee`
6. Rename: `trip_distance` → `trip_miles`, `p_*` → `*`
7. Compute `total_amount = sum(fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, airport_fee)`
8. Compute `trip_duration = timestamp_diff(SECOND, pickup, dropoff)`
9. Coalesce original `total_amount` with computed one

---

### **preprocess/fhvhv_preprocess.py** — FHV Preprocess

**Transformations**:
1. Add boolean flags for: `shared_request_flag`, `shared_match_flag`, `access_a_ride_flag`, `wav_request_flag`, `wav_match_flag`
2. Add `taxi_type='fore_hire_vehicle'`
3. Drop flag columns
4. Rename: `trip_time` → `trip_duration`, `base_passenger_fare` → `fare_amount`

---

## Development Conventions

- **Flink Table API**: All logic uses table expressions, not DataStream API
- **Debezium CDC format**: Парсers handle nested `payload.after.<field>.value` structure
- **Watermark strategy**: Slide windows with 1-second idle timeout for green, 5-second for FHV
- **Timezone**: Local time zone set to America/New_York for consistency
- **Idempotent joins**: `join_table()` renames join keys to avoid collision

---

## Dependencies

| Module | Requirements |
|--------|--------------|
| parsers, preprocess | pyflink, pyflink-table |
| schemas.py | pyflink-table, enum34 |

---

## Known Issues (from code review)

| # | Severity | File | Issue |
|---|----------|------|-------|
| 1 | 🔴 | schemas.py | `STREAM_ONLINE_FEATURE_TABLE_WITH_KAFKA_CONNECTOR` defined twice — first overwritten |
| 2 | 🔴 | online_feat.py | No division-by-zero guard in speed calculation |
| 3 | 🔴 | main.py | Schema/payload field count mismatch (14 vs 15) — `trip_median_speed_mph` missing in schema |
| 4 | 🟡 | schemas.py | Typo: `TaxiType.FHVH = "fore_hire_vehicle"` → `"for_hire_vehicle"` |
| 5 | 🟡 | main.py | `to_unix_timestamp()` uses timezone-sensitive epoch string `"1970-01-01 00:00:00"` |
| 6 | 🟡 | online_feat.py | Window sizes (10s/5s) are too small for production |
| 7 | 🟡 | yellow_taxi_parsing.py | Brittle 4-level nested ROW for MongoDB CDC |
| 8 | 🟡 | Various | `logging.basicConfig` at module level pollutes root logger |
| 9 | 🟡 | preprocess/common.py | `process_payment_type` evaluates all 6 branches via COALESCE |
| 10 | 🟡 | online_feat.py | `join_table()` only renames join keys, not all columns |
| 11 | 🟢 | Various | Unused imports (pandas, to_timestamp, Expression) |
| 12 | 🟢 | fhvhv_parsing.py | Mixed JSON extraction (`JSON_VALUE` in DDL vs `.json_value()` in select) |

---

**Priority fixes**: #1 (dead schema), #3 (schema/payload mismatch), and #2 (division by zero) will cause runtime failures.

---

## Related

- **Airflow integration**: Batch pipeline in `batch_processing/` handles historical data
- **Kafka topics**: Input from Debezium CDC, output to `online-feature` for Kafka Connect
- **Feature store**: Live features published to Kafka are consumed by downstream services
- **PostgreSQL datamart**: Online features sink here via Kafka Connect JDBC
