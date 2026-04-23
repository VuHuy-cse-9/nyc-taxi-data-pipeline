# NYC Taxi Data Platform — Project Context

## Project Overview

This is a **full-stack MLOps data platform** that ingests, processes, and serves NYC Taxi trip data (Yellow, Green, and For-Hire Vehicles) through both **offline (batch)** and **online (streaming)** pipelines. The system is designed as a lakehouse architecture with a feature store abstraction for ML engineers.

### Key Capabilities
- **Offline pipeline**: Airflow-orchestrated, Spark-based batch processing that ingests historical data from the NYC Taxi website, transforms it through bronze (data lake on MinIO) → silver (data warehouse on MinIO) → gold (data mart in PostgreSQL) layers
- **Online pipeline**: Debezium CDC captures real-time inserts from source databases (PostgreSQL, MongoDB, Cassandra), streams them through Kafka, Flink computes windowed online features, and Kafka Connect sinks results to the data mart
- **Feature store**: Feast serves as an abstraction layer over the data mart, providing historical features (via PostgreSQL offline store) and low-latency online features (via Redis online store)
- **Query layer**: Trino with Hive Metastore provides unified SQL access across MinIO parquet files and PostgreSQL

### Source Databases (simulated)
| Source | Taxi Type | Database |
|--------|-----------|----------|
| Datasource 1 | Green Taxi | PostgreSQL |
| Datasource 2 | Yellow Taxi | MongoDB |
| Datasource 3 | For-Hire Vehicle | Cassandra |

### Online Features Computed
- **Demand per Zone** — sliding window count of requests by pickup zone
- **Fleet Composition per Zone** — proportion of vehicle types active in a zone
- **Net Flow of Vehicles Between Zones** — directional movement signal (outgoing − incoming trips)
- **Congestion Proxy via Recent Trip Speeds** — average speed of completed trips in last 30 min

---

## Directory Structure

```
├── airflow/                  # Airflow DAGs and configuration
│   ├── dags/                 # DAG definitions (green_taxi_dag, yellow_taxi_dag, fhvhv_dag)
│   └── config/
├── batch_processing/         # Offline pipeline code (Spark jobs)
│   ├── ingest/               # Data ingestion (website → MinIO data lake)
│   ├── datawarehouse/        # Bronze → Silver transformations
│   ├── datamart/             # Silver → Gold transformations
│   ├── commons/              # Shared utilities
│   ├── configs/              # Configuration modules
│   ├── database/             # SQL scripts for datamart tables
│   └── schemas/              # Data schemas
├── stream_processing/        # Online pipeline (Flink streaming jobs)
│   ├── parsers/              # CDC event parsers
│   └── preprocess/           # Preprocessing logic
├── feature_store/            # Feast feature store
│   ├── feature_repo/         # Feature definitions, entities, feature_store.yaml
│   └── app.py                # Feature serving app
├── jars/                     # Connector JARs (Spark, Flink, Kafka Connect, Debezium)
├── local/                    # Local Docker configs and custom images
│   ├── spark/                # Spark Docker image
│   ├── debezium/             # Debezium init scripts
│   ├── cassandra/            # Cassandra Docker image
│   ├── mongodb/              # MongoDB init scripts
│   ├── generator/            # Data generator services for source DBs
│   ├── trino/                # Trino catalog configs
│   └── superset/             # Superset (BI) config
├── tests/                    # Test notebooks
├── docs/                     # Documentation
├── dataset/                  # Sample CSV data files
└── docker-compose.*.yaml     # Docker Compose files (split by subsystem)
```

---

## Docker Compose Files

| File | Purpose |
|------|---------|
| `docker-compose.source.yaml` | Source databases (PostgreSQL, Cassandra, MongoDB) + data generators |
| `docker-compose.batch.yaml` | Offline pipeline: MinIO, PostgreSQL datamart, Trino, Hive Metastore, Spark cluster |
| `docker-compose.stream.yaml` | Online pipeline: Kafka, Zookeeper, Schema Registry, Debezium CDC, Flink cluster, Kafka Connect |
| `docker-compose.airflow.yaml` | Airflow orchestrator |
| `docker-compose.feast.yaml` | Feast feature store (PostgreSQL offline store, Redis online store) |

**Network**: `doan_nyc_network` is the shared external network. Source and stream compose files declare it as `external: true`, so it must be created first (typically by `docker-compose.source.yaml`).

---

## Building and Running

### Prerequisites
- Docker & Docker Compose
- `uv` package manager (Python dependency management)
- Python >= 3.9

### Start Services

```bash
# 1. Start source databases + generators
docker compose -f docker-compose.source.yaml up -d

# 2. Start offline pipeline infrastructure (MinIO, Spark, Trino, datamart)
docker compose -f docker-compose.batch.yaml up -d

# 3. Start Airflow
docker compose -f docker-compose.airflow.yaml up -d

# 4. Start streaming pipeline infrastructure (Kafka, Debezium, Flink)
docker compose -f docker-compose.stream.yaml up -d

# 5. Start Feast
docker compose -f docker-compose.feast.yaml up -d
```

### Build Batch Processor Image
Required before running Airflow DAGs (tasks use DockerOperator):

```bash
cd batch_processing
docker build -t doan-batch_processor:latest .
```

### Run Offline Pipeline
- Access Airflow at `localhost:8085` (credentials: `airflow` / `airflow`)
- Trigger a DAG (e.g., `green_taxi_dag`) → use the "Trigger" button with config to backfill for a specific month/year

### Submit Flink Streaming Job
```bash
# Zip dependencies
cd stream_processing
zip -r deps.zip parsers preprocess schemas.py online_feat.py

# Submit to Flink
flink run -m localhost:8090 -py main.py --pyFiles deps.zip
```

### Key Service Ports
| Service | Port |
|---------|------|
| MinIO Console | 9001 |
| MinIO API | 9000 |
| Spark Master | 8080 |
| Trino | 10000 |
| Airflow | 8085 |
| Datamart (PostgreSQL) | 5434 |
| Kafka Broker | 9092 |
| Flink Dashboard | 8090 |
| Debezium UI | 8085 |
| Confluent Control Center | 9021 |
| Green Taxi DB (PostgreSQL) | 10001 |
| Yellow Taxi DB (MongoDB) | 27017 |
| FHV DB (Cassandra) | 9042 |

---

## Python Dependencies

Managed via `uv` in `pyproject.toml`. Three mutually exclusive extras:

| Extra | Key Packages |
|-------|-------------|
| `spark` | pyspark==4.0.2, apache-airflow, deltalake |
| `feast` | feast[redis], sqlalchemy |
| `flink` | apache-flink |

Core dependencies (always installed): aiokafka, cassandra-driver, minio, pandas, pymongo, psycopg, pyarrow, s3fs, pydantic, requests.

```bash
# Install core deps
uv sync

# Install with spark extra
uv sync --extra spark
```

---

## Pipeline Details

### Offline Pipeline (Airflow + Spark)

Each taxi type has its own DAG with 4 tasks:
1. **Check X url** — verifies data availability for the schedule month
2. **run_ingestion_X** — downloads data, uploads to MinIO data-lake bucket (parquet)
3. **run_warehouse_X** — Spark job: bronze → silver (fill defaults, categorize/format values, derive features, drop unused columns)
4. **run_create_partition_X** — creates monthly partition in datamart
5. **run_datamart_X** — Spark job: silver → gold (filter valid records, deduplicate, select features)

**Scheduling**: `@monthly` with `catchup=True` to support backfilling.

### Online Pipeline (Debezium + Kafka + Flink)

Flow: Source DB inserts → Debezium CDC → `Raw-x-topic` → Flink (parse, window aggregation, join sources) → `online-feature-topic` → Kafka Connect → Datamart PostgreSQL.

---

## Development Conventions

- **PySpark**: Uses built-in functions only (no UDFs) for performance. All operations are lazy; Spark optimizes the full pipeline plan.
- **Task granularity**: Pipelines are split by responsibility (not merged into single tasks) to facilitate retry, monitoring, and extensibility.
- **Source separation**: Each taxi type has its own DAG for clarity, independent re-runs, and source-specific scheduling.
- **JAR dependencies**: Stored in `jars/` directory and mounted into containers (Spark, Flink, Kafka Connect).

---

## Environment Configuration

All configuration is in `.env` at the project root. Key variables:
- MinIO credentials and endpoints
- Source database connection strings
- Datamart connection details
- Feast registry URL and Redis endpoint
- Airflow UID and project directory

---

## Useful Commands

```bash
# Create MinIO buckets (data-lake, data-warehouse) via MinIO Console at localhost:9001

# Register tables in Trino (exec into container)
trino
CREATE SCHEMA dwh.nyc_taxi WITH (location = 's3a://data-warehouse/nyc_taxi_dataset');
CALL dwh.system.register_table(schema_name => 'nyc_taxi', table_name => 'green_taxi', table_location => 's3://data-warehouse/nyc_taxi_dataset/green_taxi/11_2025.parquet');

# View Spark monitoring dashboard at localhost:8080
```
