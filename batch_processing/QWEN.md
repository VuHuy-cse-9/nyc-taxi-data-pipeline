# NYC Taxi Batch Processing Pipeline — Context

## Overview

This module implements the **offline (batch) pipeline** for NYC Taxi data processing. It ingests historical trip data, transforms it through a lakehouse architecture (Bronze → Silver → Gold), and loads it into a PostgreSQL data mart with partitioning for efficient querying.

### Pipeline Stages

1. **Ingest (Bronze)**: Download parquet files from NYC Taxi website → upload to MinIO `data-lake` bucket
2. **Data Warehouse (Silver)**: Spark transformations on MinIO → clean, enrich, write to `data-warehouse` bucket as Delta format
3. **Data Mart (Gold)**: Spark transformations → filter valid records → append to PostgreSQL partitioned tables
4. **Feature Store**: Data is exposed via Feast for ML training/inference

---

## Directory Structure

```
batch_processing/
├── commons/              # Shared utilities & constants
│   ├── constants.py      # Bucket paths, Base URLs, Folder mappings
│   └── logging.py        # Centralized logger configuration
├── configs/              # Configuration management
│   ├── config.py         # Pydantic settings for MinIO, Spark, Datamart
│   └── spark.py          # Spark session factory with Delta support
├── database/             # Storage clients & SQL scripts
│   ├── minio_client.py   # MinIO client (bucket creation, upload)
│   ├── datamart_client.py # PostgreSQL partition management
│   └── sql/              # SQL DDL scripts for datamart tables
├── datamart/             # Gold layer transformations
│   ├── common.py         # Shared ingestion/sink utility functions
│   ├── green_taxi.py     # Green taxi data mart transformation
│   ├── yellow_taxi.py    # Yellow taxi data mart transformation
│   └── fhvhv.py          # For-Hire Vehicle data mart transformation
├── datasource/           # Data ingest helpers
│   └── ingest_data.py    # Local file → MinIO uploader (unused)
├── datawarehouse/        # Silver layer transformations
│   ├── common.py         # Shared preprocessing utilities
│   ├── green_taxi.py     # Green taxi warehouse transformation
│   ├── yellow_taxi.py    # Yellow taxi warehouse transformation
│   ├── fhvhv.py          # FHV warehouse transformation
│   └── taxi_zone.py      # Taxi zone lookup metadata processor
├── ingest/               # Bronze layer (web → MinIO)
│   └── ingest.py         # Download NYC Taxi website → MinIO data-lake
├── schemas/              # Type definitions
│   └── models.py         # Enums (TripType, TaxiType, PaymentType)
└── Dockerfile            # Batch processor container image
```

---

## Data Flow

```
Website (Parquet) → MinIO data-lake (Bronze)
                  → Spark transformations
                  → MinIO data-warehouse (Silver, Delta)
                  → Spark transformations
                  → PostgreSQL datamart (Gold, partitioned)
```

### Transformation Details

**Bronze → Silver (datawarehouse/):**
- Convert timezone from America/New_York to Asia/Ho_Chi_Minh
- Parse `store_and_fwd_flag` to boolean
- Map `payment_type` (1-6) to enum strings
- Derive `trip_duration_seconds` from timestamp difference
- Rename `trip_distance` → `trip_miles`
- Add `taxi_type` and `trip_type` columns

**Silver → Gold (datamart/):**
- Filter: `passenger_count > 0 AND trip_duration_seconds > 100 AND trip_miles > 0 AND fare_amount > 0`
- Drop duplicates by (pickup_datetime, dropoff_datetime, trip_miles, pulocationid, dolocationid)
- Select minimal feature set for analytics

---

## Configuration

All settings are in `configs/config.py` (Pydantic `BaseSettings`), loaded from `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `minio_endpoint` | localhost:9000 | MinIO host |
| `minio_access_key` | minio_access_key | MinIO credentials |
| `minio_secret_key` | minio_secret_key | MinIO credentials |
| `minio_secure` | False | Use HTTPS |
| `datalake_bucket` | data-lake | Source MinIO bucket |
| `warehouse_bucket` | data-warehouse | Transformed MinIO bucket |
| `datamart_endpoint` | localhost | PostgreSQL host |
| `datamart_port` | 5434 | PostgreSQL port |
| `datamart_user` | datamart_user | Database credentials |
| `datamart_password` | datamart_password | Database credentials |
| `spark_master` | spark://localhost:7077 | Spark cluster master |
| `ingestion_year` | 2025 | Batch job year |
| `ingestion_month` | 7 | Batch job month |

---

## Running

### Docker-Based Execution (Production)

Build the batch processor image:

```bash
docker build -t doan-batch_processor:latest .
```

Execute scripts inside the container (via Airflow DockerOperator or manually):

```bash
# Ingest a specific month
python -m ingest.ingest  # datasource_to_download, ingestion_year, ingestion_month via .env

# Transform to data warehouse
python -m datawarehouse.green_taxi
python -m datawarehouse.yellow_taxi
python -m datawarehouse.fhvhv

# Create datamart partition
python -m database.datamart_client

# Load to datamart
python -m datamart.green_taxi
python -m datamart.yellow_taxi
python -m datamart.fhvhv
```

### Local Execution (Development)

Install dependencies:

```bash
uv sync --extra spark
```

Set `.env` with local credentials, then run:

```bash
python -m datawarehouse.green_taxi
```

---

## Key Files Reference

### **configs/spark.py** — Spark Session Factory

Creates a Spark session with Delta Lake support:

- Registers S3A filesystem for MinIO access
- Configures memory (2GB driver, 2GB executor, 0.6 fraction)
- Loads JARs: `hadoop-aws-3.4.1.jar`, `bundle-2.32.24.jar`, `postgresql-42.7.7.jar`
- Enables Delta Lake extensions

**Note**: `.master()` is commented out — runs in local mode by default. Uncomment to submit to Spark cluster.

---

### **database/minio_client.py** — MinIO Operations

```python
create_minio_client()       # Returns Minio client
create_bucket_if_not_exists(bucket_name)  # Create data-lake/warehouse
upload_file_to_minio(bucket, file_path, object_name)  # Upload if not exists
```

Upload is **idempotent**: checks if object exists before uploading.

---

### **database/datamart_client.py** — PostgreSQL Partitioning

Creates monthly partitions for range-partitioned datamart tables:

```sql
CREATE TABLE green_taxi_mart (
    pickup_datetime TIMESTAMP,
    ...
) PARTITION BY RANGE (pickup_datetime);

CREATE TABLE green_taxi_mart_2025_07 PARTITION OF green_taxi_mart
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
```

Run the partition job with `python -m database.datamart_client` — it checks if the partition exists and creates if missing.

---

### ** ingest/ingest.py** — Web → MinIO Ingestion

Downloads parquet from NYC Taxi website with **pause/resume support**:

- Uses HTTP Range headers to resume partial downloads
- 8MB chunk size for streaming
- Logs progress every chunk
- Removes local file after successful upload
- Returns exit code 10 if file already exists in MinIO

---

### **datawarehouse/common.py** — Shared Transformation Utilities

```python
transform_ts_to_asia_timezone(col)    # Convert NY timezone to Asia/Ho_Chi_Minh
ensure_boolean_type(col, true_value)  # Parse "Y"/"N" to boolean
process_payment_type(col)             # Map 1-6 → enum strings
ingest_data(spark, dataset, month, year)  # Read parquet from MinIO
write_to_warehouse(df, dataset_name)  # Write as Delta to data-warehouse bucket
```

**Bug**: `trip_duration_seconds` calculation multiplies by 60 (line 77 in green_taxi.py) — see Severity #1 in code review.

---

## Schema Notes

### Input (Bronze)

**Green/Yellow Taxi fields**: vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, ratecode_id, store_and_fwd_flag, pickup_location_id, dropoff_location_id, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, ehail_fee, trip_type

**FHV fields**: dispatching_base_num, affiliated_base_num, pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, sr_flag

### Output (Gold)

**Data mart tables** have a minimal schema (7 columns):
- `pickup_datetime`, `dropoff_datetime` (TIMESTAMP)
- `trip_miles` (NUMERIC)
- `pulocationid`, `dolocationid` (INTEGER)
- `passenger_count` (INTEGER)
- `fare_amount` (NUMERIC)

---

## Dependencies

Minimal required dependencies per module:

| Module | Dependencies |
|--------|-------------|
| ingest, database clients | minio, requests, s3fs |
| datawarehouse, datamart | pyspark, minio, psycopg |
| configs | pydantic-settings |
| schemas | enum34 |

---

## Development Conventions

- **No UDFs**: All transformations use PySpark built-in functions for performance
- **Lazy evaluation**: No actions until the final write to ensure Spark can optimize the full plan
- **Delta Lake format**: Silver layer uses Delta for ACID compliance and time travel
- **Monthly partitioning**: PostgreSQL datamart tables are partitioned by month via `pickup_datetime`
- **Idempotent operations**: Check before upload/insert to allow safe retries
- **Timezone normalization**: All timestamps converted to Asia/Ho_Chi_Minh homogeneous zone

---

## Docker Image

**Base**: `apache/spark:4.0.2-java21`

**Layers**:
1. Copy uv binary
2. Install system deps (git, curl)
3. Copy source
4. `uv sync --extra spark`
5. Entrypoint: `CMD sleep infinity` (for interactive use by Airflow)

The image is built once and reused by Airflow DockerOperator tasks.

---

## Known Issues (from code review)

| # | Severity | File | Issue |
|---|----------|------|-------|
| 1 | 🔴 | green_taxi.py, yellow_taxi.py | `trip_duration_seconds = diff * 60` — should be `diff` only |
| 2 | 🔴 | yellow_taxi.py | `sum_columns()` returns 0.0 when any component is NULL |
| 3 | 🟡 | spark.py | `.master()` commented out — runs in local mode |
| 4 | 🟡 | ingest.py | Return code 10 on "already exists" is non-standard |
| 5 | 🟡 | common.py | String match in `filter_data_path_by_month_year` is fragile |
| 6 | 🟡 | fhvhv.py | `TaxiType.FHVH` typo: `"fore_hire_vehicle"` → `"for_hire_vehicle"` |

---

## related

- **Airflow integration**: DAGs in `airflow/dags/` use DockerOperator to invoke these scripts
- **Feature store**: Data from PostgreSQL datamart is consumed by Feast for ML feature serving
- **Trino query**: Hive Metastore registers Delta tables for SQL-based exploration
