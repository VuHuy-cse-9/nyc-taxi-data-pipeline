# NYC Taxi Airflow Orchestrator — Context

## Overview

The `airflow/` folder contains the **Apache Airflow orchestration** configuration for the NYC Taxi batch processing pipeline. It manages the scheduled, repeatable execution of data ingestion, transformation, and loading tasks for Green, Yellow, and For-Hire Vehicle (FHV) taxi data.

---

## Directory Structure

```
airflow/
├── dags/                     # DAG definitions
│   ├── green_taxi_dag.py     # DAG for green taxi processing
│   ├── yellow_taxi_dag.py    # DAG for yellow taxi processing
│   └── fhvhv_dag.py          # DAG for for-hire vehicle processing
├── config/                   # Airflow configuration
│   └── airflow.cfg           # Custom Airflow config (base path settings)
└── .env                      # Airflow user ID configuration
```

---

## Architecture

### Component Relationships

```
Scheduler → Reads DAGs from dags/ folder
    ↓
Triggers tasks based on schedule (@monthly)
    ↓
Executor (CeleryExecutor) → Runs tasks via DockerOperator
    ↓
Docker containers → Execute batch_processing scripts
    ↓
Batch processing → Ingest → Warehouse → Datamart
```

### Services (from docker-compose.airflow.yaml)

| Service | Purpose | Port |
|---------|---------|------|
| `airflow-apiserver` | REST API server | 8085 |
| `airflow-scheduler` | DAG scheduling & parsing | — |
| `airflow-worker` | Celery worker for task execution | — |
| `airflow-triggerer` | Triggered task coordinator | — |
| `airflow-dag-processor` | DAG file processor | — |
| `airflow-init` | One-time init (db create, user create) | — |
| `postgres` | Airflow metadata database | 5432 (internal) |
| `redis` | Celery broker | 6379 (internal) |

---

## DAG Details

All three DAGs (green_taxi_dag, yellow_taxi_dag, fhvhv_dag) follow the same pattern:

### DAG Schedule

- **Start date**: `2025-01-01`
- **Schedule**: `@monthly` (runs on the 1st of each month)
- **Catchup**: `True` (backfills missed runs)
- **Tags**: `batch`, `docker`

### DAG Tasks (8 total per DAG)

| Task ID | Operator | Purpose |
|---------|----------|---------|
| `check_X_url` | PythonOperator | Verify data file exists on NYC Taxi website (skips if 404) |
| `run_ingestion_X` | DockerOperator | Download parquet → upload to MinIO `data-lake` |
| `run_warehouse_X` | DockerOperator | Spark: Bronze → Silver (Delta on MinIO) |
| `run_create_partition_X` | DockerOperator | Create monthly partition in PostgreSQL datamart |
| `run_datamart_X` | DockerOperator | Spark: Silver → Gold (PostgreSQL partition) |

### Task Dependencies

```python
run_check_url_job >> run_ingestion_batch_processor >> [run_warehouse_batch_processor, run_create_partition_job] >> run_datamart_batch_processor
```

Graphically:
```
check_url  →  ingestion  →  warehouse  →  datamart
                       ↘  create_partition  ↗
```

The `warehouse` and `create_partition` tasks run **in parallel** because they don't depend on each other.

---

## Configuration

### Environment Variables (`.env`)

| Variable | Value | Purpose |
|----------|-------|---------|
| `AIRFLOW_UID` | 501 | User ID for file permissions |

### Airflow Configuration (`config/airflow.cfg`)

Key settings for this deployment:

| Section | Key | Value | Note |
|---------|-----|-------|------|
| `[core]` | `executor` | LocalExecutor (inside Dockerfile) | Overridden by compose to CeleryExecutor |
| `[core]` | `dags_folder` | `/opt/airflow/dags` | Volume-mounted from `airflow/dags` |
| `[core]` | `executor` | CeleryExecutor | Set via environment in docker-compose |
| `[database]` | `sql_alchemy_conn` | SQLite (default) | Overridden to PostgreSQL |
| `[logging]` | `base_log_folder` | `/opt/airflow/logs` | Volume-mounted from `airflow/logs` |
| `[core]` | `simple_auth_manager_users` | `admin:admin` | Basic auth credentials |

### Environment Variables (from docker-compose)

| Variable | Default | Purpose |
|----------|---------|---------|
| `AIRFLOW_IMAGE_NAME` | `apache/airflow:3.1.7` | Airflow Docker image |
| `_AIRFLOW_WWW_USER_USERNAME` | `airflow` | Web UI username |
| `_AIRFLOW_WWW_USER_PASSWORD` | `airflow` | Web UI password |
| `AIRFLOW_PROJ_DIR` | `./airflow` | Base path for volume mounts |
| `AIRFLOW_CONFIG` | `/opt/airflow/config/airflow.cfg` | Custom config location |

---

## DockerOperator Configuration

Each task uses `DockerOperator` to run batch processing inside isolated containers:

```python
DockerOperator(
    task_id="run_ingestion_green_taxi_job",
    image="doan-batch_processor:latest",
    container_name="ingestion_green_taxi_container",
    command="uv run -m ingest.ingest",
    environment=common_env,
    docker_url="unix://var/run/docker.sock",
    network_mode="doan_nyc_network",
)
```

### Environment Variables Passed to Containers

| Variable | Source | Example |
|----------|--------|---------|
| `DATASOURCE_TO_DOWNLOAD` | DAG config | `green` / `yellow` / `fhvhv` |
| `INGESTION_YEAR` | Airflow macro | `2025` |
| `INGESTION_MONTH` | Airflow macro | `07` |
| `MINIO_ENDPOINT` | Hardcoded | `minio:9000` |
| `MINIO_ACCESS_KEY` | Hardcoded | `minio_access_key` |
| `MINIO_SECRET_KEY` | Hardcoded | `minio_secret_key` |
| `DATAMART_ENDPOINT` | Hardcoded | `datamart_db` |
| `DATAMART_PORT` | Hardcoded | `5434` |
| `SPARK_MASTER` | Hardcoded | `spark://spark-master:7077` |

### Airflow Macros Used

- `{{ data_interval_start.year }}` — Extract year from the DAG's data interval
- `{{ '%02d' % data_interval_start.month }}` — Zero-padded month string

These macros enable the DAG to backfill specific months correctly.

---

## Running Airflow

### Start Services

```bash
# Start Airflow with CeleryExecutor
docker compose -f docker-compose.airflow.yaml up -d
```

### Access Web UI

- **URL**: `http://localhost:8085`
- **Username**: `airflow`
- **Password**: `airflow`

### Trigger a DAG Manually

1. Open Airflow UI
2. Select a DAG (e.g., `green_taxi_dag`)
3. Click **Trigger DAG**
4. Optionally pass config to override default month/year

### Backfill a Specific Month

Click **Trigger DAG** → populate `Run config` with:
```json
{
  "ingestion_year": 2025,
  "ingestion_month": 3
}
```

Or use CLI:
```bash
airflow dags trigger -c '{"INGESTION_YEAR": "2025", "INGESTION_MONTH": "03"}' green_taxi_dag
```

### Manage DAGs

```bash
# List DAGs
airflow dags list

# Show DAG info
airflow dags show green_taxi_dag

# Test a task
airflow tasks test green_taxi_dag check_green_taxi_url 2025-01-01
```

---

## Batch Processor Image

All DAG tasks use a common Docker image: `doan-batch_processor:latest`

**Build command**:
```bash
cd batch_processing
docker build -t doan-batch_processor:latest .
```

**Container image**:
- Based on `apache/spark:4.0.2-java21`
- Installs dependencies via `uv sync --extra spark`
- Entrypoint: `CMD sleep infinity` (keeps container alive for interactive commands)

---

## Concurrency Settings

From `docker-compose.airflow.yaml` environment:

| Setting | Value | Effect |
|---------|-------|--------|
| `AIRFLOW__CORE__PARALLELISM` | 32 | Max tasks running across all DAGs |
| `AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG` | 16 | Max tasks per DAG run |
| `AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG` | 16 | Max concurrent DAG runs |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `true` | New DAGs start paused |

---

## Monitoring & Debugging

### View Logs

- **Web UI**: Click any task → `Logs` tab
- **CLI**: `airflow tasks logs green_taxi_dag check_green_taxi_url 2025-01-01`
- **File**: `/opt/airflow/logs/dags/green_taxi_dag/`

### Health Check

The Airflow scheduler has a built-in health endpoint:
```bash
curl http://localhost:8974/health
```

### CLI Commands

```bash
# View running jobs
airflow jobs list

# Check scheduler status
airflow scheduler --help

# List all connections (Airflow stores MinIO credentials here)
airflow connections list
```

---

## Known Issues & Limitations

| Issue | Impact | Workaround |
|-------|--------|------------|
| Hardcoded `doan_nyc_network` network | Fails if network name changes | Update `network_mode` in DAG |
| `uv` dependency in container | Requires `doan-batch_processor` image build | Rebuild image on package changes |
| No task-level retries | Failed tasks require manual trigger | Set `retries` in DAG or task args |
| Snapshot-based config | Config changes require DAG reload | Use Airflow UI to reload DAGs |

---

## Development Conventions

- **One DAG per taxi type**: Green, Yellow, FHV have separate DAGs for independent scheduling
- **Monthly cadence**: Schedule `@monthly` to match NYC Taxi data release cadence
- **Catchup enabled**: Backfill historical months automatically
- **URL check first**: Skip tasks if source file doesn't exist yet
- **Parallel partitioning**: `create_partition` runs in parallel with `warehouse` for efficiency
- **Docker-as-executor**: Uses `DockerOperator` for isolation and reproducibility

---

## Related

- **Batch processing**: `batch_processing/` contains the Spark scripts called by Airflow
- **Metadata storage**: PostgreSQL (`airflow` database) stores task state, DAG runs, connections
- **Broker**: Redis stores Celery task queue
- **External network**: All Airflow services connect to `doan_nyc_network` to reach MinIO, Spark, PostgreSQL

---

## Quick Reference

| Objective | Command / UI Action |
|-----------|---------------------|
| Start Airflow | `docker compose -f docker-compose.airflow.yaml up -d` |
| Stop Airflow | `docker compose -f docker-compose.airflow.yaml down` |
| View logs | `docker compose logs -f airflow-scheduler airflow-worker` |
| Trigger DAG | Airflow UI → DAG name → Trigger button |
| Backfill specific month | Trigger DAG → JSON config with `INGESTION_YEAR`, `INGESTION_MONTH` |
| Check DAG syntax | `airflow dags list` |
| Rebuild batch processor | `cd batch_processing && docker build -t doan-batch_processor:latest .` |
