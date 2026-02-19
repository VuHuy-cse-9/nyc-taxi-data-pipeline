from datetime import datetime
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.common.compat.standard.operators import PythonOperator
import requests

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

def check_url_callable(**kwargs):
    url = kwargs["url"]
    try:
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            return True
        else:
            raise AirflowSkipException(f"File not found at URL: {url} (status code: {response.status_code})")
    except requests.RequestException as e:
        raise AirflowSkipException(f"Error checking URL: {url} ({str(e)})")

with DAG(
    dag_id="fhvhv_dag",
    default_args=default_args,
    description="Run fhvhv batch job via Docker",
    start_date=datetime(2025, 1, 1),
    schedule="@monthly",
    catchup=True,
    tags=["batch", "docker"],
) as dag:
    
    # Airflow macros for partition values
    YEAR = "{{ data_interval_start.year }}"
    MONTH = "{{ '%02d' % data_interval_start.month }}"

    common_env = {
        "DATASOURCE_TO_DOWNLOAD": "fhvhv",
        "INGESTION_YEAR": YEAR,
        "INGESTION_MONTH": MONTH,
        "MINIO_ENDPOINT": "minio:9000",
        "MINIO_ACCESS_KEY": "minio_access_key",
        "MINIO_SECRET_KEY": "minio_secret_key",
        "DATAMART_ENDPOINT": "datamart_db",
        "DATAMART_PORT": "5434",
        "SPARK_MASTER": "spark://spark-master:7077",
    }

    # check url job
    run_check_url_job = PythonOperator(
        task_id="check_fhvhv_url",
        python_callable=check_url_callable,
        op_kwargs={
            "url": f"https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{YEAR}-{MONTH}.parquet"
        },
    )
    
    # Ingestion job
    run_ingestion_batch_processor = DockerOperator(
        task_id="run_ingestion_fhvhv_job",
        image="doan-batch_processor:latest",
        container_name="ingestion_fhvhv_container",
        api_version="auto",
        auto_remove="force",
        command="uv run -m ingest.ingest",
        environment=common_env,
        docker_url="unix://var/run/docker.sock",  # adjust if remote
        network_mode="doan_nyc_network",  # change if using custom network
    )

    # Warehouse job
    run_warehouse_batch_processor = DockerOperator(
        task_id="run_warehouse_fhvhv_job",
        image="doan-batch_processor:latest",
        container_name="warehouse_fhvhv_container",
        api_version="auto",
        auto_remove="force",
        command="uv run -m datawarehouse.fhvhv",
        environment=common_env,
        docker_url="unix://var/run/docker.sock",  # adjust if remote
        network_mode="doan_nyc_network",  # change if using custom network
    )

    # Create partition job
    run_create_partition_job = DockerOperator(
        task_id="run_create_partition_fhvhv_job",
        image="doan-batch_processor:latest",
        container_name="create_partition_fhvhv_container",
        api_version="auto",
        auto_remove="force",
        command="uv run -m database.datamart_client",
        environment=common_env,
        docker_url="unix://var/run/docker.sock",  # adjust if remote
        network_mode="doan_nyc_network",  # change if using custom network
    )

    # Datamart job
    run_datamart_batch_processor = DockerOperator(
        task_id="run_datamart_fhvhv_job",
        image="doan-batch_processor:latest",
        container_name="datamart_fhvhv_container",
        api_version="auto",
        auto_remove="force",
        command="uv run -m datamart.fhvhv",
        environment=common_env,
        docker_url="unix://var/run/docker.sock",  # adjust if remote
        network_mode="doan_nyc_network",  # change if using custom network
    )

    # Run [ingestion->warehouse] parallel with [create_partition], then run datamart
    run_check_url_job >> run_ingestion_batch_processor >> [run_warehouse_batch_processor, run_create_partition_job] >> run_datamart_batch_processor
