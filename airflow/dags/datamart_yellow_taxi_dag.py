from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="datamart_yellow_taxi_dag",
    default_args=default_args,
    description="Run yellow taxi batch job via Docker",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # set cron if needed
    catchup=False,
    tags=["batch", "docker"],
) as dag:

    run_batch_processor = DockerOperator(
        task_id="run_datamart_yellow_taxi_job",
        image="doan-batch_processor:latest",
        api_version="auto",
        auto_remove="never",
        command="uv run -m datamart.yellow_taxi",
        environment={
            "MINIO_ENDPOINT": "minio:9000",
            "MINIO_ACCESS_KEY": "minio_access_key",
            "MINIO_SECRET_KEY": "minio_secret_key",
            "DATAMART_ENDPOINT": "datamart_db",
            "DATAMART_PORT": "5434",
            "SPARK_MASTER": "spark://spark-master:7077",
        },
        docker_url="unix://var/run/docker.sock",  # adjust if remote
        network_mode="doan_nyc_network",  # change if using custom network
    )

    run_batch_processor
