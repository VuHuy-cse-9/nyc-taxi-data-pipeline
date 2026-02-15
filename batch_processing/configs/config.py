from pydantic_settings import BaseSettings


class Config(BaseSettings):
    # MinIO configuration
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool = False  # Set to True if using HTTPS

    # Lake house
    datalake_bucket: str = "data-lake"

    # Warehouse
    warehouse_bucket: str = "data-warehouse"
    metadata_uri: str = "thrift://localhost:9083"

    # Dataset name
    dataset_name: str = "nyc_taxi_dataset"
    fhvhv_dataset_name: str = "for_hire_vehicle"
    green_taxi_dataset_name: str = "green_taxi"
    yellow_taxi_dataset_name: str = "yellow_taxi"


    # Data mart
    datamart_endpoint: str = "localhost"
    datamart_port: int = 5434
    datamart_user: str = "datamart_user"
    datamart_password: str = "datamart_password"
    datamart_db: str = "datamart"
    datamart_yellow_taxi_table: str = "yellow_taxi_mart"
    datamart_green_taxi_table: str = "green_taxi_mart"
    datamart_fhvhv_table: str = "fhvhv_mart"

    # Ingestion time
    ingestion_year: int = 2025
    ingestion_month: int = 7

    # Spark configuration
    spark_master: str = "local[*]"
    spark_app_name: str = "BatchProcessingApp"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra='ignore'


settings = Config()