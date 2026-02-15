from pyspark.sql import SparkSession, DataFrame
from configs.config import settings
from commons.logging import logger

def ingest_data(spark: SparkSession, dataset: str, month: int, year: int):
    # Connect to Minio
    minio_path = f"{settings.dataset_name}/{dataset}/{month:02d}_{year}.parquet"
    
    logger.info(f"Reading data from Minio object: {minio_path}...")
        
    path_read = f"s3a://{settings.warehouse_bucket}/" + minio_path
    df = spark.read.parquet(path_read)

    return df

def sink_data(df: DataFrame, table_name: str):
    df.write.jdbc(
        url="jdbc:postgresql://{}:{}/{}".format(settings.datamart_endpoint, settings.datamart_port, settings.datamart_db),
        table="public.{}".format(table_name),
        mode="append",
        properties={
            "user": settings.datamart_user,
            "password": settings.datamart_password,
            "driver": "org.postgresql.Driver"
        }
    )
    return