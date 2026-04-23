import pyspark.sql.functions as F
from schemas.models import PaymentType
import os
from pyspark.sql import DataFrame
from configs.config import settings

from database.minio_client import create_minio_client
from pyspark.sql import SparkSession
from typing import List
from commons.logging import logger


def transform_ts_to_asia_timezone(column_name: str) ->F.Column:
    # Convert timestamp from New York timezone to Asia/Ho_Chi_Minh timezone
    return F.when(
        F.col(column_name).isNull(), F.lit(None),
    ).otherwise(
        F.convert_timezone(
            F.lit("America/New_York"), F.lit("Asia/Ho_Chi_Minh"), column_name
        )
    )

def ensure_boolean_type(column_name: str, true_value: str)->F.Column:
    cleaned_column = F.trim(F.lower(F.col(column_name)))
    return F.when(
        F.col(column_name).isNull(), F.lit(None)
    ).when(
        cleaned_column == true_value.lower(), F.lit(True)
    ).otherwise(
        F.lit(False)
    )

def process_payment_type(column_name: str)->F.Column:
    return F.when(
        F.col(column_name).isNull(), F.lit(None),
    ).when(
        F.col(column_name) == 1, F.lit(PaymentType.CREDIT_CARD.value)
    ).when(
        F.col(column_name) == 2, F.lit(PaymentType.CASH.value)
    ).when(
        F.col(column_name) == 3, F.lit(PaymentType.NO_CHARGE.value)
    ).when(
        F.col(column_name) == 4, F.lit(PaymentType.DISPUTE.value)
    ).when(
        F.col(column_name) == 5, F.lit(PaymentType.UNKNOWN.value)
    ).when(
        F.col(column_name) == 6, F.lit(PaymentType.VOIDED_TRIP.value)
    ).otherwise(
        F.lit(None)
    )



def write_to_warehouse(df: DataFrame, dataset_name: str):
    """
    Write DataFrame to Hive Metastore as a table.
    """
    # Save as parquet file
    output_path = os.path.join(
        settings.dataset_name, 
        f"{dataset_name}.parquet")
    # df.write.parquet(
    #     f"s3a://{settings.warehouse_bucket}/{output_path}", mode="overwrite", compression="snappy")

    df.write.format("delta").save(f"s3a://{settings.warehouse_bucket}/{output_path}")

    return


def filter_data_path_by_month_year(obj_paths: List[str], month: int, year: int):
    for path in obj_paths:
        if f"{year}-{month:02d}" in path:
            return path
    raise Exception(f"No file found for month {month} and year {year}.")
    

def ingest_data(spark: SparkSession, dataset: str, month: int, year: int):
    # Connect to Minio
    minio_client = create_minio_client()

    minio_dir_path = f"{settings.dataset_name}/{dataset}"

    logger.info(f"Listing objects in Minio directory: {minio_dir_path} at bucket: {settings.datalake_bucket}")    
    
    # Read all parquet files in the directory
    objects = minio_client.list_objects(
        bucket_name=settings.datalake_bucket, prefix=minio_dir_path, recursive=True)
    object_names = [obj.object_name for obj in objects]

    logger.info(f"Found {len(object_names)} objects in Minio directory {minio_dir_path}.")

    object_name = \
        filter_data_path_by_month_year(
            object_names, 
            month, 
            year
        )
    
    logger.info(f"Reading data from Minio object: {object_name}...")
        
    path_read = f"s3a://{settings.datalake_bucket}/" + object_name
    df = spark.read.parquet(path_read)

    return df