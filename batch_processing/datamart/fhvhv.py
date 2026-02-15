from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from commons.logging import logger
from configs.spark import create_spark_session
from datamart.common import ingest_data, sink_data
from configs.config import settings


def filter_data(df: DataFrame):    
    valid_trip = \
        (F.col("trip_duration_seconds") > 100) & \
        (F.col("trip_miles") > 0) & \
        (F.col("fare_amount") > 0)

    df = df.filter(
        F.col("pickup_datetime").isNotNull() &\
        valid_trip
    ).dropDuplicates(
        ["pickup_datetime", "dropoff_datetime",
         "trip_miles", "pulocationid", "dolocationid"]
    ).select(
        "request_datetime", "on_scene_datetime",
        "pickup_datetime", "dropoff_datetime", 
        "trip_miles", "pulocationid", "dolocationid",
        "fare_amount"
    )
    return df

def main():
    # Create Spark session
    spark = create_spark_session()
    logger.info("Spark session created successfully.")

    # Ingest data
    # Ingest data
    df = ingest_data(
        spark, 
        settings.fhvhv_dataset_name, 
        settings.ingestion_month, 
        settings.ingestion_year
    )
    logger.info("Data ingested successfully.")

    # Filter data
    df = filter_data(df)
    logger.info("Data filtered successfully.")

    # Sink data
    sink_data(df, settings.datamart_fhvhv_table)
    logger.info("Data sunk successfully.")

    # Stop the Spark session
    spark.stop()
    logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()