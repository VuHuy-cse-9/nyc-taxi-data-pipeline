from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from configs.spark import create_spark_session
from commons.logging import logger
from datamart.common import ingest_data, sink_data
from configs.config import settings

def filter_data(df: DataFrame):
    valid_trip = \
        F.col("passenger_count").isNotNull() & \
        (F.col("passenger_count") > 0) & \
        (F.col("trip_duration_seconds") > 100) & \
        (F.col("trip_miles") > 0) & \
        (F.col("fare_amount") > 0)

    df = df.filter(
        F.col("pickup_datetime").isNotNull() &\
        valid_trip & \
        (F.col("taxi_type") == "green_taxi")
    ).dropDuplicates(
        ["pickup_datetime", "dropoff_datetime", 
         "trip_miles", "pulocationid", "dolocationid"]
    ).select(
        "pickup_datetime", "dropoff_datetime", 
        "trip_miles", "pulocationid", "dolocationid",
        "passenger_count", "fare_amount"
    )
    return df

def main():
    # Create Spark session
    spark = create_spark_session()
    logger.info("Spark session created successfully.")

    # Ingest data
    df = ingest_data(
        spark, 
        settings.green_taxi_dataset_name, 
        settings.ingestion_month, 
        settings.ingestion_year
    )
    logger.info("Data ingested successfully.")

    df = filter_data(df)
    logger.info("Data filtered successfully.")

    sink_data(df, settings.datamart_green_taxi_table)
    logger.info("Data sunk successfully.")
    # Stop the Spark session
    spark.stop()
    
    logger.info("Spark session stopped.")
if __name__ == "__main__":
    main()