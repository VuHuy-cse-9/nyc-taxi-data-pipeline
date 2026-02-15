from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from configs.config import settings
from configs.spark import create_spark_session
from commons.logging import logger

def ingest_data(spark: SparkSession):
    path_read = f"s3a://{settings.warehouse_bucket}/" + "nyc_taxi_dataset/traditional_taxi.parquet"
    df = spark.read.parquet(path_read)
    return df

def filter_data(df: DataFrame):
    # Your data processing logic here
    within_july_condition = \
        (F.year("pickup_datetime") == F.lit(settings.ingestion_year)) & \
        (F.month("pickup_datetime") == F.lit(settings.ingestion_month))
    
    valid_trip = \
        F.col("passenger_count").isNotNull() & \
        (F.col("passenger_count") > 0) & \
        (F.col("trip_duration_seconds") > 100) & \
        (F.col("trip_miles") > 0) & \
        (F.col("fare_amount") > 0)

    df = df.filter(
        F.col("pickup_datetime").isNotNull() &\
        within_july_condition & \
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

def sink_data(df: DataFrame):
    df.write.jdbc(
        url="jdbc:postgresql://{}:{}/{}".format(settings.datamart_endpoint, settings.datamart_port, settings.datamart_db),
        table="public.{}".format(settings.datamart_green_taxi_table),
        mode="overwrite",
        properties={
            "user": settings.datamart_user,
            "password": settings.datamart_password,
            "driver": "org.postgresql.Driver"
        }
    )
    return

def main():
    # Create Spark session
    spark = create_spark_session()
    logger.info("Spark session created successfully.")

    # Ingest data
    df = ingest_data(spark)
    logger.info("Data ingested successfully.")
    df = filter_data(df)
    print("Data filtered successfully.")

    df.show(5, truncate=False)

    sink_data(df)

    # Stop the Spark session
    spark.stop()
    
    logger.info("Spark session stopped.")
if __name__ == "__main__":
    main()