from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from configs.config import settings
from commons.logging import logger

def create_spark_session() -> SparkSession:
    """
    Create a Spark session with the specified configuration.
    """
    jars = "jars/hadoop-aws-3.4.1.jar,jars/bundle-2.32.24.jar,jars/postgresql-42.7.7.jar"
    builder = SparkSession.builder\
        .appName("Yellow Taxi Mart") \
        .config('spark.memory.fraction', '0.8')\
        .config("spark.executor.memory", "12g")\
        .config("spark.driver.memory", "12g") \
        .config('spark.hadoop.fs.s3a.endpoint', settings.minio_endpoint)\
        .config('spark.hadoop.fs.s3a.access.key', settings.minio_access_key)\
        .config('spark.hadoop.fs.s3a.secret.key', settings.minio_secret_key)\
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')\
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')\
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')\
        .config('spark.jars', jars)

    spark = builder.getOrCreate()

     # Check spark session is created successfully
    if spark is None:
        raise Exception("Failed to create Spark session.")

    return spark

def ingest_data(spark: SparkSession):
    path_read = f"s3a://{settings.warehouse_bucket}/" + "nyc_taxi_dataset/traditional_taxi.parquet"
    df = spark.read.parquet(path_read)
    return df

def sink_data(df: DataFrame):
    df.write.jdbc(
        url="jdbc:postgresql://{}:{}/{}".format(settings.datamart_endpoint, settings.datamart_port, settings.datamart_db),
        table="public.{}".format(settings.datamart_yellow_taxi_table),
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
        (F.col("taxi_type") == "yellow_taxi")
    ).dropDuplicates(
        ["pickup_datetime", "dropoff_datetime", 
         "trip_miles", "pulocationid", "dolocationid"]
    ).select(
        "pickup_datetime", "dropoff_datetime", 
        "trip_miles", "pulocationid", "dolocationid",
        "passenger_count", "fare_amount"
    )

    sink_data(df)

    # Stop the Spark session
    spark.stop()
    logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()