from pyspark.sql import SparkSession, DataFrame
import os
from dotenv import load_dotenv
from datetime import datetime
import pyspark.sql.functions as F

load_dotenv()

# Minio Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY")
WAREHOUSE_BUCKET = os.getenv("WAREHOUSE_BUCKET", "data-warehouse")
DATAMART_USER=os.getenv("DATAMART_USER", "datamart_user")
DATAMART_PASSWORD=os.getenv("DATAMART_PASSWORD", "datamart_password")
DATAMART_DB=os.getenv("DATAMART_DB", "datamart")

def create_spark_session() -> SparkSession:
    """
    Create a Spark session with the specified configuration.
    """
    jars = "jars/hadoop-aws-3.4.1.jar,jars/bundle-2.32.24.jar,jars/postgresql-42.7.7.jar"
    builder = SparkSession.builder\
        .appName("Green Taxi Mart") \
        .config('spark.memory.fraction', '0.8')\
        .config("spark.executor.memory", "12g")\
        .config("spark.driver.memory", "12g") \
        .config('spark.hadoop.fs.s3a.endpoint', MINIO_ENDPOINT)\
        .config('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY)\
        .config('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY)\
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
    path_read = f"s3a://{WAREHOUSE_BUCKET}/" + "nyc_taxi_dataset/traditional_taxi.parquet"
    df = spark.read.parquet(path_read)
    return df

def sink_data(df: DataFrame):
    df.write.jdbc(
        url="jdbc:postgresql://localhost:5434/{}".format(DATAMART_DB),
        table="public.green_taxi_mart",
        mode="overwrite",
        properties={
            "user": DATAMART_USER,
            "password": DATAMART_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
    )
    return

def main():

    # Datetime
    YEAR, MONTH = 2025, 7

    # Create Spark session
    spark = create_spark_session()
    print("Spark session created successfully.")

    # Ingest data
    df = ingest_data(spark)
    print("Data ingested successfully.")

    # Your data processing logic here
    within_july_condition = \
        (F.year("pickup_datetime") == F.lit(YEAR)) & \
        (F.month("pickup_datetime") == F.lit(MONTH))
    
    valid_trip = \
        F.col("passenger_count").isNotNull() & \
        (F.col("passenger_count") > 0) & \
        (F.col("trip_duration_seconds") > 100) & \
        (F.col("trip_miles") > 0)

    df.printSchema()
    df = df.filter(
        F.col("pickup_datetime").isNotNull() &\
        # within_july_condition & \
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

    df.printSchema()
    df.show(5, truncate=False)

    sink_data(df)

    # Stop the Spark session
    spark.stop()
    print("Spark session stopped.")

if __name__ == "__main__":
    main()