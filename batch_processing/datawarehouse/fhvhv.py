from pyspark.sql import DataFrame
from datawarehouse.yellow_taxi import (
    transform_ts_to_asia_timezone,
    ensure_boolean_type
)
from datawarehouse.common import (
    ingest_data, write_to_warehouse
)
from configs.spark import create_spark_session
from configs.config import settings

def preprocess(df: DataFrame):
    # Rename columns for more clarity
    # Rename column base_passenger_fare to fare_amount for consistency
    df = df.withColumnRenamed(
        'trip_time', 'trip_duration_seconds'
    ).withColumnRenamed(
        'base_passenger_fare', 'fare_amount'
    )

    # Ensure datetime in Asia/Ho_Chi_Minh timezone
    for column_name in [
        'request_datetime', 'on_scene_datetime', 'pickup_datetime', 'dropoff_datetime'
    ]:
        df = df.withColumn(
            column_name,
            transform_ts_to_asia_timezone(column_name)
        )

    # Ensure boolean in 
    for column_name in [
        'shared_request_flag', 'shared_match_flag', 
        'access_a_ride_flag', 'wav_request_flag',
        'wav_match_flag'
    ]:
        df = df.withColumn(
            column_name,
            ensure_boolean_type(column_name, 'Y')
        )

    return df


def main():
    # Create Spark session
    spark = create_spark_session()

    # Ingest data from local files
    df = ingest_data(
        spark, 
        settings.fhvhv_dataset_name,
        settings.ingestion_month,
        settings.ingestion_year
    )

    # Preprocess datas
    df = preprocess(df)

    
    write_to_warehouse(
        df, 
        f"{settings.fhvhv_dataset_name}/{settings.ingestion_month:02d}_{settings.ingestion_year}"
    )

    spark.stop()

    return

if __name__ == "__main__":
    main()