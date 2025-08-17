from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from preprocess.datasource.yellow_taxi_preprocess import (
    transform_ts_to_asia_timezone,
    ensure_boolean_type
)

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