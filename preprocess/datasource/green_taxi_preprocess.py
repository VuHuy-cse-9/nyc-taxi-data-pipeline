from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from schemas.models import TripType, TaxiType
from preprocess.datasource.common import (
    transform_ts_to_asia_timezone, ensure_boolean_type, process_payment_type)

def process_trip_type(column_name: str):
    return F.when(
        F.col(column_name).isNull(), F.lit(None)
    ).when(
        F.col(column_name) == "1", F.lit(TripType.STREET_HAIL.value)
    ).when(
        F.col(column_name) == "2", F.lit(TripType.DISPATCH.value)
    ).otherwise(
        F.lit(None)
    )

def preprocess(df: DataFrame)->DataFrame:
    print("Preprocessing green taxi data...")
    # Convert from New york zone to Asia/Ho_Chi_Minh timezone
    df = df.withColumn(
        'lpep_pickup_datetime', 
        transform_ts_to_asia_timezone('lpep_pickup_datetime')
    ).withColumn(
        'lpep_dropoff_datetime', 
        transform_ts_to_asia_timezone('lpep_dropoff_datetime')
    ).withColumnRenamed(
        'lpep_pickup_datetime', 'pickup_datetime'
    ).withColumnRenamed(
        'lpep_dropoff_datetime', 'dropoff_datetime'
    )

    # Convert store_and_fwd_flag to boolean type
    df = df.withColumn(
        'store_and_fwd_flag', 
        ensure_boolean_type('store_and_fwd_flag', 'Y')
    )

    # Drop null column
    df = df.drop('ehail_fee')

    # Process trip_type column
    df = df.withColumn(
        'trip_type',
        process_trip_type('trip_type')
    )

    # Add columns to merge with yellow taxi data
    df = df.withColumn(
        'airport_fee',
        F.lit(0.0)
    )

    # Taxi type
    df = df.withColumn(
        'taxi_type',
        F.lit(TaxiType.GREEN.value)
    )

    # Rename trip_distance to trip_miles for clarity
    df = df.withColumnRenamed(
        'trip_distance', 'trip_miles'
    )

    # Compute feature trip_duration
    df = df.withColumn(
        'trip_duration_seconds',
        F.when(
            F.col('dropoff_datetime').isNull() | F.col('pickup_datetime').isNull(),
            F.lit(None)
        ).otherwise(
            (F.unix_timestamp('dropoff_datetime') - F.unix_timestamp('pickup_datetime')) * 60
        )
    )

    # Process payment_type column
    df = df.withColumn(
        'payment_type',
        process_payment_type('payment_type')
    )

    return df