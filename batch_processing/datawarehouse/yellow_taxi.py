from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from schemas.models import TripType, TaxiType
from datawarehouse.common import (
    transform_ts_to_asia_timezone, ensure_boolean_type, process_payment_type,
    ingest_data, write_to_warehouse
)
from configs.spark import create_spark_session
from configs.config import settings

def sum_columns(col_names: list[str])->F.Column:
    sum_col = F.lit(0.0)
    for col_name in col_names:
        sum_col = F.when(
            F.col(col_name).isNull(), sum_col,
        ).otherwise(
            sum_col + F.col(col_name)
        )
    return sum_col

def preprocess(df: DataFrame)->DataFrame:
    print("Preprocessing yellow taxi data...")

    # Convert from New york zone to Asia/Ho_Chi_Minh timezone
    for column in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
        if column not in df.columns:
            raise ValueError(f"Column {column} does not exist in the DataFrame.")
        
        df = df.withColumn(
            column,
            transform_ts_to_asia_timezone(column)
        )

    df = df.withColumnRenamed(
        'tpep_pickup_datetime', 'pickup_datetime'
    ).withColumnRenamed(
        'tpep_dropoff_datetime', 'dropoff_datetime'
    )

    # Ensure boolean in store_and_fwd_flag
    df = df.withColumn(
        'store_and_fwd_flag',
        ensure_boolean_type('store_and_fwd_flag', 'Y')
    )

    # TODO: There are some fee columns that contains negative values,
    # we need to handle them later.

    # Add Trip type to merge with green taxi data
    df = df.withColumn(
        'trip_type',
        F.lit(TripType.STREET_HAIL.value)
    )

    # Fill null airport_fee with 0.0
    df = df.withColumn(
        'airport_fee',
        F.when(F.col('airport_fee').isNull(), F.lit(0.0))
            .otherwise(F.col('airport_fee'))
    )

    # Total amount
    passenger_charged_cols = [
        'fare_amount',
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'improvement_surcharge',
        'airport_fee'
    ]

    df = df.withColumn(
        'total_amount',
        sum_columns(passenger_charged_cols)
    )

    # Add taxi_type column to distinguish yellow taxi
    df = df.withColumn(
        'taxi_type',
        F.lit(TaxiType.YELLOW.value)
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


def main():
    # Create Spark session
    spark = create_spark_session()

    # Ingest data from local files
    df = ingest_data(
        spark, 
        settings.yellow_taxi_dataset_name,
        settings.ingestion_month,
        settings.ingestion_year
    )

    # Preprocess datas
    df = preprocess(df)

    write_to_warehouse(
        df, 
        f"{settings.yellow_taxi_dataset_name}/{settings.ingestion_month:02d}_{settings.ingestion_year}"
    )

    spark.stop()

    return

if __name__ == "__main__":
    main()