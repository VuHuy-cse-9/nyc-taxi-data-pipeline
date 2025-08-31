import os
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.expressions import col, lit, TimePointUnit, to_timestamp, Expression, if_then_else, timestamp_diff, coalesce
from pyflink.table.udf import udf
from stream_processing.preprocess.common import (
    ensure_boolean_type, process_payment_type
)
import pandas as pd
import logging
from schemas.models import TaxiType, TripType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JARS_PATH = f"{os.getcwd()}/jars"

YELLOW_TAXI_SCHEMA = DataTypes.ROW([
    DataTypes.FIELD("VendorID", DataTypes.INT()),
    DataTypes.FIELD("tpep_pickup_datetime", DataTypes.STRING()),
    DataTypes.FIELD("tpep_dropoff_datetime", DataTypes.STRING()),
    DataTypes.FIELD("passenger_count", DataTypes.INT()),
    DataTypes.FIELD("trip_distance", DataTypes.DOUBLE()),
    DataTypes.FIELD("RatecodeID", DataTypes.INT()),
    DataTypes.FIELD("store_and_fwd_flag", DataTypes.CHAR(length=1)),
    DataTypes.FIELD("PULocationID", DataTypes.INT()),
    DataTypes.FIELD("DOLocationID", DataTypes.INT()),
    DataTypes.FIELD("payment_type", DataTypes.INT()),
    DataTypes.FIELD("fare_amount", DataTypes.DOUBLE()),
    DataTypes.FIELD("extra", DataTypes.DOUBLE()),
    DataTypes.FIELD("mta_tax", DataTypes.DOUBLE()),
    DataTypes.FIELD("tip_amount", DataTypes.DOUBLE()),
    DataTypes.FIELD("tolls_amount", DataTypes.DOUBLE()),
    DataTypes.FIELD("improvement_surcharge", DataTypes.DOUBLE()),
    DataTypes.FIELD("congestion_surcharge", DataTypes.DOUBLE()),
    DataTypes.FIELD("cbd_congestion_fee", DataTypes.DOUBLE()),
    DataTypes.FIELD("airport_fee", DataTypes.DOUBLE()),
])

def process_airport_fee(column: Expression):
    return if_then_else(column.is_null, lit(0.0), column)

def estimate_total_amount(fare_amount: Expression, 
                          extra: Expression, mta_tax: Expression, tip_amount: Expression, 
                          tolls_amount: Expression, improvement_surcharge: Expression, airport_fee: Expression):
    total_amount = lit(0.0)
    total_amount += if_then_else(fare_amount.is_null, 0.0, fare_amount)
    total_amount += if_then_else(extra.is_null, 0.0, extra)
    total_amount += if_then_else(mta_tax.is_null, 0.0, mta_tax)
    total_amount += if_then_else(tip_amount.is_null, 0.0, tip_amount)
    total_amount += if_then_else(tolls_amount.is_null, 0.0, tolls_amount)
    total_amount += if_then_else(improvement_surcharge.is_null, 0.0, improvement_surcharge)
    total_amount += if_then_else(airport_fee.is_null, 0.0, airport_fee)
    return total_amount

def preprocess(table: Table):
    return table.add_columns(
        ensure_boolean_type(col("store_and_fwd_flag"), "Y").alias("p_store_and_fwd_flag"),
        process_payment_type(col("payment_type")).alias("p_payment_type"),
        process_airport_fee(col("airport_fee")).alias("p_airport_fee"),
        lit(TaxiType.YELLOW.value).alias("taxi_type"),
        lit(TripType.STREET_HAIL.value).alias("trip_type"),
    ).drop_columns(
        col("payment_type"),
        col("store_and_fwd_flag"),
        col("airport_fee")
    ).rename_columns(
        col("trip_distance").alias("trip_miles"),
        col("p_store_and_fwd_flag").alias(name="store_and_fwd_flag"),
        col("p_payment_type").alias("payment_type"),
        col("p_airport_fee").alias("airport_fee"),
    ).add_columns(
        estimate_total_amount(
            col("fare_amount"),
            col("extra"),
            col("mta_tax"),
            col("tip_amount"),
            col("tolls_amount"),
            col("improvement_surcharge"),
            col("airport_fee")
        ).alias("total_amount_extra"),
        timestamp_diff(TimePointUnit.SECOND, 
                       col("pickup_datetime"), 
                       col("dropoff_datetime")).alias("trip_duration"),
    ).add_or_replace_columns(
        coalesce(col('total_amount'), col('total_amount_extra')).alias('total_amount')
    ).drop_columns(
        col('total_amount_extra')
    )

if __name__ == "__main__":
    # Environment configuration
    logger.info("Setting up Flink environment...")
    t_env = TableEnvironment.create(
        environment_settings=EnvironmentSettings.in_streaming_mode()
    )
    t_env.get_config().set("table.local-time-zone", "America/New_York")

    # Load sample data from parquet file
    logger.info("Loading sample data...")
    df = pd.read_csv("dataset/yellow_tripdata_2025-05.csv")
    df = df[YELLOW_TAXI_SCHEMA.names]

    table = t_env.from_pandas(df, schema=YELLOW_TAXI_SCHEMA)
    table = preprocess(table)


    logger.info("Inserting data into sink table...")
    table.limit(20).execute().print()