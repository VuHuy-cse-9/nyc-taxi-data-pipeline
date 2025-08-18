import os
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.expressions import col, lit
from pyflink.table.udf import udf
from streaming.preprocess.common import (
    transform_ts_to_asia_timezone, ensure_boolean_type, total_seconds_between_timestamps,
    process_trip_type, process_payment_type
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
    DataTypes.FIELD("Airport_fee", DataTypes.DOUBLE()),
])

@udf(result_type=DataTypes.DOUBLE())
def process_airport_fee(value):
    if value is None:
        return 0.0
    return value

@udf(result_type=DataTypes.DOUBLE())
def estimate_total_amount(fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, airport_fee):
    total_amount = 0.0
    total_amount += fare_amount if fare_amount is not None else 0.0
    total_amount += extra if extra is not None else 0.0
    total_amount += mta_tax if mta_tax is not None else 0.0
    total_amount += tip_amount if tip_amount is not None else 0.0
    total_amount += tolls_amount if tolls_amount is not None else 0.0
    total_amount += improvement_surcharge if improvement_surcharge is not None else 0.0
    total_amount += airport_fee if airport_fee is not None else 0.0
    return total_amount

def preprocess(table: Table):
    return table.add_columns(
        transform_ts_to_asia_timezone(col("tpep_pickup_datetime")).alias("pickup_datetime"),
        transform_ts_to_asia_timezone(col("tpep_dropoff_datetime")).alias("dropoff_datetime"),
        ensure_boolean_type("store_and_fwd_flag", "Y").alias("p_store_and_fwd_flag"),
        process_payment_type(col("payment_type")).alias("p_payment_type"),
        process_airport_fee(col("Airport_fee")).alias("airport_fee"),
        estimate_total_amount(
            col("fare_amount"),
            col("extra"),
            col("mta_tax"),
            col("tip_amount"),
            col("tolls_amount"),
            col("improvement_surcharge"),
            col("Airport_fee")
        ).alias("total_amount"),
        lit(TaxiType.YELLOW.value).alias("taxi_type"),
        lit(TripType.STREET_HAIL.value).alias("trip_type"),
    ).drop_columns(
        col("tpep_pickup_datetime"),
        col("tpep_dropoff_datetime"),
        col("payment_type"),
        col("store_and_fwd_flag"),
        col("Airport_fee")
    ).rename_columns(
        col("trip_distance").alias("trip_miles"),
        col("p_store_and_fwd_flag").alias(name="store_and_fwd_flag"),
        col("p_payment_type").alias("payment_type"),
    ).add_columns(
        total_seconds_between_timestamps(col("dropoff_datetime"), 
                                         col("pickup_datetime")).alias("trip_duration"),
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