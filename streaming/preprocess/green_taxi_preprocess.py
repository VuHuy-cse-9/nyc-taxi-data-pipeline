import os
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.expressions import col, lit
from pyflink.table.udf import udf
from streaming.preprocess.common import transform_ts_to_asia_timezone, ensure_boolean_type, total_seconds_between_timestamps
import pandas as pd
import logging
from schemas.models import TripType, TaxiType, PaymentType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JARS_PATH = f"{os.getcwd()}/jars"

GREEN_TAXI_SCHEMA = DataTypes.ROW([
    DataTypes.FIELD("VendorID", DataTypes.INT()),
    DataTypes.FIELD("lpep_pickup_datetime", DataTypes.STRING()),
    DataTypes.FIELD("lpep_dropoff_datetime", DataTypes.STRING()),
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
    DataTypes.FIELD("total_amount", DataTypes.DOUBLE()),
    DataTypes.FIELD("ehail_fee", DataTypes.DOUBLE()),
    DataTypes.FIELD("trip_type", DataTypes.DOUBLE()),
    DataTypes.FIELD("congestion_surcharge", DataTypes.DOUBLE()),
    DataTypes.FIELD("cbd_congestion_fee", DataTypes.DOUBLE()),
])

@udf(result_type=DataTypes.STRING())
def process_trip_type(value):
    if value == 1.0:
        return TripType.STREET_HAIL.value
    elif value == 2.0:
        return TripType.DISPATCH.value
    else:
        return None
    
@udf(result_type=DataTypes.STRING())
def process_payment_type(value):
    if value == 1:
        return PaymentType.CREDIT_CARD.value
    elif value == 2:
        return PaymentType.CASH.value
    elif value == 3:
        return PaymentType.NO_CHARGE.value
    elif value == 4:
        return PaymentType.DISPUTE.value
    elif value == 5:
        return PaymentType.UNKNOWN.value
    elif value == 6:
        return PaymentType.VOIDED_TRIP.value
    else:
        return None

def preprocess(table: Table):
    return table.add_columns(
        transform_ts_to_asia_timezone(col("lpep_pickup_datetime")).alias("pickup_datetime"),
        transform_ts_to_asia_timezone(col("lpep_dropoff_datetime")).alias("dropoff_datetime"),
        ensure_boolean_type("store_and_fwd_flag", "Y").alias("p_store_and_fwd_flag"),
        process_trip_type(col("trip_type")).alias("p_trip_type"),
        process_payment_type(col("payment_type")).alias("p_payment_type"),
        lit(0.0).alias("airport_fee"),
        lit(TaxiType.GREEN.value).alias("taxi_type"),
    ).drop_columns(
        col("lpep_pickup_datetime"),
        col("lpep_dropoff_datetime"),
        col("ehail_fee"),
        col("trip_type"),
        col("payment_type"),
        col("store_and_fwd_flag")
    ).rename_columns(
        col("trip_distance").alias("trip_miles"),
        col("p_store_and_fwd_flag").alias("store_and_fwd_flag"),
        col("p_trip_type").alias("trip_type"),
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
    df = pd.read_csv("dataset/green_taxi/green_taxi_sample.csv")
    df = df[GREEN_TAXI_SCHEMA.names]

    table = t_env.from_pandas(df, schema=GREEN_TAXI_SCHEMA)
    table = preprocess(table)


    logger.info("Inserting data into sink table...")
    table.limit(20).execute().print()