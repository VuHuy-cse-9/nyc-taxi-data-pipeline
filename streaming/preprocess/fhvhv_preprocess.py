import os
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.expressions import col
from streaming.preprocess.common import (
    transform_ts_to_asia_timezone, ensure_boolean_type,
)
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JARS_PATH = f"{os.getcwd()}/jars"

FHVHV_TAXI_SCHEMA = DataTypes.ROW([
    DataTypes.FIELD("hvfhs_license_num", DataTypes.STRING()),
    DataTypes.FIELD("dispatching_base_num", DataTypes.STRING()),
    DataTypes.FIELD("originating_base_num", DataTypes.STRING()),
    DataTypes.FIELD("request_datetime", DataTypes.STRING()),
    DataTypes.FIELD("on_scene_datetime", DataTypes.STRING()),
    DataTypes.FIELD("pickup_datetime",DataTypes.STRING()),
    DataTypes.FIELD("dropoff_datetime", DataTypes.STRING()),
    DataTypes.FIELD("PULocationID", DataTypes.INT()),
    DataTypes.FIELD("DOLocationID", DataTypes.INT()),
    DataTypes.FIELD("trip_miles", DataTypes.DOUBLE()),
    DataTypes.FIELD("trip_time", DataTypes.DOUBLE()),
    DataTypes.FIELD("base_passenger_fare", DataTypes.DOUBLE()),
    DataTypes.FIELD("tolls", DataTypes.DOUBLE()),
    DataTypes.FIELD("bcf", DataTypes.DOUBLE()),
    DataTypes.FIELD("sales_tax", DataTypes.DOUBLE()),
    DataTypes.FIELD("congestion_surcharge", DataTypes.DOUBLE()),
    DataTypes.FIELD("airport_fee", DataTypes.DOUBLE()),
    DataTypes.FIELD("tips", DataTypes.DOUBLE()),
    DataTypes.FIELD("driver_pay", DataTypes.DOUBLE()),
    DataTypes.FIELD("shared_request_flag", DataTypes.STRING()),
    DataTypes.FIELD("shared_match_flag", DataTypes.STRING()),
    DataTypes.FIELD("access_a_ride_flag", DataTypes.STRING()),
    DataTypes.FIELD("wav_request_flag", DataTypes.STRING()),
    DataTypes.FIELD("wav_match_flag", DataTypes.STRING()),
    DataTypes.FIELD("cbd_congestion_fee", DataTypes.DOUBLE()),
])

def preprocess(table: Table):
    datetime_format = "%Y-%m-%dT%H:%M:%S.%f"
    return table.add_columns(
        transform_ts_to_asia_timezone(col("request_datetime"), datetime_format).alias("p_request_datetime"),
        transform_ts_to_asia_timezone(col("on_scene_datetime"), datetime_format).alias("p_on_scene_datetime"),
        transform_ts_to_asia_timezone(col("pickup_datetime"), datetime_format).alias("p_pickup_datetime"),
        transform_ts_to_asia_timezone(col("dropoff_datetime"), datetime_format).alias("p_dropoff_datetime"),
        ensure_boolean_type("shared_request_flag", "Y").alias("p_shared_request_flag"),
        ensure_boolean_type("shared_match_flag", "Y").alias("p_shared_match_flag"),
        ensure_boolean_type("access_a_ride_flag", "Y").alias("p_access_a_ride_flag"),
        ensure_boolean_type("wav_request_flag", "Y").alias("p_wav_request_flag"),
        ensure_boolean_type("wav_match_flag", "Y").alias("p_wav_match_flag"),
    ).drop_columns(
        col("request_datetime"),
        col("on_scene_datetime"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("shared_request_flag"),
        col("shared_match_flag"),
        col("access_a_ride_flag"),
        col("wav_request_flag"),
        col("wav_match_flag"),
    ).rename_columns(
        col("trip_time").alias("trip_duration_seconds"),
        col("base_passenger_fare").alias("fare_amount"),
        col("p_request_datetime").alias("request_datetime"),
        col("p_on_scene_datetime").alias("on_scene_datetime"),
        col("p_pickup_datetime").alias("pickup_datetime"),
        col("p_dropoff_datetime").alias("dropoff_datetime"),
        col("p_shared_request_flag").alias("shared_request_flag"),
        col("p_shared_match_flag").alias("shared_match_flag"),
        col("p_access_a_ride_flag").alias("access_a_ride_flag"),
        col("p_wav_request_flag").alias("wav_request_flag"),
        col("p_wav_match_flag").alias("wav_match_flag"),
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
    df = pd.read_csv(
        "dataset/samples/csv/fhvhv.csv")
    df = df[FHVHV_TAXI_SCHEMA.names]

    table = t_env.from_pandas(df, schema=FHVHV_TAXI_SCHEMA)
    table = preprocess(table)


    logger.info("Inserting data into sink table...")
    table.limit(20).execute().print()