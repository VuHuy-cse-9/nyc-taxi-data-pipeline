import os
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table, TableDescriptor, Schema
from pyflink.table.expressions import col, to_timestamp
from streaming.preprocess.common import (
    ensure_boolean_type,
)
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JARS_PATH = f"{os.getcwd()}/jars"

def preprocess(table: Table):
    datetime_format = "%Y-%m-%dT%H:%M:%S.%f"
    return table.add_columns(
        to_timestamp(col("request_datetime"), datetime_format).alias("p_request_datetime"),
        to_timestamp(col("on_scene_datetime"), datetime_format).alias("p_on_scene_datetime"),
        to_timestamp(col("pickup_datetime"), datetime_format).alias("p_pickup_datetime"),
        to_timestamp(col("dropoff_datetime"), datetime_format).alias("p_dropoff_datetime"),
        ensure_boolean_type(col("shared_request_flag"), "Y").alias("p_shared_request_flag"),
        ensure_boolean_type(col("shared_match_flag"), "Y").alias("p_shared_match_flag"),
        ensure_boolean_type(col("access_a_ride_flag"), "Y").alias("p_access_a_ride_flag"),
        ensure_boolean_type(col("wav_request_flag"), "Y").alias("p_wav_request_flag"),
        ensure_boolean_type(col("wav_match_flag"), "Y").alias("p_wav_match_flag"),
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
    t_env.get_config().set(
        "pipeline.jars",
        f"file://{JARS_PATH}/flink-connector-kafka-4.0.0-2.0.jar;"
        + f"file://{JARS_PATH}/kafka-clients-3.9.0.jar"
    )

    t_env.create_temporary_table(
        "raw_for_hire_vehicle",
        TableDescriptor.for_connector("kafka")
        .option("topic", "parsed.public.for_hire_vehicle")
        .option("properties.bootstrap.servers", value="localhost:9092")
        .option("properties.group.id", "parser-consumer-1-group")
        .option("scan.startup.mode", "latest-offset")
        .format("json")
        .schema(Schema.new_builder().from_row_data_type(
            FHVHV_TAXI_SCHEMA
        ).build())
        .build()
    )

    table = t_env.from_path("raw_for_hire_vehicle")

    # Load sample data from parquet file
    logger.info("Loading sample data...")
    # df = pd.read_csv(
    #     "dataset/samples/csv/fhvhv.csv")
    # df = df[FHVHV_TAXI_SCHEMA.names]

    # table = t_env.from_pandas(df, schema=FHVHV_TAXI_SCHEMA)
    table = preprocess(table)


    logger.info("Inserting data into sink table...")
    table.limit(20).execute().print()