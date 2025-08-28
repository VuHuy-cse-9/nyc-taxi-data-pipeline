from pyflink.table import TableEnvironment, EnvironmentSettings, Table
from pyflink.table.window import Tumble, Slide
from pyflink.table.expressions import col, lit, date_format
import os
from schemas.stream.tables import (
    STREAM_PREPROCESSED_TRADITIONAL_TAXI_TABLE_WITH_KAFKA_CONNECTOR,
    STREAM_PREPROCESSED_FHVHV_TABLE_WITH_KAFKA_CONNECTOR
)
import pandas as pd

JARS_PATH = f"{os.getcwd()}/jars"

def compute_recent_activity_intensity(table: Table):
    return

def main():
    print("Setting up Flink environment...")
    # Set up the environment settings for streaming mode
    t_env = TableEnvironment.create(
        environment_settings=EnvironmentSettings.in_streaming_mode())
    
    # Setup configuration
    t_env.get_config().set_local_timezone("UTC")
    t_env.get_config().set('table.display.max-column-width', '60')
    t_env.get_config().set(
        "pipeline.jars",
        f"file://{JARS_PATH}/flink-connector-kafka-4.0.0-2.0.jar;"
        + f"file://{JARS_PATH}/kafka-clients-3.9.0.jar;"
        + f"file://{JARS_PATH}/flink-table-api-java-2.1.0.jar"
    )

    # Ingesting stream
    t_env.execute_sql(
        STREAM_PREPROCESSED_TRADITIONAL_TAXI_TABLE_WITH_KAFKA_CONNECTOR.format(
            'preprocessed_traditional_taxi', 
            'preprocess.public.traditional_taxi', 
            'localhost:9092'
        )
    )

    # t_env.execute_sql(
    #     STREAM_PREPROCESSED_FHVHV_TABLE_WITH_KAFKA_CONNECTOR.format(
    #         'preprocessed_fhvhv', 
    #         'preprocess.public.for_hire_vehicle', 
    #         'localhost:9092'
    #     )
    # )


    traditional_taxi_table = t_env.from_path("preprocessed_traditional_taxi")
    # fhvhv_table = t_env.from_path("preprocessed_fhvhv")

    traditional_taxi_table.print_schema()

    print("Performing sliding window aggregation on traditional taxi data...")
    sliding_w = traditional_taxi_table.window(
        Tumble.over(size=lit(5).seconds).on(col("pickup_datetime")).alias("w"))\
        .group_by(col("w"), col('vendorid'))\
        .select(
            # col("vendorid"),
            date_format(col("w").start, "HH:mm:ss").alias("window_start"),
            date_format(col("w").end, "HH:mm:ss").alias("window_end"),
            col("id").count.alias("total_trips"),
            date_format(col('pickup_datetime'), "HH:mm:ss").array_agg.alias("pickup_datetime_array")
        )
    
    sliding_w.print_schema()
    
    sliding_w.execute().print()


if __name__ == "__main__":
    main()