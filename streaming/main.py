from pyflink.table import EnvironmentSettings, TableEnvironment, Table
from streaming.parser import (
    fhvhv_parse_data, green_taxi_parse_data, yellow_taxi_parse_data
)
from streaming.preprocess import (
    fhvhv_preprocess, green_taxi_preprocess, yellow_taxi_preprocess
)
from pyflink.table.expressions import col, lit, to_timestamp
from streaming.preprocess.common import (
    transform_ts_to_asia_timezone, ensure_boolean_type, total_seconds_between_timestamps,
    process_trip_type, process_payment_type, foo
)
import os
from schemas.models import TaxiType

JARS_PATH = f"{os.getcwd()}/jars"

def main():
    # Set up the environment settings for streaming mode
    print("Setting up Flink environment...")
    t_env = TableEnvironment.create(
        environment_settings=EnvironmentSettings.in_streaming_mode())
    
    # Setup configuration
    t_env.get_config().set("table.local-time-zone", "America/New_York")
    t_env.get_config().set(
        "pipeline.jars",
        f"file://{JARS_PATH}/flink-connector-kafka-4.0.0-2.0.jar;"
        + f"file://{JARS_PATH}/kafka-clients-3.9.0.jar"
    )

    # Step 1: Parse data from source
    print("Parsing data from source...")
    # fhvhv_table = fhvhv_parse_data(t_env)
    green_taxi_table = green_taxi_parse_data(t_env)
    # yellow_taxi_table = yellow_taxi_parse_data(t_env)


    # Step 2: Preprocess data
    green_taxi_table.print_schema()

    green_taxi_table = green_taxi_table.add_columns(
        # transform_ts_to_asia_timezone(col("lpep_pickup_datetime")).alias("pickup_datetime"),
        # transform_ts_to_asia_timezone(col("lpep_dropoff_datetime")).alias("dropoff_datetime"),
        foo(col("payment_type")).alias("p_payment_type"),
        # lit(0.0).alias("airport_fee"),
        # lit(TaxiType.GREEN.value).alias("taxi_type"),
    )

    # fhvhv_table = fhvhv_preprocess(fhvhv_table)
    # green_taxi_table = green_taxi_preprocess(green_taxi_table)
    # yellow_taxi_table = yellow_taxi_preprocess(yellow_taxi_table)

    # fhvhv_table.limit(100).execute().print()
    green_taxi_table.execute().print()

    return

if __name__ == "__main__":
    main()