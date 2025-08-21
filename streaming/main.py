from pyflink.table import EnvironmentSettings, TableEnvironment
from streaming.parser import (
    fhvhv_parse_data, green_taxi_parse_data, yellow_taxi_parse_data
)
from streaming.preprocess import (
    fhvhv_preprocess, green_taxi_preprocess, yellow_taxi_preprocess
)
import os


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
    fhvhv_table = fhvhv_parse_data(t_env)
    green_taxi_table = green_taxi_parse_data(t_env)
    yellow_taxi_table = yellow_taxi_parse_data(t_env)

    fhvhv_table = fhvhv_preprocess(fhvhv_table)
    green_taxi_table = green_taxi_preprocess(green_taxi_table)
    yellow_taxi_table = yellow_taxi_preprocess(yellow_taxi_table)

    

    # green_taxi_table.limit(100).execute().print()
    # yellow_taxi_table.limit(100).execute().print()
    fhvhv_table.limit(100).execute().print()
    # fhvhv_table.execute_insert("parsed_for_hire_vehicle").wait()

    return

if __name__ == "__main__":
    main()