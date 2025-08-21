from pyflink.table import EnvironmentSettings, TableEnvironment, StatementSet
from streaming.parser import (
    fhvhv_parse_data, green_taxi_parse_data, yellow_taxi_parse_data
)
from streaming.preprocess import (
    fhvhv_preprocess, green_taxi_preprocess, yellow_taxi_preprocess
)
import os
from schemas.stream.tables import (
    PREPROCESSED_GREEN_TAXI_TABLE_WITH_KAFKA_CONNECTOR,
    PREPROCESSED_YELLOW_TAXI_TABLE_WITH_KAFKA_CONNECTOR,
    PREPROCESSED_FHVHV_TABLE_WITH_KAFKA_CONNECTOR
)


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

    green_taxi_table.execute()
    yellow_taxi_table.execute()
    fhvhv_table.execute()


    print("Executing FHVHV table creation")

    t_env.execute_sql(
        PREPROCESSED_GREEN_TAXI_TABLE_WITH_KAFKA_CONNECTOR.format(
            'preprocessed_green_taxi', 'preprocess.public.green_taxi', 'localhost:9092'
        )
    )

    t_env.execute_sql(
        PREPROCESSED_YELLOW_TAXI_TABLE_WITH_KAFKA_CONNECTOR.format(
            'preprocessed_yellow_taxi', 'preprocess.public.yellow_taxi', 'localhost:9092'
        )
    )

    t_env.execute_sql(
        PREPROCESSED_FHVHV_TABLE_WITH_KAFKA_CONNECTOR.format(
            'preprocessed_fhvhv', 'preprocess.public.for_hire_vehicle', 'localhost:9092'
        )
    )

    statement_set: StatementSet = t_env.create_statement_set()

    print("Inserting data into sink tables...")
    statement_set.add_insert("preprocessed_green_taxi", green_taxi_table)
    statement_set.add_insert("preprocessed_yellow_taxi", yellow_taxi_table)
    statement_set.add_insert("preprocessed_fhvhv", fhvhv_table)
    statement_set.execute().wait()

    # green_taxi_table.execute_insert("preprocessed_green_taxi").wait()
    # yellow_taxi_table.execute_insert("preprocessed_yellow_taxi").wait()
    # fhvhv_table.execute_insert("preprocessed_fhvhv").wait()


    return

if __name__ == "__main__":
    main()