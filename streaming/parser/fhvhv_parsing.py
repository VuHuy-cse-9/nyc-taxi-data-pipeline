import os
from pyflink.table import EnvironmentSettings, TableEnvironment, Table
from pyflink.table.expressions import col
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JARS_PATH = f"{os.getcwd()}/jars"

source_ddl = """
CREATE TABLE raw_for_hire_vehicle (
    payload ROW(
        after ROW(
            id STRING,
            hvfhs_license_num STRING,
            dispatching_base_num STRING,
            originating_base_num STRING,
            request_datetime STRING,
            on_scene_datetime STRING,
            pickup_datetime STRING,
            dropoff_datetime STRING,
            pulocationid INT,
            dolocationid INT,
            trip_miles FLOAT,
            trip_time INT,
            base_passenger_fare FLOAT,
            tolls FLOAT,
            bcf FLOAT,
            sales_tax FLOAT,
            congestion_surcharge FLOAT,
            airport_fee FLOAT,
            tips FLOAT,
            driver_pay FLOAT,
            shared_request_flag STRING,
            shared_match_flag STRING,
            access_a_ride_flag STRING,
            wav_request_flag STRING,
            wav_match_flag STRING,
            cbd_congestion_fee FLOAT
        )
    )
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw.public.for_hire_vehicle',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'parser-consumer-2-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
)
"""

def parse_data(t_env: TableEnvironment) -> Table:
    
    # Create table from schema.
    t_env.execute_sql(source_ddl)

    table = t_env.from_path("raw_for_hire_vehicle")

    table = table.select(
        col('payload').get('after').get('id').alias('id'),
        col('payload').get('after').get('hvfhs_license_num').alias('hvfhs_license_num'),
        col('payload').get('after').get('dispatching_base_num').alias('dispatching_base_num'),
        col('payload').get('after').get('originating_base_num').alias('originating_base_num'),
        col('payload').get('after').get('request_datetime').alias('request_datetime'),
        col('payload').get('after').get('on_scene_datetime').alias('on_scene_datetime'),
        col('payload').get('after').get('pickup_datetime').alias('pickup_datetime'),
        col('payload').get('after').get('dropoff_datetime').alias('dropoff_datetime'),
        col('payload').get('after').get('pulocationid').alias('pulocationid'),
        col('payload').get('after').get('dolocationid').alias('dolocationid'),
        col('payload').get('after').get('trip_miles').alias('trip_miles'),
        col('payload').get('after').get('trip_time').alias('trip_time'),
        col('payload').get('after').get('base_passenger_fare').alias('base_passenger_fare'),
        col('payload').get('after').get('tolls').alias('tolls'),
        col('payload').get('after').get('bcf').alias('bcf'),
        col('payload').get('after').get('sales_tax').alias('sales_tax'),
        col('payload').get('after').get('congestion_surcharge').alias('congestion_surcharge'),
        col('payload').get('after').get('airport_fee').alias('airport_fee'),
        col('payload').get('after').get('tips').alias('tips'),
        col('payload').get('after').get('driver_pay').alias('driver_pay'),
        col('payload').get('after').get('shared_request_flag').alias('shared_request_flag'),
        col('payload').get('after').get('shared_match_flag').alias('shared_match_flag'),
        col('payload').get('after').get('access_a_ride_flag').alias('access_a_ride_flag'),
        col('payload').get('after').get('wav_request_flag').alias('wav_request_flag'),
        col('payload').get('after').get('wav_match_flag').alias('wav_match_flag'),
        col('payload').get('after').get('cbd_congestion_fee').alias('cbd_congestion_fee')
    )
    return table

if __name__ == "__main__":
    # Environment configuration
    logger.info("Setting up Flink environment...")
    t_env = TableEnvironment.create(
        environment_settings=EnvironmentSettings.in_streaming_mode()
    )
    t_env.get_config().set("table.local-time-zone", "Asia/Ho_Chi_Minh")
    t_env.get_config().set(
        "pipeline.jars",
        f"file://{JARS_PATH}/flink-connector-kafka-4.0.0-2.0.jar;"
        + f"file://{JARS_PATH}/kafka-clients-3.9.0.jar"
    )

    # Load sample data from parquet file
    logger.info("Loading sample data...")

    table = parse_data(t_env)

    logger.info("Inserting data into sink table...")
    table.limit(1000).execute().print()