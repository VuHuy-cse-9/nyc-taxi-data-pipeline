import os
from pyflink.table import EnvironmentSettings, TableEnvironment, Table
from pyflink.table.expressions import col, json
import logging
from pyflink.table.types import DataTypes

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
    ),
    pickup_datetime AS TO_TIMESTAMP(payload.after.pickup_datetime),
    dropoff_datetime AS TO_TIMESTAMP(payload.after.dropoff_datetime),
    request_datetime AS TO_TIMESTAMP(payload.after.request_datetime),
    on_scene_datetime AS TO_TIMESTAMP(payload.after.on_scene_datetime),
    WATERMARK FOR pickup_datetime AS pickup_datetime - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw.public.for_hire_vehicle',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'parser-consumer-2-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'scan.watermark.idle-timeout'='5second'
)
"""

source_ddl = """
CREATE TABLE raw_for_hire_vehicle (
    payload ROW(
        after STRING
    ),
    data AS payload.after,
    pickup_datetime AS TO_TIMESTAMP(JSON_VALUE(payload.after, '$.pickup_datetime' RETURNING STRING), 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
    dropoff_datetime AS TO_TIMESTAMP(JSON_VALUE(payload.after, '$.dropoff_datetime' RETURNING STRING), 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
    request_datetime AS TO_TIMESTAMP(JSON_VALUE(payload.after, '$.request_datetime' RETURNING STRING), 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
    on_scene_datetime AS TO_TIMESTAMP(JSON_VALUE(payload.after, '$.on_scene_datetime' RETURNING STRING), 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
    WATERMARK FOR pickup_datetime AS pickup_datetime - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw.datasource3.fhvhv_taxi',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'parser-consumer-2-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'scan.watermark.idle-timeout'='5second'
)
"""

def parse_data(t_env: TableEnvironment) -> Table:
    
    # Create table from schema.
    t_env.execute_sql(source_ddl)

    table = t_env.from_path("raw_for_hire_vehicle")
    table = table.select(
        col('data').alias('payload'),
        col('pickup_datetime'),
        col('dropoff_datetime'),
        col('request_datetime'),
        col('on_scene_datetime')
    )

    table = table.select(
        col('payload').json_value('$._id["$oid"]', DataTypes.STRING()).alias('id'),
        col('payload').json_value("$.hvfhs_license_num", DataTypes.STRING()).alias('hvfhs_license_num'),
        col('payload').json_value("$.dispatching_base_num", DataTypes.STRING()).alias('dispatching_base_num'),
        col('payload').json_value("$.originating_base_num", DataTypes.STRING()).alias('originating_base_num'),
        col('payload').json_value("$.PULocationID", DataTypes.INT()).alias('pulocationid'),
        col('payload').json_value("$.DOLocationID", DataTypes.INT()).alias('dolocationid'),
        col('payload').json_value("$.trip_miles", DataTypes.DOUBLE()).alias('trip_miles'),
        col('payload').json_value("$.trip_time", DataTypes.INT()).alias('trip_time'),
        col('payload').json_value("$.base_passenger_fare", DataTypes.DOUBLE()).alias('base_passenger_fare'),
        col('payload').json_value("$.tolls", DataTypes.DOUBLE()).alias('tolls'),
        col('payload').json_value("$.bcf", DataTypes.DOUBLE()).alias('bcf'),
        col('payload').json_value("$.sales_tax", DataTypes.DOUBLE()).alias('sales_tax'),
        col('payload').json_value("$.congestion_surcharge", DataTypes.DOUBLE()).alias('congestion_surcharge'),
        col('payload').json_value("$.airport_fee", DataTypes.DOUBLE()).alias('airport_fee'),
        col('payload').json_value("$.tips", DataTypes.DOUBLE()).alias('tips'),
        col('payload').json_value("$.driver_pay", DataTypes.DOUBLE()).alias('driver_pay'),
        col('payload').json_value("$.shared_request_flag", DataTypes.STRING()).alias('shared_request_flag'),
        col('payload').json_value("$.shared_match_flag", DataTypes.STRING()).alias('shared_match_flag'),
        col('payload').json_value("$.access_a_ride_flag", DataTypes.STRING()).alias('access_a_ride_flag'),
        col('payload').json_value("$.wav_request_flag", DataTypes.STRING()).alias('wav_request_flag'),
        col('payload').json_value("$.wav_match_flag", DataTypes.STRING()).alias('wav_match_flag'),
        col('payload').json_value("$.cbd_congestion_fee", DataTypes.DOUBLE()).alias('cbd_congestion_fee'),
        col('pickup_datetime'),
        col('dropoff_datetime'),
        col('request_datetime'),
        col('on_scene_datetime')
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