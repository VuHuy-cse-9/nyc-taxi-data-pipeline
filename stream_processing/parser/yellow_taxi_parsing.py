import os
from pyflink.table import EnvironmentSettings, TableEnvironment, Table
from pyflink.table.expressions import col
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JARS_PATH = f"{os.getcwd()}/jars"

# source_ddl = """
# CREATE TABLE raw_yellow_taxi (
#     payload ROW(
#         after ROW(
#             id STRING,
#             vendorid INT,
#             tpep_pickup_datetime STRING,
#             tpep_dropoff_datetime STRING,
#             passenger_count INT,
#             trip_distance FLOAT,
#             ratecodeid INT,
#             store_and_fwd_flag STRING,
#             pulocationid INT,
#             dolocationid INT,
#             payment_type INT,
#             fare_amount FLOAT,
#             extra FLOAT,
#             mta_tax FLOAT,
#             tip_amount FLOAT,
#             tolls_amount FLOAT,
#             improvement_surcharge FLOAT,
#             total_amount FLOAT,
#             congestion_surcharge FLOAT,
#             airport_fee FLOAT,
#             cbd_congestion_fee FLOAT
#         )
#     ),
#     pickup_datetime AS TO_TIMESTAMP(payload.after.tpep_pickup_datetime),
#     dropoff_datetime AS TO_TIMESTAMP(payload.after.tpep_dropoff_datetime),
#     WATERMARK FOR pickup_datetime AS pickup_datetime - INTERVAL '1' SECOND
# ) WITH (
#     'connector' = 'kafka',
#     'topic' = 'raw.public.yellow_taxi',
#     'properties.bootstrap.servers' = 'localhost:9092',
#     'properties.group.id' = 'parser-consumer-2-group',
#     'scan.startup.mode' = 'latest-offset',
#     'format' = 'json',
#     'scan.watermark.idle-timeout'='5second'
# )
# """


source_ddl = """
CREATE TABLE raw_yellow_taxi (
    id STRING,
    vendorid INT,
    tpep_pickup_datetime STRING,
    tpep_dropoff_datetime STRING,
    passenger_count INT,
    trip_distance FLOAT,
    ratecodeid INT,
    store_and_fwd_flag STRING,
    pulocationid INT,
    dolocationid INT,
    payment_type INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    cbd_congestion_fee FLOAT,
    pickup_datetime AS TO_TIMESTAMP(tpep_pickup_datetime),
    dropoff_datetime AS TO_TIMESTAMP(tpep_dropoff_datetime),
    WATERMARK FOR pickup_datetime AS pickup_datetime - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw.default.yellow_taxi',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'parser-consumer-2-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'debezium-json',
    'scan.watermark.idle-timeout'='5second',
    'debezium-json.schema-include' = 'true'
)
"""

def parse_data(t_env: TableEnvironment) -> Table:
    
    # Create table from schema.
    t_env.execute_sql(source_ddl)

    table = t_env.from_path("raw_yellow_taxi")
    
    table.execute().print()

    table = table.select(
        col('payload').get('after').get('id').alias('id'),
        col('payload').get('after').get('vendorid').alias('vendorid'),
        # col('payload').get('after').get('tpep_pickup_datetime').alias('tpep_pickup_datetime'),
        # col('payload').get('after').get('tpep_dropoff_datetime').alias('tpep_dropoff_datetime'),
        col('payload').get('after').get('passenger_count').alias('passenger_count'),
        col('payload').get('after').get('trip_distance').alias('trip_distance'),
        col('payload').get('after').get('ratecodeid').alias('ratecodeid'),
        col('payload').get('after').get('store_and_fwd_flag').alias('store_and_fwd_flag'),
        col('payload').get('after').get('pulocationid').alias('pulocationid'),
        col('payload').get('after').get('dolocationid').alias('dolocationid'),
        col('payload').get('after').get('payment_type').alias('payment_type'),
        col('payload').get('after').get('fare_amount').alias('fare_amount'),
        col('payload').get('after').get('extra').alias('extra'),
        col('payload').get('after').get('mta_tax').alias('mta_tax'),
        col('payload').get('after').get('tip_amount').alias('tip_amount'),
        col('payload').get('after').get('tolls_amount').alias('tolls_amount'),
        col('payload').get('after').get('improvement_surcharge').alias('improvement_surcharge'),
        col('payload').get('after').get('total_amount').alias('total_amount'),
        col('payload').get('after').get('congestion_surcharge').alias('congestion_surcharge'),
        col('payload').get('after').get('cbd_congestion_fee').alias('cbd_congestion_fee'),
        col('payload').get('after').get('airport_fee').alias('airport_fee'),
        col('pickup_datetime'),
        col('dropoff_datetime')
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
    print("Loading sample data...")

    table = parse_data(t_env)

    print("Inserting data into sink table...")
    table.limit(20).execute().print()