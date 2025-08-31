import os
from pyflink.table import EnvironmentSettings, TableEnvironment, Table
from pyflink.table.expressions import col
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JARS_PATH = f"{os.getcwd()}/jars"

# source_ddl = """
# CREATE TABLE raw_green_taxi (
#     payload ROW(
#         after ROW(
#             id STRING,
#             vendorid INT,
#             lpep_pickup_datetime STRING,
#             lpep_dropoff_datetime STRING,
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
#             ehail_fee FLOAT,
#             trip_type INT,
#             congestion_surcharge FLOAT,
#             cbd_congestion_fee FLOAT
#         )
#     )
# ) WITH (
#     'connector' = 'kafka',
#     'topic' = 'raw.public.green_taxi',
#     'properties.bootstrap.servers' = 'localhost:9092',
#     'properties.group.id' = 'parser-consumer-2-group',
#     'scan.startup.mode' = 'latest-offset',
#     'format' = 'json'
# )
# """

source_ddl = """
CREATE TABLE raw_green_taxi (
    payload ROW(
        after ROW(
            id STRING,
            vendorid INT,
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
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
            ehail_fee FLOAT,
            trip_type INT,
            congestion_surcharge FLOAT,
            cbd_congestion_fee FLOAT
        )
    ),
    pickup_datetime AS TO_TIMESTAMP(payload.after.lpep_pickup_datetime),
    dropoff_datetime AS TO_TIMESTAMP(payload.after.lpep_dropoff_datetime),
    WATERMARK FOR pickup_datetime AS pickup_datetime - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw.public.green_taxi',
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

    table = t_env.from_path("raw_green_taxi")

    table = table.select(
        col('payload').get('after').get('id').alias('id'),
        col('payload').get('after').get('vendorid').alias('vendorid'),
        col('payload').get('after').get('passenger_count').alias('passenger_count'),
        col('payload').get('after').get('trip_distance').alias('trip_distance'),
        col('payload').get(name_or_index='after').get('ratecodeid').alias('ratecodeid'),
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
        col('payload').get('after').get('ehail_fee').alias('ehail_fee'),
        col('payload').get('after').get('trip_type').alias('trip_type'),
        col('payload').get('after').get('congestion_surcharge').alias('congestion_surcharge'),
        col('payload').get('after').get('cbd_congestion_fee').alias('cbd_congestion_fee'),
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
    logger.info("Loading sample data...")

    table = parse_data(t_env)

    logger.info("Inserting data into sink table...")
    table.limit(1000).execute().print()