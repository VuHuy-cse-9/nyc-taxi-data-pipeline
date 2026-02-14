import os
from pyflink.table import EnvironmentSettings, TableEnvironment, Table
from pyflink.table.expressions import col
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JARS_PATH = f"{os.getcwd()}/jars"


avro_source_ddl = """
CREATE TABLE raw_green_taxi (
    id STRING,
    vendorid INT,
    lpep_pickup_datetime STRING,
    lpep_dropoff_datetime STRING,
    passenger_count INT,
    trip_distance DOUBLE,
    ratecodeid INT,
    store_and_fwd_flag STRING,
    pulocationid INT,
    dolocationid INT,
    payment_type INT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    ehail_fee DOUBLE,
    trip_type DOUBLE,
    congestion_surcharge DOUBLE,
    cbd_congestion_fee DOUBLE,
    pickup_datetime AS TO_TIMESTAMP(lpep_pickup_datetime),
    dropoff_datetime AS TO_TIMESTAMP(lpep_dropoff_datetime),
    WATERMARK FOR pickup_datetime AS pickup_datetime - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw.public.green_taxi',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink-consumer-group',
    'scan.startup.mode' = 'latest-offset',
    'scan.watermark.idle-timeout'='5second',
    'format' = 'debezium-avro-confluent',
    'debezium-avro-confluent.url' = 'http://schema-registry:8081'
)
"""

def parse_data(t_env: TableEnvironment) -> Table:

    # Create table from schema.
    t_env.execute_sql(avro_source_ddl)

    table = t_env.from_path("raw_green_taxi")

    table = table.select(
        col('id'),
        col('vendorid'),
        col('passenger_count'),
        col('trip_distance'),
        col('ratecodeid'),
        col('store_and_fwd_flag'),
        col('pulocationid'),
        col('dolocationid'),
        col('payment_type'),
        col('fare_amount'),
        col('extra'),
        col('mta_tax'),
        col('tip_amount'),
        col('tolls_amount'),
        col('improvement_surcharge'),
        col('total_amount'),
        col('ehail_fee'),
        col('trip_type'),
        col('congestion_surcharge'),
        col('cbd_congestion_fee'),
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