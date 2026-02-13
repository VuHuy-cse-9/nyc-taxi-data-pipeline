import os
from pyflink.table import EnvironmentSettings, TableEnvironment, Table
from pyflink.table.expressions import col
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JARS_PATH = f"{os.getcwd()}/jars"

source_ddl = """
CREATE TABLE raw_yellow_taxi (
    payload ROW(
        after ROW(
            id ROW(
                `value` STRING,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            vendorid ROW(
                `value` INT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            tpep_pickup_datetime ROW(
                `value` STRING,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            tpep_dropoff_datetime ROW(
                `value` STRING,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            passenger_count ROW(
                `value` INT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            trip_distance ROW(
                `value` FLOAT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            ratecodeid ROW(
                `value` INT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            store_and_fwd_flag ROW(
                `value` STRING,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            pulocationid ROW(
                `value` INT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            dolocationid ROW(
                `value` INT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            payment_type ROW(
                `value` INT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            fare_amount ROW(
                `value` FLOAT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            extra ROW(
                `value` FLOAT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            mta_tax ROW(
                `value` FLOAT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            tip_amount ROW(
                `value` FLOAT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            tolls_amount ROW(
                `value` FLOAT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            improvement_surcharge ROW(
                `value` FLOAT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            total_amount ROW(
                `value` FLOAT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            congestion_surcharge ROW(
                `value` FLOAT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            airport_fee ROW(
                `value` FLOAT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            ),
            cbd_congestion_fee ROW(
                `value` FLOAT,
                deletion_ts BOOLEAN,
                `set` BOOLEAN
            )
        )
    ),
    pickup_datetime AS TO_TIMESTAMP(payload.after.tpep_pickup_datetime.`value`),
    dropoff_datetime AS TO_TIMESTAMP(payload.after.tpep_dropoff_datetime.`value`),
    WATERMARK FOR pickup_datetime AS pickup_datetime - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw.default.yellow_taxi',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'parser-consumer-2-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'scan.watermark.idle-timeout'='5second'
)
"""

source_avro_ddl = """
CREATE TABLE raw_yellow_taxi (
    id ROW(
        `value` STRING,
        deletion_ts BOOLEAN,
        `set` BOOLEAN
    )
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw.default.yellow_taxi',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'parser-consumer-2-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'debezium-avro-confluent',
    'debezium-avro-confluent.url' = 'http://localhost:8081'
)
"""

def parse_avro_data(t_env: TableEnvironment) -> Table:
    
    # Create table from schema.
    t_env.execute_sql(source_avro_ddl)

    table = t_env.from_path("raw_yellow_taxi")

    table.execute().print()

    table = table.select(
        col('payload').get('after').get('id').get("value").alias('id'),
        col('payload').get('after').get('vendorid').get("value").alias('vendorid'),
        # col('payload').get('after').get('tpep_pickup_datetime').alias('tpep_pickup_datetime'),
        # col('payload').get('after').get('tpep_dropoff_datetime').alias('tpep_dropoff_datetime'),
        col('payload').get('after').get('passenger_count').get("value").alias('passenger_count'),
        col('payload').get('after').get('trip_distance').get("value").alias('trip_distance'),
        col('payload').get('after').get('ratecodeid').get("value").alias('ratecodeid'),
        col('payload').get('after').get('store_and_fwd_flag').get("value").alias('store_and_fwd_flag'),
        col('payload').get('after').get('pulocationid').get("value").alias('pulocationid'),
        col('payload').get('after').get('dolocationid').get("value").alias('dolocationid'),
        col('payload').get('after').get('payment_type').get("value").alias('payment_type'),
        col('payload').get('after').get('fare_amount').get("value").alias('fare_amount'),
        col('payload').get('after').get('extra').get("value").alias('extra'),
        col('payload').get('after').get('mta_tax').get("value").alias('mta_tax'),
        col('payload').get('after').get('tip_amount').get("value").alias('tip_amount'),
        col('payload').get('after').get('tolls_amount').get("value").alias('tolls_amount'),
        col('payload').get('after').get('improvement_surcharge').get("value").alias('improvement_surcharge'),
        col('payload').get('after').get('total_amount').get("value").alias('total_amount'),
        col('payload').get('after').get('congestion_surcharge').get("value").alias('congestion_surcharge'),
        col('payload').get('after').get('cbd_congestion_fee').get("value").alias('cbd_congestion_fee'),
        col('payload').get('after').get('airport_fee').get("value").alias('airport_fee'),
        col('pickup_datetime'),
        col('dropoff_datetime')
    )

    return table

def parse_data(t_env: TableEnvironment) -> Table:
    
    # Create table from schema.
    t_env.execute_sql(source_ddl)

    table = t_env.from_path("raw_yellow_taxi")

    table = table.select(
        col('payload').get('after').get('id').get("value").alias('id'),
        col('payload').get('after').get('vendorid').get("value").alias('vendorid'),
        # col('payload').get('after').get('tpep_pickup_datetime').alias('tpep_pickup_datetime'),
        # col('payload').get('after').get('tpep_dropoff_datetime').alias('tpep_dropoff_datetime'),
        col('payload').get('after').get('passenger_count').get("value").alias('passenger_count'),
        col('payload').get('after').get('trip_distance').get("value").alias('trip_distance'),
        col('payload').get('after').get('ratecodeid').get("value").alias('ratecodeid'),
        col('payload').get('after').get('store_and_fwd_flag').get("value").alias('store_and_fwd_flag'),
        col('payload').get('after').get('pulocationid').get("value").alias('pulocationid'),
        col('payload').get('after').get('dolocationid').get("value").alias('dolocationid'),
        col('payload').get('after').get('payment_type').get("value").alias('payment_type'),
        col('payload').get('after').get('fare_amount').get("value").alias('fare_amount'),
        col('payload').get('after').get('extra').get("value").alias('extra'),
        col('payload').get('after').get('mta_tax').get("value").alias('mta_tax'),
        col('payload').get('after').get('tip_amount').get("value").alias('tip_amount'),
        col('payload').get('after').get('tolls_amount').get("value").alias('tolls_amount'),
        col('payload').get('after').get('improvement_surcharge').get("value").alias('improvement_surcharge'),
        col('payload').get('after').get('total_amount').get("value").alias('total_amount'),
        col('payload').get('after').get('congestion_surcharge').get("value").alias('congestion_surcharge'),
        col('payload').get('after').get('cbd_congestion_fee').get("value").alias('cbd_congestion_fee'),
        col('payload').get('after').get('airport_fee').get("value").alias('airport_fee'),
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