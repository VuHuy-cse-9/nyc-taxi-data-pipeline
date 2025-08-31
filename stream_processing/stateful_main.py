from pyflink.table import TableEnvironment, EnvironmentSettings, Table
from pyflink.table.window import Tumble, Slide
from pyflink.table.expressions import col, lit, date_format, current_watermark
import pyflink.table.expressions as F
import os
from schemas.stream.tables import (
    STREAM_PREPROCESSED_TRADITIONAL_TAXI_TABLE_WITH_KAFKA_CONNECTOR,
    STREAM_PREPROCESSED_FHVHV_TABLE_WITH_KAFKA_CONNECTOR
)
from functools import reduce
import pandas as pd

JARS_PATH = f"{os.getcwd()}/jars"


def compute_window_count(table: Table, 
                         event_time_colname: str, 
                         group_colnames: list,
                         count_colname: str,
                         window_size: int = 15,
                         slide_every: int = 2,
                         feature_name: str = "count"):
    if len(group_colnames) == 0:
        raise ValueError("group_colnames must not be empty")
    # Define Window
    slide_window = \
            Slide.over(lit(window_size).minutes).every(lit(slide_every).minutes)\
                 .on(col(event_time_colname)).alias("w")
    # Define columns
    cols = [col(c) for c in group_colnames]
    return table.window(slide_window)\
            .group_by(*[col("w")] + cols)\
            .select(
                *cols,
                col("w").start.alias("window_start"),
                col("w").end.alias("window_end"),
                col(count_colname).count.alias(feature_name),
            )


def compute_netflow_per_zone(
        table: Table, event_time_colname: str, 
        window_size: int = 15, slide_every: int = 2):
    group_cols = [col("pulocationid"), col("taxi_type")]
    slide_window = \
        Slide.over(lit(window_size).minutes).every(lit(slide_every).minutes)\
            .on(col(event_time_colname)).alias("w")
    return table.window(slide_window)\
            .group_by(*[col("w")] + group_cols)\
            .select(
                *group_cols,
                col("w").start.alias("window_start"),
                col("w").end.alias("window_end"),
                F.if_then_else(
                    col("dolocationid") == col("pulocationid"), 
                    F.lit(1), F.lit(0)).sum.alias("netflow_in"),
                F.if_then_else(
                    col("dolocationid") != col("pulocationid"), 
                    F.lit(1), F.lit(0)).sum.alias("netflow_out"),
            )

def compute_congestion_proxy_via_trip_speed(table: Table, window_size: int = 15, slide_every: int = 2):
    speed_col = col("trip_miles") / col("trip_duration") * lit(3600) # miles per hour
    slide_window = \
        Slide.over(lit(window_size).minutes).every(lit(slide_every).minutes)\
             .on(col("pickup_datetime")).alias("w")
    prefix = "trip"
    return table.window(slide_window)\
                .group_by(*[col("w"), col('pulocationid')])\
                .select(
                    col("pulocationid"),
                    col("w").start.alias("window_start"),
                    col("w").end.alias("window_end"),
                    speed_col.avg.alias("{}_avg_speed_mph".format(prefix)),
                    speed_col.min.alias("{}_min_speed_mph".format(prefix)),
                    speed_col.max.alias("{}_max_speed_mph".format(prefix)),
                    speed_col.stddev_pop.alias("{}_stddev_speed_mph".format(prefix)),
                    speed_col.percentile(0.25).alias("{}_percentile_25_speed_mph".format(prefix)),
                    speed_col.percentile(0.75).alias("{}_percentile_75_speed_mph".format(prefix)),
                    speed_col.percentile(0.5).alias("{}_median_speed_mph".format(prefix)),
                )

def join_table(table1: Table, table2: Table, join_by_cols: list[str]):
    # Flink doesn't allow column names to be the same in joined tables
    # https://nightlies.apache.org/flink/flink-docs-master/api/python/reference/pyflink.table/api/pyflink.table.Table.join.html#pyflink.table.Table.join
    t_table2 = table2.rename_columns(
        *[col(c).alias("t_" + c) for c in join_by_cols]
    )

    # Define join predicate
    join_predicate = reduce(
        lambda a, b: a & b,
        [col(c) == col("t_" + c) for c in join_by_cols]
    )
    
    # Join table
    joined_table =  table1.join(t_table2,join_predicate)

    # Drop duplicate columns
    return joined_table.drop_columns(
        *[col("t_" + c) for c in join_by_cols])

def main():
    print("Setting up Flink environment...")
    # Set up the environment settings for streaming mode
    t_env = TableEnvironment.create(
        environment_settings=EnvironmentSettings.in_streaming_mode())
    
    # Setup configuration
    t_env.get_config().set_local_timezone("UTC")
    t_env.get_config().set('table.display.max-column-width', '60')
    t_env.get_config().set('web.submit.enable', 'true')
    # Configure checkpointing for web UI
    t_env.get_config().get_configuration().set_string(
        "execution.checkpointing.interval", "5000"
    )
    t_env.get_config().set(
        "pipeline.jars",
        f"file://{JARS_PATH}/flink-connector-kafka-4.0.0-2.0.jar;"
        + f"file://{JARS_PATH}/kafka-clients-3.9.0.jar;"
        + f"file://{JARS_PATH}/flink-table-api-java-2.1.0.jar"
    )

    t_env.get_config()

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

    # traditional_taxi_table.select(
    #     col("pickup_datetime"), col("dropoff_datetime")
    # ).execute().print()

    print("Performing sliding window aggregation on traditional taxi data...")
    sliding_w = traditional_taxi_table.window(
        Tumble.over(size=lit(5).seconds).on(col("pickup_datetime")).alias("w"))\
        .group_by(col("w"), col('vendorid'))\
        .select(
            # col("vendorid"),
            date_format(col("w").start, "HH:mm:ss").alias("window_start"),
            date_format(col("w").end, "HH:mm:ss").alias("window_end"),
            col("id").count.alias("total_trips"),
            date_format(col('pickup_datetime'), "HH:mm:ss").array_agg.alias("pickup_datetime_array"),
            date_format(
                current_watermark(col('pickup_datetime')),
                 "HH:mm:ss"
                ).array_agg.alias("current_watermark_array")
        )
    
    # # Feature 1: Recent demand per zone
    demand_per_zone_table = \
        compute_window_count(traditional_taxi_table, 
                             "pickup_datetime", 
                             ["pulocationid"], "id",
                             feature_name="demand_per_zone")


    # Feature 2: Fleet composition per zone
    fleet_composition_per_zone_table = \
        compute_window_count(traditional_taxi_table, 
                             "pickup_datetime", 
                             ["pulocationid", "taxi_type"], "id",
                             feature_name="fleet_composition_per_zone")


    # Feature 3: Netflow of vehicle between zones
    netflow_per_zone_table = \
        compute_netflow_per_zone(traditional_taxi_table, 'pickup_datetime')
    
    # Feature 4: Congestion Proxy via Recent Trip Speeds
    speed_features_table = \
        compute_congestion_proxy_via_trip_speed(traditional_taxi_table)

    # speed_features_table.print_schema()
    # speed_features_table.execute().print()

    stateful_features = join_table(
        demand_per_zone_table, fleet_composition_per_zone_table,
        ['pulocationid', 'window_start', 'window_end']
    )

    stateful_features = join_table(
        stateful_features, netflow_per_zone_table,
        ['pulocationid', 'window_start', 'window_end', 'taxi_type']
    )

    stateful_features = join_table(
        stateful_features, speed_features_table,
        ['pulocationid', 'window_start', 'window_end']
    )

    stateful_features.print_schema()

    stateful_features.execute().print()


if __name__ == "__main__":
    main()