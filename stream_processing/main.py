from pyflink.table import EnvironmentSettings, TableEnvironment, StatementSet, Table
from pyflink.table.window import Slide
from stream_processing.parser import (
    fhvhv_parse_data, green_taxi_parse_data, yellow_taxi_parse_data
)
from stream_processing.preprocess import (
    fhvhv_preprocess, green_taxi_preprocess, yellow_taxi_preprocess
)
from pyflink.table.expressions import col, lit, Expression, JsonOnNull
import pyflink.table.expressions as F
import os
from functools import reduce
from schemas.stream.tables import (
    STREAM_ONLINE_FEATURE_TABLE_WITH_KAFKA_CONNECTOR
)
from pyflink.table.udf import udf


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
            Slide.over(lit(window_size).seconds).every(lit(slide_every).seconds)\
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
        Slide.over(lit(window_size).seconds).every(lit(slide_every).seconds)\
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
        Slide.over(lit(window_size).seconds).every(lit(slide_every).seconds)\
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

def join_table(table1: Table, table2: Table, join_by_cols: list[str], expressions: list[Expression] = []):
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
    joined_table = joined_table.drop_columns(
        *[col("t_" + c) for c in join_by_cols])
    
    if len(expressions) > 0:
        joined_table = joined_table.add_or_replace_columns(
            *expressions
        )

    return joined_table


def compute_online_feature(table: Table):
    # # Feature 1: Recent demand per zone
    WINDOW_SIZE = 10
    SLIDE_EVERY = 2


    demand_per_zone_table = \
        compute_window_count(table, 
                             "pickup_datetime", 
                             ["pulocationid"], "id",
                             feature_name="demand_per_zone",
                             window_size=WINDOW_SIZE,
                             slide_every=SLIDE_EVERY)
    
    print("Demand per zone table schema:")

    # Feature 2: Fleet composition per zone
    fleet_composition_per_zone_table = \
        compute_window_count(table, 
                             "pickup_datetime", 
                             ["pulocationid", "taxi_type"], "id",
                             feature_name="fleet_composition_per_zone",
                             window_size=WINDOW_SIZE,
                             slide_every=SLIDE_EVERY)

    print("Fleet composition per zone table schema:")
    fleet_composition_per_zone_table.print_schema()


    # Feature 3: Netflow of vehicle between zones
    netflow_per_zone_table = \
        compute_netflow_per_zone(table, 'pickup_datetime',
                                 window_size=WINDOW_SIZE,
                                 slide_every=SLIDE_EVERY)
    
    print("Netflow per zone table schema:")
    netflow_per_zone_table.print_schema()
    
    # Feature 4: Congestion Proxy via Recent Trip Speeds
    speed_features_table = \
        compute_congestion_proxy_via_trip_speed(table,
                                                window_size=WINDOW_SIZE,
                                                slide_every=SLIDE_EVERY)


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
    return stateful_features

def main():
    # Set up the environment settings for streaming mode
    print("Setting up Flink environment...")
    t_env = TableEnvironment.create(
        environment_settings=EnvironmentSettings.in_streaming_mode())
    
    # Setup configuration
    t_env.get_config().set("table.local-time-zone", "America/New_York")
    t_env.get_config().set("taskmanager.memory.network.max", "1gb")
    t_env.get_config().set("taskmanager.memory.network.fraction", "0.5")
    t_env.get_config().set(
        "pipeline.jars",
        f"file://{JARS_PATH}/flink-connector-kafka-4.0.0-2.0.jar;"
        + f"file://{JARS_PATH}/kafka-clients-3.9.0.jar;"
        + f"file://{JARS_PATH}/postgresql-42.7.7.jar;"
        + f"file://{JARS_PATH}/flink-connector-jdbc-3.3.0-1.20.jar"
        # + f"file://{JARS_PATH}/flink-avro-2.1.0.jar;"
        # + f"file://{JARS_PATH}/avro-1.11.4.jar;"
        # + f"file://{JARS_PATH}/jackson-databind-2.20.0.jar;"
        # + f"file://{JARS_PATH}/jackson-core-2.20.0.jar;"
        # + f"file://{JARS_PATH}/jackson-annotations-2.20.jar"
    )

    # Step 1: Parse data from source
    print("Parsing data from source...")
    # fhvhv_table = fhvhv_parse_data(t_env)
    green_taxi_table = green_taxi_parse_data(t_env)
    # yellow_taxi_table = yellow_taxi_parse_data(t_env)

    print("Green taxi table:")
    green_taxi_table.execute().print()

    # Step 2: Preprocess raw data
    fhvhv_table = fhvhv_preprocess(fhvhv_table)
    green_taxi_table = green_taxi_preprocess(green_taxi_table)
    yellow_taxi_table = yellow_taxi_preprocess(yellow_taxi_table)

    # Union green and yellow taxi
    yellow_taxi_table = yellow_taxi_table.select(
        *[col(c) for c in green_taxi_table.get_schema().get_field_names()])
    traditional_taxi_table = green_taxi_table.union_all(yellow_taxi_table)

    # Step 3: Compute online features
    online_traditional_taxi_table = compute_online_feature(traditional_taxi_table)
    online_fhvh_table = compute_online_feature(fhvhv_table)

    # Step 4: Union online feature from traditional and fhvh table
    online_feature_table = online_fhvh_table.union_all(online_traditional_taxi_table)

    t_env.execute_sql(STREAM_ONLINE_FEATURE_TABLE_WITH_KAFKA_CONNECTOR.format(
        'online_feature',
        'localhost:9092'
    ))

    online_feature_table.print_schema()
    online_feature_table.execute().print()
    # online_feature_table.execute_insert('online_feature').wait()

    return

if __name__ == "__main__":
    main()