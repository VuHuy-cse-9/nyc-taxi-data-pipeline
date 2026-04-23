from pyflink.table import EnvironmentSettings, TableEnvironment, Table
from parsers.fhvhv_parsing import parse_data as fhvhv_parse_data
from parsers.green_taxi_parsing import parse_data as green_taxi_parse_data
from parsers.yellow_taxi_parsing import parse_data as yellow_taxi_parse_data
from preprocess import (
    fhvhv_preprocess, green_taxi_preprocess, yellow_taxi_preprocess
)
from pyflink.table.expressions import col, TimePointUnit
import pyflink.table.expressions as F
from pyflink.table import DataTypes
import os
from schemas import (
    STREAM_ONLINE_FEATURE_TABLE_WITH_KAFKA_CONNECTOR, SINK_SCHEMA_TYPE, SINK_PAYLOAD_TYPE
)
from online_feat import (
    compute_online_feature
)

if not os.path.exists("/opt/flink/usrlib"):
    JARS_PATH = f"{os.getcwd()}/../jars"
else:
    JARS_PATH = "/opt/flink/usrlib"


def create_sink_table(table: Table):
    # Jsonschema only has these types: 
    # https://github.com/apache/kafka/blob/3.9.0/connect/json/src/main/java/org/apache/kafka/connect/json/JsonSchema.java

    def to_unix_timestamp(col_expr):
        return (F.timestamp_diff(TimePointUnit.SECOND, 
                        F.lit("1970-01-01 00:00:00").to_timestamp,
                        col_expr).cast(DataTypes.BIGINT()) * F.lit(1000))

    return table.select(
        F.row(
            F.lit("struct"),
            F.array(
                F.row(F.lit("pulocationid"), F.lit("int32"), F.lit(False), F.lit("org.apache.kafka.connect.data.Int32"), F.lit(1)),
                F.row(F.lit("window_start"), F.lit("int64"), F.lit(False), F.lit("org.apache.kafka.connect.data.Timestamp"), F.lit(1)),
                F.row(F.lit("window_end"), F.lit("int64"), F.lit(False), F.lit("org.apache.kafka.connect.data.Timestamp"), F.lit(1)),
                F.row(F.lit("demand_per_zone"), F.lit("int64"), F.lit(False), F.lit("org.apache.kafka.connect.data.Int64"), F.lit(1)),
                F.row(F.lit("taxi_type"), F.lit("string"), F.lit(False), F.lit("org.apache.kafka.connect.data.String"), F.lit(1)),
                F.row(F.lit("fleet_composition_per_zone"), F.lit("int64"), F.lit(False), F.lit("org.apache.kafka.connect.data.Int64"), F.lit(1)),
                F.row(F.lit("netflow_in"), F.lit("int32"), F.lit(False), F.lit("org.apache.kafka.connect.data.Int32"), F.lit(1)),
                F.row(F.lit("netflow_out"), F.lit("int32"), F.lit(False), F.lit("org.apache.kafka.connect.data.Int32"), F.lit(1)),
                F.row(F.lit("trip_avg_speed_mph"), F.lit("float"), F.lit(True), F.lit("org.apache.kafka.connect.data.Float32"), F.lit(1)),
                F.row(F.lit("trip_min_speed_mph"), F.lit("float"), F.lit(True), F.lit("org.apache.kafka.connect.data.Float32"), F.lit(1)),
                F.row(F.lit("trip_max_speed_mph"), F.lit("float"), F.lit(True), F.lit("org.apache.kafka.connect.data.Float32"), F.lit(1)),
                F.row(F.lit("trip_stddev_speed_mph"), F.lit("float"), F.lit(True), F.lit("org.apache.kafka.connect.data.Float32"), F.lit(1)),
                F.row(F.lit("trip_percentile_25_speed_mph"), F.lit("double"), F.lit(True), F.lit("org.apache.kafka.connect.data.Float64"), F.lit(1)),
                F.row(F.lit("trip_percentile_75_speed_mph"), F.lit("double"), F.lit(True), F.lit("org.apache.kafka.connect.data.Float64"), F.lit(1))
            )
        ).cast(SINK_SCHEMA_TYPE).alias("schema"),
        F.row(
            col("pulocationid"),
            to_unix_timestamp(col("window_start")).alias("window_start"),
            to_unix_timestamp(col("window_end")).alias("window_end"),
            col("demand_per_zone"),
            col("taxi_type"),
            col("fleet_composition_per_zone"),
            col("netflow_in"),
            col("netflow_out"),
            col("trip_avg_speed_mph"),
            col("trip_min_speed_mph"),
            col("trip_max_speed_mph"),
            col("trip_stddev_speed_mph"),
            col("trip_percentile_25_speed_mph"),
            col("trip_percentile_75_speed_mph"),
            col("trip_median_speed_mph")
        ).cast(SINK_PAYLOAD_TYPE).alias("payload")
    )

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
        + f"file://{JARS_PATH}/flink-connector-jdbc-3.3.0-1.20.jar;"
        + f"file://{JARS_PATH}/flink-avro-2.1.0.jar;"
        + f"file://{JARS_PATH}/debezium_plugins/avro-1.11.4.jar;"
        + f"file://{JARS_PATH}/jackson-databind-2.20.0.jar;"
        + f"file://{JARS_PATH}/jackson-core-2.20.0.jar;"
        + f"file://{JARS_PATH}/jackson-annotations-2.20.jar;"
        + f"file://{JARS_PATH}/jsr305-1.3.9.jar;"
        + f"file://{JARS_PATH}/flink-avro-confluent-registry-2.1.0.jar;"
        + f"file://{JARS_PATH}/debezium_plugins/kafka-schema-registry-client-8.0.0.jar;"
        + f"file://{JARS_PATH}/debezium_plugins/guava-32.0.1-jre.jar;"
        + f"file://{JARS_PATH}/jackson-datatype-jdk8-2.16.0.jar;"
        + f"file://{JARS_PATH}/debezium_plugins/failureaccess-1.0.3.jar"
    )

    # Step 1: Parse data from source
    print("Parsing data from source...")
    fhvhv_table = fhvhv_parse_data(t_env)
    green_taxi_table = green_taxi_parse_data(t_env)
    yellow_taxi_table = yellow_taxi_parse_data(t_env)

    # Step 2: Preprocess raw data
    fhvhv_table = fhvhv_preprocess(fhvhv_table)
    green_taxi_table = green_taxi_preprocess(green_taxi_table)
    yellow_taxi_table = yellow_taxi_preprocess(yellow_taxi_table)

    # Union green and yellow taxi
    yellow_taxi_table = yellow_taxi_table.select(
        *[col(c) for c in green_taxi_table.get_schema().get_field_names()])
    traditional_taxi_table = green_taxi_table.union_all(yellow_taxi_table)

    # Step 3: Compute online features
    online_fhvh_table = compute_online_feature(fhvhv_table)
    online_traditional_taxi_table = compute_online_feature(traditional_taxi_table)

    # Step 4: Union online feature from traditional and fhvh table
    online_feature_table = online_fhvh_table.union_all(online_traditional_taxi_table)
    
    # Step 5: Write to sink table
    sink_table = create_sink_table(online_feature_table)
    t_env.execute_sql(STREAM_ONLINE_FEATURE_TABLE_WITH_KAFKA_CONNECTOR.format(
        'online_feature', 'broker:29092'
    ))
    sink_table.execute_insert('online_feature').wait()

    return

if __name__ == "__main__":
    main()