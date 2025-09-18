from pyflink.table import DataTypes

PREPROCESSED_TRADITIONAL_TAXI_TABLE_WITH_KAFKA_CONNECTOR = """
CREATE TABLE {} (
    id STRING,
    vendorid INT,
    passenger_count INT,
    trip_miles FLOAT,
    ratecodeid INT,
    pulocationid INT,
    dolocationid INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    cbd_congestion_fee FLOAT,
    pickup_datetime TIMESTAMP(3),
    dropoff_datetime TIMESTAMP(3),
    store_and_fwd_flag BOOLEAN,
    trip_type STRING,
    payment_type STRING,
    airport_fee DOUBLE,
    taxi_type STRING,
    trip_duration INT
) WITH (
    'connector' = 'kafka',
    'topic' = '{}',
    'properties.bootstrap.servers' = '{}',
    'format' = 'json',
    'sink.transactional-id-prefix' = 'traditional_taxi'
)
"""

PREPROCESSED_FHVHV_TABLE_WITH_KAFKA_CONNECTOR = """
CREATE TABLE {} (
    id STRING,
    hvfhs_license_num STRING,
    dispatching_base_num STRING,
    originating_base_num STRING,
    pulocationid INT,
    dolocationid INT,
    trip_miles FLOAT,
    trip_duration_seconds INT,
    fare_amount FLOAT,
    tolls FLOAT,
    bcf FLOAT,
    sales_tax FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    tips FLOAT,
    driver_pay FLOAT,
    cbd_congestion_fee FLOAT,
    request_datetime TIMESTAMP(3),
    on_scene_datetime TIMESTAMP(3),
    pickup_datetime TIMESTAMP(3),
    dropoff_datetime TIMESTAMP(3),
    shared_request_flag BOOLEAN,
    shared_match_flag BOOLEAN,
    access_a_ride_flag BOOLEAN,
    wav_request_flag BOOLEAN,
    wav_match_flag BOOLEAN
) WITH (
    'connector' = 'kafka',
    'topic' = '{}',
    'properties.bootstrap.servers' = '{}',
    'format' = 'json',
    'sink.transactional-id-prefix' = 'fore_hire_vehicle'
)
"""

STREAM_PREPROCESSED_TRADITIONAL_TAXI_TABLE_WITH_KAFKA_CONNECTOR = """
CREATE TABLE {} (
    id STRING,
    vendorid INT,
    passenger_count INT,
    trip_miles FLOAT,
    ratecodeid INT,
    pulocationid INT,
    dolocationid INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    cbd_congestion_fee FLOAT,
    pickup_datetime TIMESTAMP(0),
    dropoff_datetime TIMESTAMP(0),
    store_and_fwd_flag BOOLEAN,
    trip_type STRING,
    payment_type STRING,
    airport_fee DOUBLE,
    taxi_type STRING,
    trip_duration INT,
    WATERMARK FOR pickup_datetime AS pickup_datetime - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '{}',
    'properties.bootstrap.servers' = '{}',
    'properties.group.id' = 'preprocess-consumer-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'scan.watermark.idle-timeout' = '5second'
)
"""


STREAM_PREPROCESSED_FHVHV_TABLE_WITH_KAFKA_CONNECTOR = """
CREATE TABLE {} (
    id STRING,
    hvfhs_license_num STRING,
    dispatching_base_num STRING,
    originating_base_num STRING,
    pulocationid INT,
    dolocationid INT,
    trip_miles FLOAT,
    trip_duration_seconds INT,
    fare_amount FLOAT,
    tolls FLOAT,
    bcf FLOAT,
    sales_tax FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    tips FLOAT,
    driver_pay FLOAT,
    cbd_congestion_fee FLOAT,
    request_datetime TIMESTAMP(3),
    on_scene_datetime TIMESTAMP(3),
    pickup_datetime TIMESTAMP(3),
    dropoff_datetime TIMESTAMP(3),
    shared_request_flag BOOLEAN,
    shared_match_flag BOOLEAN,
    access_a_ride_flag BOOLEAN,
    wav_request_flag BOOLEAN,
    wav_match_flag BOOLEAN,
    WATERMARK FOR pickup_datetime AS pickup_datetime - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '{}',
    'properties.bootstrap.servers' = '{}',
    'properties.group.id' = 'preprocess-consumer-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
)
"""

STREAM_ONLINE_FEATURE_TABLE_WITH_KAFKA_CONNECTOR = """
CREATE TABLE {} (
    pulocationid INT,
    window_start TIMESTAMP(3) NOT NULL,
    window_end TIMESTAMP(3) NOT NULL,
    demand_per_zone BIGINT NOT NULL,
    taxi_type STRING,
    fleet_composition_per_zone BIGINT NOT NULL,
    netflow_in INT NOT NULL,
    netflow_out INT NOT NULL,
    trip_avg_speed_mph FLOAT,
    trip_min_speed_mph FLOAT,
    trip_max_speed_mph FLOAT,
    trip_stddev_speed_mph FLOAT,
    trip_percentile_25_speed_mph DOUBLE,
    trip_percentile_75_speed_mph DOUBLE,
    trip_median_speed_mph DOUBLE,
    id STRING NOT NULL PRIMARY KEY NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'online-feature',
    'properties.bootstrap.servers' = '{}',
    'format' = 'debezium-json',
    'key.format' = 'json',
    'key.fields' = 'id'
)
"""


STREAM_ONLINE_FEATURE_TABLE_WITH_KAFKA_CONNECTOR = """
CREATE TABLE {} (
    schema ROW(
        type STRING,
        fields ARRAY<ROW(field STRING, type STRING, optional BOOLEAN, name STRING, version INT)>
    ),
    payload ROW(
        pulocationid INT,
        window_start BIGINT NOT NULL,
        window_end BIGINT NOT NULL,
        demand_per_zone BIGINT NOT NULL,
        taxi_type STRING,
        fleet_composition_per_zone BIGINT NOT NULL,
        netflow_in INT NOT NULL,
        netflow_out INT NOT NULL,
        trip_avg_speed_mph FLOAT,
        trip_min_speed_mph FLOAT,
        trip_max_speed_mph FLOAT,
        trip_stddev_speed_mph FLOAT,
        trip_percentile_25_speed_mph DOUBLE,
        trip_percentile_75_speed_mph DOUBLE,
        trip_median_speed_mph DOUBLE
    )
) WITH (
    'connector' = 'kafka',
    'topic' = 'online-feature',
    'properties.bootstrap.servers' = '{}',
    'format' = 'json'
)
"""


SINK_SCHEMA_TYPE = DataTypes.ROW([
        DataTypes.FIELD("type", DataTypes.STRING()),
        DataTypes.FIELD("fields", DataTypes.ARRAY(
            DataTypes.ROW([
                DataTypes.FIELD("field", DataTypes.STRING()),
                DataTypes.FIELD("type", DataTypes.STRING()),
                DataTypes.FIELD("optional", DataTypes.BOOLEAN()),
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("version", DataTypes.INT())
            ])
        ))
    ])

SINK_PAYLOAD_TYPE = DataTypes.ROW([
    DataTypes.FIELD("pulocationid", DataTypes.INT()),
    DataTypes.FIELD("window_start", DataTypes.BIGINT()),
    DataTypes.FIELD("window_end", DataTypes.BIGINT()),
    DataTypes.FIELD("demand_per_zone", DataTypes.BIGINT()),
    DataTypes.FIELD("taxi_type", DataTypes.STRING()),
    DataTypes.FIELD("fleet_composition_per_zone", DataTypes.BIGINT()),
    DataTypes.FIELD("netflow_in", DataTypes.INT()),
    DataTypes.FIELD("netflow_out", DataTypes.INT()),
    DataTypes.FIELD("trip_avg_speed_mph", DataTypes.FLOAT()),
    DataTypes.FIELD("trip_min_speed_mph", DataTypes.FLOAT()),
    DataTypes.FIELD("trip_max_speed_mph", DataTypes.FLOAT()),
    DataTypes.FIELD("trip_stddev_speed_mph", DataTypes.FLOAT()),
    DataTypes.FIELD("trip_percentile_25_speed_mph", DataTypes.DOUBLE()),
    DataTypes.FIELD("trip_percentile_75_speed_mph", DataTypes.DOUBLE()),
    DataTypes.FIELD("trip_median_speed_mph", DataTypes.DOUBLE())
])