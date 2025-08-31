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
    'scan.watermark.idle-timeout'='5second'
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


"""
(
  `pulocationid` INT,
  `window_start` TIMESTAMP(3),
  `window_end` TIMESTAMP(3),
  `demand_per_zone` BIGINT NOT NULL,
  `taxi_type` STRING NOT NULL,
  `fleet_composition_per_zone` BIGINT NOT NULL,
  `netflow_in` INT,
  `netflow_out` INT,
  `trip_avg_speed_mph` FLOAT,
  `trip_min_speed_mph` FLOAT,
  `trip_max_speed_mph` FLOAT,
  `trip_stddev_speed_mph` FLOAT,
  `trip_percentile_25_speed_mph` DOUBLE,
  `trip_percentile_75_speed_mph` DOUBLE,
  `trip_median_speed_mph` DOUBLE
)
"""

STREAM_ONLINE_FEATURE_TABLE_WITH_KAFKA_CONNECTOR = """
CREATE TABLE {} (
    pulocationid INT,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    demand_per_zone BIGINT NOT NULL,
    taxi_type STRING NOT NULL,
    fleet_composition_per_zone BIGINT NOT NULL,
    netflow_in INT,
    netflow_out INT,
    trip_avg_speed_mph FLOAT,
    trip_min_speed_mph FLOAT,
    trip_max_speed_mph FLOAT,
    trip_stddev_speed_mph FLOAT,
    trip_percentile_25_speed_mph DOUBLE,
    trip_percentile_75_speed_mph DOUBLE,
    trip_median_speed_mph DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'online-feature',
    'properties.bootstrap.servers' = '{}',
    'format' = 'json'
)
"""