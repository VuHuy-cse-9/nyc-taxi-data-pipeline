-- CREATE TABLE --
CREATE TABLE dwh.nyc_taxi.fh_vehicle (
    hvfhs_license_num VARCHAR(30),
    dispatching_base_num VARCHAR(30),
    originating_base_num VARCHAR(30),
    request_datetime TIMESTAMP,
    on_scene_datetime TIMESTAMP,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    PULocationID INT,
    DOLocationID INT,
    trip_miles DOUBLE,
    trip_duration_seconds INT,
    fare_amount DOUBLE,
    tolls DOUBLE,
    bcf DOUBLE,
    sales_tax DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    tips DOUBLE,
    driver_pay DOUBLE,
    shared_request_flag BOOLEAN,
    shared_match_flag BOOLEAN,
    access_a_ride_flag BOOLEAN,
    wav_request_flag BOOLEAN,
    wav_match_flag BOOLEAN,
    cbd_congestion_fee DOUBLE
) WITH (
    external_location = 's3://data-warehouse/nyc_taxi_dataset/fh_vehicle.parquet',
    format = 'PARQUET'
);