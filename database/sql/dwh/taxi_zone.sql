CREATE TABLE IF NOT EXISTS dwh.nyc_taxi.taxi_zone (
    location_id INT,
    borough VARCHAR(256),
    zone VARCHAR(256),
    service_zone VARCHAR(256)
) WITH (
    external_location = 's3://data-warehouse/nyc_taxi_dataset/taxi_zone.parquet',
    format = 'PARQUET'
);