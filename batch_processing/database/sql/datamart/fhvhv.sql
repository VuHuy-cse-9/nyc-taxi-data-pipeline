CREATE TABLE fhvhv_mart (
    request_datetime TIMESTAMP,
    on_scene_datetime TIMESTAMP,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    trip_miles NUMERIC(10, 2),
    pulocationid INTEGER,
    dolocationid INTEGER,
    passenger_count INTEGER,
    fare_amount NUMERIC(10, 2)
) PARTITION BY RANGE (request_datetime);

-- Optional: If you want a default partition for out-of-range data (e.g., future dates beyond planned partitions)
CREATE TABLE fhvhv_mart_default PARTITION OF fhvhv_mart
    DEFAULT;