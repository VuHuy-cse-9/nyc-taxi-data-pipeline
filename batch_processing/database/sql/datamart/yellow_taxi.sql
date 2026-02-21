CREATE TABLE yellow_taxi_mart (
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    trip_miles NUMERIC(10, 2),
    pulocationid INTEGER,
    dolocationid INTEGER,
    passenger_count INTEGER,
    fare_amount NUMERIC(10, 2)
) PARTITION BY RANGE (pickup_datetime);

-- Optional: If you want a default partition for out-of-range data (e.g., future dates beyond planned partitions)
CREATE TABLE yellow_taxi_mart_default PARTITION OF yellow_taxi_mart
    DEFAULT;