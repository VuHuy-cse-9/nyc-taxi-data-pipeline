CREATE TABLE yellow_taxi_mart (
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    trip_miles NUMERIC(10, 2),
    pulocationid INTEGER,
    dolocationid INTEGER,
    passenger_count INTEGER,
    fare_amount NUMERIC(10, 2)
) PARTITION BY RANGE (pickup_datetime);

CREATE TABLE yellow_taxi_mart_2025_07 PARTITION OF yellow_taxi_mart
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');

-- Optional: Add indexes on partitions (recommended for performance)
CREATE INDEX idx_yellow_taxi_mart_2025_07_pickup ON yellow_taxi_mart_2025_07 (pickup_datetime);

-- Optional: If you want a default partition for out-of-range data (e.g., future dates beyond planned partitions)
CREATE TABLE yellow_taxi_mart_default PARTITION OF yellow_taxi_mart
    DEFAULT;