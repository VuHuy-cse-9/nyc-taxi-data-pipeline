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
) PARTITION BY RANGE (pickup_datetime);

CREATE TABLE fhvhv_mart_2025_07 PARTITION OF fhvhv_mart
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');

-- Optional: Add indexes on partitions (recommended for performance)
CREATE INDEX idx_fhvhv_mart_2025_07_pickup ON fhvhv_mart_2025_07 (pickup_datetime);

-- Optional: If you want a default partition for out-of-range data (e.g., future dates beyond planned partitions)
CREATE TABLE fhvhv_mart_default PARTITION OF fhvhv_mart
    DEFAULT;