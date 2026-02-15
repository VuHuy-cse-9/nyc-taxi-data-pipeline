-- CREATE TABLE --
CREATE TABLE IF NOT EXISTS dwh.nyc_taxi.traditional_taxi (
	VendorID INT,
	pickup_datetime TIMESTAMP,
	dropoff_datetime TIMESTAMP,
	passenger_count INT,
	trip_miles DOUBLE,
	RatecodeID INT,
	store_and_fwd_flag BOOLEAN,
	PULocationID INT,
	DOLocationID INT,
	payment_type VARCHAR(30),
	fare_amount DOUBLE,
	extra DOUBLE,
	mta_tax DOUBLE,
	tip_amount DOUBLE,
	tolls_amount DOUBLE,
	improvement_surcharge DOUBLE,
	total_amount DOUBLE,
	congestion_surcharge DOUBLE,
	cbd_congestion_fee DOUBLE,
	trip_type VARCHAR(30),
	airport_fee DOUBLE,
	taxi_type VARCHAR(30),
	trip_duration_seconds INTEGER
) WITH (
  external_location = 's3://data-warehouse/nyc_taxi_dataset/traditional_taxi.parquet',
  format = 'PARQUET'
);