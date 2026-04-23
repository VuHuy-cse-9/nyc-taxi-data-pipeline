-- CREATE SCHEMA --
CREATE SCHEMA dwh.nyc_taxi
WITH (location = 's3a://data-warehouse/nyc_taxi_dataset');

-- Register Green Taxi tables in the schema --
CALL dwh.system.register_table(schema_name => 'nyc_taxi', table_name => 'green_taxi', table_location => 's3://data-warehouse/nyc_taxi_dataset/green_taxi/11_2025.parquet');

-- Register Yellow Taxi tables in the schema --
CALL dwh.system.register_table(schema_name => 'nyc_taxi', table_name => 'yellow_taxi', table_location => 's3://data-warehouse/nyc_taxi_dataset/yellow_taxi/11_2025.parquet');

-- Register For-Hire Vehicle (FHV) Taxi tables in the schema --
CALL dwh.system.register_table(schema_name => 'nyc_taxi', table_name => 'fhv_taxi', table_location => 's3://data-warehouse/nyc_taxi_dataset/for_hire_vehicle/11_2025.parquet');