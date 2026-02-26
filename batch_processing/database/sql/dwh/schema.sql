-- CREATE SCHEMA --
CREATE SCHEMA deltalake.nyc_taxi
WITH (location = 's3a://data-warehouse/nyc_taxi_dataset');


-- Register tables in the schema --
CALL deltalake.system.register_table(schema_name => 'nyc_taxi', table_name => 'green_taxi', table_location => 's3://data-warehouse/nyc_taxi_dataset/green_taxi/07_2025.parquet')
