from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)

# -----------------
# Green taxi source
# -----------------

green_taxi_source = PostgreSQLSource(
    name="green_taxi_postgres_source",
    query="""
        SELECT
            pickup_datetime,
            dropoff_datetime,
            trip_miles,
            pulocationid,
            dolocationid,
            passenger_count,
            fare_amount
        FROM public.green_taxi_mart
    """,
    timestamp_field="pickup_datetime",
)

# -----------------
# Yellow taxi source
# -----------------
yellow_taxi_source = PostgreSQLSource(
    name="yellow_taxi_postgres_source",
    query="""
        SELECT
            pickup_datetime,
            dropoff_datetime,
            trip_miles,
            pulocationid,
            dolocationid,
            passenger_count,
            fare_amount
        FROM public.yellow_taxi_mart
    """,
    timestamp_field="pickup_datetime",
)

# -----------------
# FHVHV source
# -----------------
fhvhv_source = PostgreSQLSource(
    name="fhvhv_postgres_source",
    query="""
        SELECT
            request_datetime,
            pickup_datetime,
            dropoff_datetime,
            trip_miles,
            pulocationid,
            dolocationid,
            passenger_count,
            fare_amount
        FROM public.fhvhv_mart
    """,
    timestamp_field="request_datetime",
)