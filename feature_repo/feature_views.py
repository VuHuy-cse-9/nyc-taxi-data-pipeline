from feast import FeatureView, Field
from feast.types import Float32, Int64
from datetime import timedelta

from entities import pickup_zone
from data_sources import green_taxi_source, yellow_taxi_source, fhvhv_source

green_taxi_features = FeatureView(
    name="green_taxi_features",
    entities=[pickup_zone],
    ttl=timedelta(days=365),
    schema=[
        Field(name="trip_miles", dtype=Float32),
        Field(name="passenger_count", dtype=Int64),
        Field(name="fare_amount", dtype=Float32),
    ],
    source=green_taxi_source,
)

yellow_taxi_features = FeatureView(
    name="yellow_taxi_features",
    entities=[pickup_zone],
    ttl=timedelta(days=365),
    schema=[
        Field(name="trip_miles", dtype=Float32),
        Field(name="passenger_count", dtype=Int64),
        Field(name="fare_amount", dtype=Float32),
    ],
    source=yellow_taxi_source,
)

fhvhv_features = FeatureView(
    name="fhvhv_features",
    entities=[pickup_zone],
    ttl=timedelta(days=365),
    schema=[
        Field(name="trip_miles", dtype=Float32),
        Field(name="passenger_count", dtype=Int64),
        Field(name="fare_amount", dtype=Float32),
    ],
    source=fhvhv_source,
)