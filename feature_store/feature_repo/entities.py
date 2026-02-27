from feast import Entity

pickup_zone = Entity(
    name="pickup_zone",
    join_keys=["pulocationid"],
)