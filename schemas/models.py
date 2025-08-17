from enum import Enum

class TripType(str, Enum):
    STREET_HAIL = "Street-hail"
    DISPATCH = "Dispatch"

class TaxiType(str, Enum):
    GREEN = "green_taxi"
    YELLOW = "yellow_taxi"