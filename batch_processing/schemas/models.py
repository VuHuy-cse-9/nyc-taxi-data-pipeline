from enum import Enum

class TripType(str, Enum):
    STREET_HAIL = "Street-hail"
    DISPATCH = "Dispatch"

class TaxiType(str, Enum):
    GREEN = "green_taxi"
    YELLOW = "yellow_taxi"
    FHVH = "fore_hire_vehicle"

class PaymentType(str, Enum):
    FLEX_FAIR_TRIP = "Flex Fair Trip"
    CREDIT_CARD = "Credit Card"
    CASH = "Cash"
    NO_CHARGE = "No Charge"
    DISPUTE = "Dispute"
    UNKNOWN = "Unknown"
    VOIDED_TRIP = "Voided Trip"