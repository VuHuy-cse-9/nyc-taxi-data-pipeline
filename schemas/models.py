from enum import Enum

class TripType(str, Enum):
    STREET_HAIL = "Street-hail"
    DISPATCH = "Dispatch"
    UNKNOWN = "Unknown"

class TaxiType(str, Enum):
    GREEN = "green_taxi"
    YELLOW = "yellow_taxi"

class PaymentType(str, Enum):
    FLEX_FAIR_TRIP = "Flex Fair Trip"
    CREDIT_CARD = "Credit Card"
    CASH = "Cash"
    NO_CHARGE = "No Charge"
    DISPUTE = "Dispute"
    UNKNOWN = "Unknown"
    VOIDED_TRIP = "Voided Trip"