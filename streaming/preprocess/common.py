from pyflink.table.expressions import col, to_timestamp
from pyflink.table.udf import udf
from pyflink.table import DataTypes
from datetime import datetime
from zoneinfo import ZoneInfo
from schemas.models import TripType, TaxiType, PaymentType


@udf(result_type=DataTypes.TIMESTAMP_LTZ(3))
def transform_ts_to_asia_timezone(value: str, format: str = "%Y-%m-%d %H:%M:%S"):
    return datetime.strptime(value, format).replace(
        tzinfo=ZoneInfo("America/New_York")
    ).astimezone(ZoneInfo("Asia/Ho_Chi_Minh"))

def ensure_boolean_type(column_name: str, true_value: str):
    return col(column_name).lower_case.trim().similar(true_value.lower())

@udf(result_type=DataTypes.DOUBLE())
def total_seconds_between_timestamps(start: datetime, end: datetime):
    return (end - start).total_seconds()

@udf(result_type=DataTypes.STRING())
def process_trip_type(value):
    if value == 1.0:
        return TripType.STREET_HAIL.value
    elif value == 2.0:
        return TripType.DISPATCH.value
    else:
        return None
    
@udf(result_type=DataTypes.STRING())
def process_payment_type(value):
    if value == 1:
        return PaymentType.CREDIT_CARD.value
    elif value == 2:
        return PaymentType.CASH.value
    elif value == 3:
        return PaymentType.NO_CHARGE.value
    elif value == 4:
        return PaymentType.DISPUTE.value
    elif value == 5:
        return PaymentType.UNKNOWN.value
    elif value == 6:
        return PaymentType.VOIDED_TRIP.value
    else:
        return None