from pyflink.table.expressions import col, coalesce, lit, if_then_else, null_of, Expression
from pyflink.table import DataTypes
from schemas.models import TripType, PaymentType


def ensure_boolean_type(column: Expression, true_value: str):
    return column.lower_case.trim().similar(lit(true_value.lower()))

def process_trip_type(column: Expression):
    return if_then_else(
        column.is_null, 
        lit(TripType.UNKNOWN.value), 
        if_then_else(
            column == 1.0, 
            lit(TripType.STREET_HAIL.value), 
            lit(TripType.DISPATCH.value)
        )
    )
    
def process_payment_type(value: Expression):
    """
    table.select(
        process_payment_type(col("payment_type"))
    )
    """
    return coalesce(
        if_then_else(value == 1, lit(PaymentType.CREDIT_CARD.value), null_of(DataTypes.STRING())),
        if_then_else(value == 2, lit(PaymentType.CASH.value), null_of(DataTypes.STRING())),
        if_then_else(value == 3, lit(PaymentType.NO_CHARGE.value), null_of(DataTypes.STRING())),
        if_then_else(value == 4, lit(PaymentType.DISPUTE.value), null_of(DataTypes.STRING())),
        if_then_else(value == 5, lit(PaymentType.UNKNOWN.value), null_of(DataTypes.STRING())),
        if_then_else(value == 6, lit(PaymentType.VOIDED_TRIP.value), null_of(DataTypes.STRING()))
    )