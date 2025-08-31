import pyspark.sql.functions as F
from schemas.models import PaymentType

def transform_ts_to_asia_timezone(column_name: str) ->F.Column:
    # Convert timestamp from New York timezone to Asia/Ho_Chi_Minh timezone
    return F.when(
        F.col(column_name).isNull(), F.lit(None),
    ).otherwise(
        F.convert_timezone(
            F.lit("America/New_York"), F.lit("Asia/Ho_Chi_Minh"), column_name
        )
    )

def ensure_boolean_type(column_name: str, true_value: str)->F.Column:
    cleaned_column = F.trim(F.lower(F.col(column_name)))
    return F.when(
        F.col(column_name).isNull(), F.lit(None)
    ).when(
        cleaned_column == true_value.lower(), F.lit(True)
    ).otherwise(
        F.lit(False)
    )

def process_payment_type(column_name: str)->F.Column:
    return F.when(
        F.col(column_name).isNull(), F.lit(None),
    ).when(
        F.col(column_name) == 1, F.lit(PaymentType.CREDIT_CARD.value)
    ).when(
        F.col(column_name) == 2, F.lit(PaymentType.CASH.value)
    ).when(
        F.col(column_name) == 3, F.lit(PaymentType.NO_CHARGE.value)
    ).when(
        F.col(column_name) == 4, F.lit(PaymentType.DISPUTE.value)
    ).when(
        F.col(column_name) == 5, F.lit(PaymentType.UNKNOWN.value)
    ).when(
        F.col(column_name) == 6, F.lit(PaymentType.VOIDED_TRIP.value)
    ).otherwise(
        F.lit(None)
    )

