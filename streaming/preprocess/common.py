from pyflink.table.expressions import col, to_timestamp
from pyflink.table.udf import udf
from pyflink.table import DataTypes
from datetime import datetime
from zoneinfo import ZoneInfo


@udf(result_type=DataTypes.TIMESTAMP_LTZ(3))
def transform_ts_to_asia_timezone(value: str):
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=ZoneInfo("America/New_York")
    ).astimezone(ZoneInfo("Asia/Ho_Chi_Minh"))

def ensure_boolean_type(column_name: str, true_value: str):
    return col(column_name).lower_case.trim().similar(true_value.lower())

@udf(result_type=DataTypes.DOUBLE())
def total_seconds_between_timestamps(start: datetime, end: datetime):
    return (end - start).total_seconds()