import os
from pyflink.table import DataTypes, Table
from pyflink.table.expressions import col, lit
from pyflink.table.udf import udf
from stream_processing.preprocess.common import (
    ensure_boolean_type, process_trip_type, process_payment_type
)
from pyflink.table.expressions import to_timestamp, timestamp_diff, TimePointUnit
import pandas as pd
import logging
from schemas.models import TaxiType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JARS_PATH = f"{os.getcwd()}/jars"

def preprocess(table: Table):
    return table.add_columns(
        ensure_boolean_type(col("store_and_fwd_flag"), "Y").alias("p_store_and_fwd_flag"),
        process_trip_type(col("trip_type")).alias("p_trip_type"),
        process_payment_type(col("payment_type")).alias("p_payment_type"),
        lit(0.0).cast(DataTypes.DOUBLE()).alias("airport_fee"),
        lit(TaxiType.GREEN.value).cast(DataTypes.STRING()).alias("taxi_type"),
    ).drop_columns(
        col("ehail_fee"),
        col("trip_type"),
        col("payment_type"),
        col("store_and_fwd_flag")
    ).rename_columns(
        col("trip_distance").alias("trip_miles"),
        col("p_store_and_fwd_flag").alias("store_and_fwd_flag"),
        col("p_trip_type").alias("trip_type"),
        col("p_payment_type").alias("payment_type"),
    ).add_columns(
        timestamp_diff(TimePointUnit.SECOND, 
                       col("pickup_datetime"), 
                       col("dropoff_datetime")).alias("trip_duration"),
    )