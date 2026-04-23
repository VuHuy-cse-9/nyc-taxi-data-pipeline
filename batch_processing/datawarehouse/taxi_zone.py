from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from schemas.models import TaxiType

def preprocess(df: DataFrame)->DataFrame:
    df = df.withColumn(
        'service_zone',
        F.when(
            F.col('service_zone').isNull(), F.lit(None)
        ).when(
            F.col('service_zone') == F.lit('Boro Zone'), F.lit(TaxiType.GREEN.value)
        ).when(
            F.col('service_zone') == F.lit('Yellow Zone'), F.lit(TaxiType.YELLOW.value)
        ).otherwise(F.lit(None))
    )
    return df