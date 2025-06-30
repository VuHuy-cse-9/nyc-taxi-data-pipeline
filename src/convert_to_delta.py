from src.utils import create_minio_client, create_bucket_if_not_exists
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as T
import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip, DeltaTable

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY")
METADATA_DB = os.getenv("METADATA_DB", "metastore")

print(f"MINIO_ENDPOINT: {MINIO_ENDPOINT}")
print(f"MINIO_ACCESS_KEY: {MINIO_ACCESS_KEY}")
print(f"MINIO_SECRET_KEY: {MINIO_SECRET_KEY}")
print(f"METADATA_DB: {METADATA_DB}")

def create_spark_session():
    jars = "jars/hadoop-aws-3.4.1.jar,jars/aws-java-sdk-bundle-1.11.271-javadoc.jar"
    builder = SparkSession.builder\
        .appName("Convert parquet to Delta Lake") \
        .config('spark.memory.fraction', '0.8')\
        .config("spark.executor.memory", "12g")\
        .config("spark.driver.memory", "12g") \
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
        .config("spark.sql.warehouse.dir", "s3a://datawarehouse/") \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
        .config('spark.hadoop.fs.s3a.endpoint', "http://localhost:9000")\
        .config('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY)\
        .config('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY)\
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')\
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')\
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')\
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
        .config('spark.jars', jars)
    
    spark = configure_spark_with_delta_pip(
        builder, 
        extra_packages=['org.apache.hadoop:hadoop-aws:3.4.1']).getOrCreate()
    
    # .config("hive.metastore.uris", "thrift://localhost:9083") \
    # .enableHiveSupport()\
    # .config('spark.sql.catalog.spark_catalog', 'hive') \
    
    # Check spark session is created successfully
    if spark is None:
        raise Exception("Failed to create Spark session.")

    return spark

def preprocessing(df: DataFrame):
    return df.withColumn(
        'request_datetime', F.to_timestamp(F.col('request_datetime'), 'yyyy-MM-dd HH:mm:ss')
    ).withColumn(
        'on_scene_datetime', F.to_timestamp(F.col('on_scene_datetime'), 'yyyy-MM-dd HH:mm:ss')
    ).withColumn(
        'pickup_datetime', F.to_timestamp(F.col('pickup_datetime'), 'yyyy-MM-dd HH:mm:ss')
    ).withColumn(
        'dropoff_datetime', F.to_timestamp(F.col('dropoff_datetime'), 'yyyy-MM-dd HH:mm:ss')
    ).withColumnRenamed(
        'trip_time', 'trip_time_in_seconds'
    )

def create_delta_table(spark: SparkSession, path):
    return DeltaTable.create(spark) \
        .tableName("for_hire_vehicle") \
        .addColumn("hvfhs_license_num", T.StringType(), comment='') \
        .addColumn("dispatching_base_num", T.StringType()) \
        .addColumn("originating_base_num", T.StringType()) \
        .addColumn("request_datetime", T.TimestampType()) \
        .addColumn("on_scene_datetime", T.TimestampType()) \
        .addColumn("pickup_datetime", T.TimestampType()) \
        .addColumn("dropoff_datetime", T.TimestampType()) \
        .addColumn("PULocationID", T.IntegerType()) \
        .addColumn("DOLocationID", T.IntegerType()) \
        .addColumn("trip_miles", T.DoubleType()) \
        .addColumn("trip_time_in_seconds", T.LongType()) \
        .addColumn("base_passenger_fare", T.DoubleType()) \
        .addColumn("tolls", T.DoubleType()) \
        .addColumn("bcf", T.DoubleType()) \
        .addColumn("sales_tax", T.DoubleType()) \
        .addColumn("congestion_surcharge", T.DoubleType()) \
        .addColumn("airport_fee", T.DoubleType()) \
        .addColumn("tips", T.DoubleType()) \
        .addColumn("driver_pay", T.DoubleType()) \
        .addColumn("shared_request_flag", T.StringType()) \
        .addColumn("shared_match_flag", T.StringType()) \
        .addColumn('access_a_ride_flag', T.StringType()) \
        .addColumn('wav_request_flag', T.StringType()) \
        .addColumn('wav_match_flag', T.StringType()) \
        .addColumn('cbd_congestion_fee', T.DoubleType()) \
        .partitionedBy("hvfhs_license_num") \
        .location(path) \
        .execute()
        
        

def main():
    # Create Spark session
    spark = create_spark_session()
    
    minio_client = create_minio_client(
        MINIO_ENDPOINT, MINIO_ACCESS_KEY, 
        MINIO_SECRET_KEY, secure=False)
    
    # Create Delta Lake bucket if it doesn't exist
    raw_bucket_name = 'raw-bucket'
    dw_bucket_name = 'datawarehouse'
    create_bucket_if_not_exists(minio_client, dw_bucket_name)

    # List all file in bucket
    objects = minio_client.list_objects(bucket_name=raw_bucket_name, prefix='for-hire-vehicle/')
    object_names = [obj.object_name for obj in objects]
    print(f"Found {len(object_names)} files in bucket '{raw_bucket_name}'.")

    df = None
    for object_name in object_names:
        print(f"Processing file: {object_name}")
        path_read = f"s3a://{raw_bucket_name}/" + object_name
        if df is None:
            df = spark.read.parquet(path_read)
        else:
            df_temp = spark.read.parquet(path_read)
            df = df.union(df_temp)
        
    df = preprocessing(df)

    delta_table_path = f"s3a://{dw_bucket_name}/for-hire-vehicle"
    create_delta_table(spark, delta_table_path)

    # df.write.format("delta").mode('append').save(delta_table_path)
    df.write.format("delta")\
            .mode('append')\
            .save(delta_table_path)

    spark.stop()

    return


if __name__ == "__main__":
    main()