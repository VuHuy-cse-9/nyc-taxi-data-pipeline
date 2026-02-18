from pyspark.sql import SparkSession
from configs.config import settings

def create_spark_session() -> SparkSession:
    """
    Create a Spark session with the specified configuration.
    """
    JAR_DIR = "jars"
    jars = f"{JAR_DIR}/hadoop-aws-3.4.1.jar,{JAR_DIR}/bundle-2.32.24.jar,{JAR_DIR}/postgresql-42.7.7.jar"
    builder: SparkSession.Builder = SparkSession.builder\
        .appName(settings.spark_app_name) \
        .master(settings.spark_master) \
        .config('spark.memory.fraction', '0.6')\
        .config("spark.executor.memory", "2g")\
        .config("spark.driver.memory", "2g") \
        .config('spark.hadoop.fs.s3a.endpoint', settings.minio_endpoint)\
        .config('spark.hadoop.fs.s3a.access.key', settings.minio_access_key)\
        .config('spark.hadoop.fs.s3a.secret.key', settings.minio_secret_key)\
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')\
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')\
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', str(settings.minio_secure).lower())\
        .config('spark.jars', jars)

    spark = builder.getOrCreate()
    
    # Check spark session is created successfully
    if spark is None:
        raise Exception("Failed to create Spark session.")

    return spark