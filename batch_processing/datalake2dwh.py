from database.utils import create_minio_client, create_bucket_if_not_exists
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from batch_processing.datasource.green_taxi_preprocess import preprocess as green_taxi_preprocess
from batch_processing.datasource.yellow_taxi_preprocess import preprocess as yellow_taxi_preprocess
from batch_processing.datasource.fhvhv_preprocess import preprocess as fhvhv_preprocess
from batch_processing.datasource.taxi_zone_preprocess import preprocess as taxi_zone_preprocess

load_dotenv()

# Minio Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY")
WAREHOUSE_BUCKET = os.getenv("WAREHOUSE_BUCKET", "data-warehouse")
DATALAKE_BUCKET = os.getenv("DATA_LAKE_BUCKET", "data-lake")

# Hive Metastore Configuration
METADATA_URI = os.getenv("METADATA_URI", "thrift://localhost:9083")

DATA2MINIO_DIR = {
    'for-hire-vehicle_dir': 'nyc_taxi_dataset/for_hire_vehicle',
    'green-taxi_dir': 'nyc_taxi_dataset/green_taxi',
    'yellow-taxi_dir': 'nyc_taxi_dataset/yellow_taxi',
    'taxi_zone': 'nyc_taxi_dataset/metadata/taxi_zone_lookup.csv',
    'fh_license_affiliation': 'nyc_taxi_dataset/metadata/High_Volume_License_Numbers_and_Affiliations.csv',
    'green_taxi_dictionary': 'nyc_taxi_dataset/metadata/green_taxi_dictionary.csv',
    'yellow_taxi_dictionary': 'nyc_taxi_dataset/metadata/yellow_taxi_dictionary.csv',
    'fhvhv_dictionary': 'nyc_taxi_dataset/metadata/High_Volume_FHV_trip_data_dictionary.csv'
}

def create_spark_session() -> SparkSession:
    """
    Create a Spark session with the specified configuration.
    """
    jars = "jars/hadoop-aws-3.4.1.jar,jars/bundle-2.32.24.jar"
    builder = SparkSession.builder\
        .appName("Preprocessing data") \
        .config('spark.memory.fraction', '0.8')\
        .config("spark.executor.memory", "12g")\
        .config("spark.driver.memory", "12g") \
        .config('spark.hadoop.fs.s3a.endpoint', MINIO_ENDPOINT)\
        .config('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY)\
        .config('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY)\
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')\
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')\
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')\
        .config('spark.jars', jars)

    spark = builder.getOrCreate()
    
    # Check spark session is created successfully
    if spark is None:
        raise Exception("Failed to create Spark session.")

    return spark

def ingest_data(spark: SparkSession):
    # Connect to Minio
    minio_client = create_minio_client(
        MINIO_ENDPOINT, MINIO_ACCESS_KEY, 
        MINIO_SECRET_KEY, secure=False)
    
    # Create Delta Lake bucket if it doesn't exist
    # List all file in bucket
    dataset2df = {}
    for dataset, minio_path in DATA2MINIO_DIR.items():
        if '_dir' in dataset:
            # Read all parquet files in the directory
            objects = minio_client.list_objects(
                bucket_name=DATALAKE_BUCKET, prefix=minio_path, recursive=True)
            object_names = [obj.object_name for obj in objects]
            
            print(f"Found {len(object_names)} files in bucket '{DATALAKE_BUCKET}'.")

            df_list = []
            for object_name in object_names:
                print(f"Processing file: {object_name}")
                path_read = f"s3a://{DATALAKE_BUCKET}/" + object_name
                df = spark.read.parquet(path_read)
                df_list.append(df)

            if len(df_list) > 0:
                df: DataFrame = df_list[0]
                for df_temp in df_list[1:]:
                    df = df.union(df_temp)

        elif 'csv' in minio_path:
            # Read CSV file
            path_read = f"s3a://{DATALAKE_BUCKET}/" + minio_path
            df = spark.read.csv(path_read, header=True, inferSchema=True)

        dataset2df[dataset] = df
    return dataset2df

def ingest_local_data(spark: SparkSession):
    import glob

    DATA2MINIO_DIR = {
        'for-hire-vehicle_dir': 'dataset/fhvhv',
        'green-taxi_dir': 'dataset/green_taxi',
        'yellow-taxi_dir': 'dataset/yellow_taxi',
        'taxi_zone': 'dataset/metadata/taxi_zone_lookup.csv',
        'hv_license_affi': 'dataset/metadata/High_Volume_License_Numbers_and_Affiliations.csv',
        'green_taxi_dictionary': 'dataset/metadata/green_taxi_dictionary.csv',
        'yellow_taxi_dictionary': 'dataset/metadata/yellow_taxi_dictionary.csv',
        'fhvhv_dictionary': 'dataset/metadata/High_Volume_FHV_trip_data_dictionary.csv'
    }

    # List all file in bucket
    dataset2df = {}
    for dataset, minio_path in DATA2MINIO_DIR.items():
        if '_dir' in dataset:
            # Read all parquet files in the directory
            object_paths = glob.glob(os.path.join(minio_path, '*.parquet'))

            # print(f"Found {len(object_paths)} files'.")

            df_list = []
            for object_path in object_paths:
                # print(f"Processing file: {object_path}")
                df = spark.read.parquet(object_path)
                df_list.append(df)

            if len(df_list) > 0:
                df: DataFrame = df_list[0]
                for df_temp in df_list[1:]:
                    df = df.union(df_temp)

        elif 'csv' in minio_path:
            # Read CSV file
            assert os.path.exists(minio_path), f"File {minio_path} does not exist."
            df = spark.read.csv(minio_path, header=True, inferSchema=True)

        dataset2df[dataset] = df

    return dataset2df

def write_to_warehouse(dataset2df: dict[str, DataFrame]):
    """
    Write DataFrame to Hive Metastore as a table.
    """

    for dataset, df in dataset2df.items():
        # Save as parquet file
        df = df.sample(fraction=1.0, seed=32).sample(fraction=0.1, seed=23)  # Sample 10% of the data
        output_path = os.path.join('nyc_taxi_dataset', f"{dataset}.parquet")
        df.write.parquet(
            f"s3a://{WAREHOUSE_BUCKET}/{output_path}", mode="overwrite", compression="snappy")


    return

def create_spark_local_session():
    return SparkSession.builder \
        .appName("Preprocessing data - Local") \
        .master("local[*]") \
        .config("spark.driver.memory", "12g") \
        .getOrCreate()

def preprocess(dataset2df: dict[str, DataFrame]):
    dataset2df['green-taxi_dir'] = green_taxi_preprocess(dataset2df['green-taxi_dir'])
    dataset2df['yellow-taxi_dir'] = yellow_taxi_preprocess(dataset2df['yellow-taxi_dir'])

    # Union green and yellow taxi data into single traditional taxi data
    dataset2df['green-taxi_dir'] = dataset2df['green-taxi_dir'].select(
        *dataset2df['yellow-taxi_dir'].columns
    ) # Reorder columns
    dataset2df['traditional_taxi'] = dataset2df['green-taxi_dir'].union(dataset2df['yellow-taxi_dir'])

    # Remove the original green and yellow taxi data
    del dataset2df['green-taxi_dir']
    del dataset2df['yellow-taxi_dir']

    dataset2df['fh_vehicle'] = fhvhv_preprocess(dataset2df['for-hire-vehicle_dir'])
    dataset2df['taxi_zone'] = taxi_zone_preprocess(dataset2df['taxi_zone'])

    del dataset2df['for-hire-vehicle_dir']

    return dataset2df
        
        

def main():
    # Create Spark session
    spark = create_spark_session()

    # Ingest data from local files
    dataset2df = ingest_data(spark)

    # Preprocess datas
    dataset2df = preprocess(dataset2df)

    write_to_warehouse(dataset2df)

    spark.stop()

    return


if __name__ == "__main__":
    main()