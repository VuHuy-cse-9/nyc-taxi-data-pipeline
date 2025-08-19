from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Convert Parquet to CSV") \
        .getOrCreate()

    # Define the input and output paths
    input_path = "local/volumes/minio/data/data-warehouse/nyc_taxi_dataset/fh_license_affiliation.parquet"
    output_path = "sample.csv"

    # Read the Parquet files
    df = spark.read.parquet(input_path)
    df = df.sample(0.1)
    df = df.toPandas()

    # Write the DataFrame to CSV format
    df.to_csv(output_path, index=False)

    print(f"Converted Parquet files from {input_path} to CSV at {output_path}")
    
    spark.stop()