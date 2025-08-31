import os
import glob
from dotenv import load_dotenv
from database.utils import create_minio_client, create_bucket_if_not_exists, upload_file_to_minio
from threading import Thread

load_dotenv()
    
DATA_DIR = "dataset"
FHVHV_DIR = os.path.join(DATA_DIR, "fhvhv")
GREEN_TAXI_DIR = os.path.join(DATA_DIR, "green_taxi")
YELLOW_TAXI_DIR = os.path.join(DATA_DIR, "yellow_taxi")
METADATA_DIR = os.path.join(DATA_DIR, "metadata")

LOCAL2MINIO_DIR = {
    # FHVHV_DIR: "nyc_taxi_dataset/for_hire_vehicle",
    # GREEN_TAXI_DIR: "nyc_taxi_dataset/green_taxi",
    YELLOW_TAXI_DIR: "nyc_taxi_dataset/yellow_taxi",
    # METADATA_DIR: "nyc_taxi_dataset/metadata"
}


def main():
    # Create Minio Client
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key=os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key=os.getenv("MINIO_SECRET_KEY")
    minio_client = create_minio_client(
        minio_endpoint, minio_access_key, 
        minio_secret_key, secure=False)
    
    # Bucket to ingest data into
    bucket_name = os.getenv("DATA_LAKE_BUCKET", "data-lake")
    
    # Create bucket if it does not exist
    create_bucket_if_not_exists(minio_client, bucket_name=bucket_name)

    # Update load files to Minio
    threads = []
    for local_dir, minio_dir in LOCAL2MINIO_DIR.items():
        print(f"Uploading files from {local_dir} to Minio directory {minio_dir}...")

        # Get all file paths in the local directory
        file_paths = glob.glob(os.path.join(local_dir, '*'))
        print(f"Found {len(file_paths)} files to upload.")

        for file_path in file_paths:
            file_name = os.path.basename(file_path)
            print(f"Uploading {file_name} to Minio...")
            thread = Thread(
                target=upload_file_to_minio,
                args=(minio_client, bucket_name, file_path, os.path.join(minio_dir, file_name)))
            thread.start()
            threads.append(thread)

    for thread in threads:
        thread.join()
        print("All files uploaded successfully.")
    return

if __name__ == "__main__":
    main()