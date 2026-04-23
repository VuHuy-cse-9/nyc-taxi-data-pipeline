import os
import glob
from dotenv import load_dotenv
from database.minio_client import create_minio_client, create_bucket_if_not_exists, upload_file_to_minio
from threading import Thread
from configs.config import settings
from commons.logging import logger

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
    minio_client = create_minio_client()
    
    # Create bucket if it does not exist
    create_bucket_if_not_exists(minio_client, bucket_name=settings.datalake_bucket)

    # Update load files to Minio
    threads = []
    for local_dir, minio_dir in LOCAL2MINIO_DIR.items():
        logger.info(f"Uploading files from {local_dir} to Minio directory {minio_dir}...")

        # Get all file paths in the local directory
        file_paths = glob.glob(os.path.join(local_dir, '*'))
        logger.info(f"Found {len(file_paths)} files to upload.")
        for file_path in file_paths:
            file_name = os.path.basename(file_path)
            logger.info(f"Uploading {file_name} to Minio...")
            thread = Thread(
                target=upload_file_to_minio,
                args=(minio_client, settings.datalake_bucket, file_path, os.path.join(minio_dir, file_name)))
            thread.start()
            threads.append(thread)

    for thread in threads:
        thread.join()
    logger.info("All files uploaded successfully.")
    return

if __name__ == "__main__":
    main()