import os
import glob
from dotenv import load_dotenv
from src.utils import create_minio_client, create_bucket_if_not_exists, upload_file_to_minio
from threading import Thread

load_dotenv()
    
def main():
    # Create Minio Client
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key=os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key=os.getenv("MINIO_SECRET_KEY")
    minio_client = create_minio_client(
        minio_endpoint, minio_access_key, 
        minio_secret_key, secure=False)
    bucket_name = 'raw-bucket'
    
    create_bucket_if_not_exists(minio_client, bucket_name=bucket_name)


    file_paths = glob.glob('dataset/fhvhv_tripdata_2025-*.parquet')
    print(f"Found {len(file_paths)} files to upload.")

    # Update load files to Minio
    threads = []
    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        print(f"Uploading {file_name} to Minio...")
        thread = Thread(target=upload_file_to_minio, args=(minio_client, bucket_name, file_path, os.path.join('for-hire-vehicle', file_name)))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
        
    print("All files uploaded successfully.")
    return

if __name__ == "__main__":
    main()