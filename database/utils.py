from minio import Minio
from minio.error import S3Error
import os


def create_minio_client(endpoint, secret_key, access_key, secure=False):
    minio_client = Minio(
        endpoint,
        access_key=secret_key,
        secret_key=access_key,
        secure=secure
    )

    try:
        minio_client.list_buckets()
        print(f"Connected to Minio successfully.")
        return minio_client

    except S3Error as e:
        print(f"Error connecting to Minio: {e}")
        raise Exception(f"Failed to connect to Minio: {str(e)}")


def create_bucket_if_not_exists(minio_client, bucket_name):
    # Create Delta Lake bucket if it doesn't exist
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except S3Error as e:
        print(f"Error creating bucket '{bucket_name}': {e}")
        raise Exception(f"Failed to create bucket: {str(e)}")

def upload_file_to_minio(minio_client, bucket_name, file_path, object_name):
    try:
        minio_client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=file_path
            )
        print(f"File '{file_path}' uploaded to bucket '{bucket_name}' as '{object_name}'.")
    except S3Error as e:
        print(f"Error uploading file '{file_path}' to bucket '{bucket_name}': {e}")
        raise Exception(f"Failed to upload file: {str(e)}")