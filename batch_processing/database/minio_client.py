from minio import Minio
from minio.error import S3Error
from configs.config import settings
from commons.logging import logger


def create_minio_client():
    minio_client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure
    )

    try:
        minio_client.list_buckets()
        logger.info("Connected to Minio successfully.")
        return minio_client

    except S3Error as e:
        logger.error(f"Error connecting to Minio: {e}")
        raise Exception(f"Failed to connect to Minio: {str(e)}")


def create_bucket_if_not_exists(minio_client, bucket_name):
    # Create Delta Lake bucket if it doesn't exist
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")
    except S3Error as e:
        logger.error(f"Error creating bucket '{bucket_name}': {e}")
        raise Exception(f"Failed to create bucket: {str(e)}")

def upload_file_to_minio(minio_client: Minio, bucket_name, file_path, object_name):
    try:
        # This will fetch metadata without downloading the object
        try:
            minio_client.stat_object(bucket_name, object_name)
        except S3Error as e:
            if e.code == "NoSuchKey":
                minio_client.fput_object(
                        bucket_name=bucket_name,
                        object_name=object_name,
                        file_path=file_path
                    )
            
                logger.info(f"File '{file_path}' uploaded to bucket '{bucket_name}' as '{object_name}'.")
            else:
                raise e
    except S3Error as e:
        logger.error(f"Error uploading file '{file_path}' to bucket '{bucket_name}': {e}")
        raise Exception(f"Failed to upload file: {str(e)}")