import os
import requests
from minio.error import S3Error

from database.minio_client import create_minio_client, upload_file_to_minio
from configs.config import settings
from commons.constants import NYC_TAXI_BASE_URL, SOURCE2MINIO_FOLDER
from commons.logging import logger

# ===== S3 CLIENT =====
client = create_minio_client()


def build_url(source: str, year: int, month: int):
    month_str = f"{month:02d}"
    filename = f"{source}_tripdata_{year}-{month_str}.parquet"
    return f"{NYC_TAXI_BASE_URL}/{filename}", filename


def object_exists(client, bucket: str, object_name: str) -> bool:
    try:
        client.stat_object(bucket, object_name)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        raise


def download_file(url, local_path):
    logger.info(f"Downloading {url}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024):
                if chunk:
                    f.write(chunk)


def main():
    url, filename = build_url(
        settings.datasource_to_download,
        settings.ingestion_year,
        settings.ingestion_month,
    )

    object_key = (
        f"{settings.dataset_name}/"
        f"{SOURCE2MINIO_FOLDER[settings.datasource_to_download]}/"
        f"{filename}"
    )

    # 🔍 Check if file already exists in MinIO
    if object_exists(client, settings.datalake_bucket, object_key):
        logger.info(f"🟡 File already exists in MinIO → skip download: {object_key}")
        return

    try:
        download_file(url, filename)

        upload_file_to_minio(
            client,
            settings.datalake_bucket,
            filename,
            object_key,
        )
        logger.info(f"Uploaded to MinIO → {object_key}")

    finally:
        if os.path.exists(filename):
            os.remove(filename)
            logger.info(f"Removed local file {filename}")

    logger.info("✅ Done")


if __name__ == "__main__":
    main()
