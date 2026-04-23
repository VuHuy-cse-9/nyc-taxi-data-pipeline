import os
import requests
from requests.exceptions import HTTPError
from minio.error import S3Error

from database.minio_client import create_minio_client, upload_file_to_minio
from configs.config import settings
from commons.constants import NYC_TAXI_BASE_URL, SOURCE2MINIO_FOLDER
from commons.logging import logger

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB
TIMEOUT = (10, 300)  # connect, read

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


def download_file(url: str, local_path: str):
    temp_path = local_path + ".part"
    downloaded_bytes = 0

    # Resume support
    if os.path.exists(temp_path):
        downloaded_bytes = os.path.getsize(temp_path)

    headers = {}
    if downloaded_bytes > 0:
        headers["Range"] = f"bytes={downloaded_bytes}-"
        logger.info(f"Resuming download at byte {downloaded_bytes}")

    with requests.get(url, stream=True, headers=headers, timeout=TIMEOUT) as r:
        r.raise_for_status()

        mode = "ab" if downloaded_bytes > 0 else "wb"
        total_size = int(r.headers.get("Content-Length", 0)) + downloaded_bytes

        with open(temp_path, mode) as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
                    downloaded_bytes += len(chunk)

                    if total_size > 0:
                        progress = downloaded_bytes / total_size * 100
                        logger.info(
                            f"Download progress: {downloaded_bytes / 1e6:.1f} MB "
                            f"({progress:.2f}%)"
                        )

    # Rename after successful download
    os.rename(temp_path, local_path)
    logger.info(f"✅ Download complete → {local_path}")
    return

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
        return 10

    try:
        download_file(url, filename)

        upload_file_to_minio(
            client,
            settings.datalake_bucket,
            filename,
            object_key,
        )
        logger.info(f"Uploaded to MinIO → {object_key}")

    except HTTPError as http_err:
        if http_err.response.status_code in [404, 403]:
            logger.warning(f"File not found at URL: {url}")
            return
        raise http_err
    finally:
        if os.path.exists(filename):
            os.remove(filename)
            logger.info(f"Removed local file {filename}")

    logger.info("✅ Done")

if __name__ == "__main__":
    main()
