####
## Utilities for ETL process
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

from minio import Minio
from pathlib import Path

from utils.logging_utils import logger

class ETLHelper:
    """
    Helper for common ETL tasks
    """
    def create_minio_conn(self, credentials):
        endpoint         = credentials["MINIO_ENDPOINT"]
        access_key       = credentials["MINIO_ACCESS_KEY"]
        secret_key       = credentials["MINIO_SECRET_KEY"]

        return Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
    

    def upload_member(self, minio_client, bucket_name, folder_prefix, tar_file, member):
        try:
            fileobj = tar_file.extractfile(member)
            if fileobj is None:
                logger.warning(f">> Skip file: {member.name} - empty or corrupt")
                return

            target_path = f"{folder_prefix}/{Path(member.name).name}"

            minio_client.put_object(
                bucket_name=bucket_name,
                object_name=target_path,
                data=fileobj,
                length=member.size,
                content_type="application/octet-stream"
            )
            logger.debug(f">> Uploaded: {target_path}")
        except Exception as e:
            logger.error(f"!! Failed upload: {member.name} - {e}")