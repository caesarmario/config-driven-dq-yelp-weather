####
## ETL files to extract Yelp tar files to MinIO
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

# --- Imports libraries
import tarfile
import argparse
import json

from io import BytesIO
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.logging_utils import logger
from utils.etl_utils import ETLHelper

class ExtractData:

    def __init__(self, file_name:str, credentials: dict):
        try:
            self.helper            = ETLHelper()
            self.credentials       = credentials
            self.raw_tar_path      = f"./data/yelp_raw/{file_name}.tar"
            self.bucket_name       = credentials.get("MINIO_BUCKET_STAGING")
            self.minio_folder_path = f"{file_name}_raw"
            self.minio_client      = self.helper.create_minio_conn(credentials)
            logger.info(">> Configuration for loaded successfully! ")
        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise


    def _upload_member(self, member_bytes: bytes, target_path: str):
        try:
            file_data = BytesIO(member_bytes)
            self.minio_client.put_object(
                bucket_name=self.bucket_name,
                object_name=target_path,
                data=file_data,
                length=file_data.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            logger.debug(f">> Uploaded: {target_path}")
        except Exception as e:
            logger.error(f"!! Failed to upload {target_path}: {e}")

    def extract(self):
        logger.info(f">> Extracting Yelp Dataset - {self.raw_tar_path}...")

        try:
            with tarfile.open(self.raw_tar_path, 'r') as tar:
                members = [m for m in tar.getmembers() if m.isfile()]
                total_size = sum(m.size for m in members)
                logger.info(f">> Total files: {len(members)} | Total size: {total_size / (1024 ** 2):.2f} MB")

                tasks = []
                with ThreadPoolExecutor(max_workers=8) as executor:
                    for member in tqdm(members, desc=">> Scheduling uploads"):
                        try:
                            fileobj = tar.extractfile(member)
                            if fileobj is None:
                                logger.warning(f">> Skip file: {member.name} - empty or corrupt")
                                continue

                            member_bytes = fileobj.read()
                            target_path = f"{self.minio_folder_path}/{Path(member.name).name}"

                            tasks.append(executor.submit(self._upload_member, member_bytes, target_path))
                        except Exception as e:
                            logger.warning(f">> Failed to extract {member.name}: {e}")
                            continue

                    for future in tqdm(as_completed(tasks), total=len(tasks), desc=">> Uploading to MinIO..."):
                        _ = future.result()

                logger.info(">> Extraction and upload completed successfully.")

        except Exception as e:
            logger.error(f"!! Failed extracting/uploading Yelp dataset: {e}")
            raise


def main():
    try:
        parser = argparse.ArgumentParser(description="Extracting tar files")
        parser.add_argument("--file_name", type=str, required=True, help="tar file name")
        parser.add_argument("--creds", type=str, required=True, help="MinIO credentials")
        args = parser.parse_args()
    except Exception as e:
        logger.error(f"!! One of the arguments is empty! - {e}")

    try:
        creds         = json.loads(args.creds)
    except Exception as e:
        logger.error(f"!! Failed to parse JSON credentials: {e}")
        raise ValueError("!! Invalid credentials JSON format")

    try:
        extractor = ExtractData(args.file_name, creds)
        extractor.extract()
    except Exception as e:
        logger.error(f"!! Error running extractor - {e}")


if __name__ == "__main__":
    main()