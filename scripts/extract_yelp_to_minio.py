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
        self.helper            = ETLHelper()
        self.credentials       = credentials
        self.raw_tar_path    = f"./data/yelp_raw/{file_name}.tar"
        self.bucket_name       = credentials.get("MINIO_BUCKET_STAGING")
        self.minio_folder_path = f"{file_name}"


    def extract(self):
        logger.info(f">> Extracting Yelp Dataset - {self.raw_tar_path}...")
        
        try:
            with tarfile.open(self.raw_tar_path, 'r') as tar:

                # Getting Info
                members = tar.getmembers()
                total_size = sum([m.size for m in members if m.isfile()])
                logger.info(f">> Total files: {len(members)} | Total size: {total_size / (1024 ** 2):.2f} MB")

                for member in tqdm(members, desc=">> Uploading to MinIO..."):
                    if member.isdir():
                        continue
                    
                    fileobj = tar.extractfile(member)
                    if fileobj is None:
                        logger.warning(f">> Skip file: {member.name} - empty or corrupt")
                        continue

                    file_data = BytesIO(fileobj.read())
                    target_path = f"{self.minio_folder_path}/{Path(member.name).name}"

                    minio_client = self.helper.create_minio_conn(self.credentials)

                    minio_client.put_object(
                        bucket_name=self.bucket_name,
                        object_name=target_path,
                        data=file_data,
                        length=file_data.getbuffer().nbytes,
                        content_type="application/octet-stream"
                    )
                    logger.debug(f">> Uploaded: {target_path}")

        except Exception as e:
            logger.error(f"!! Failed extracting/uploading Yelp dataset: {e}")
            raise


def main():
    # Retrieving arguments
    try:
        parser = argparse.ArgumentParser(description="Extracting tar files")
        parser.add_argument("--file_name", type=str, required=True, help="tar file name")
        parser.add_argument("--creds", type=str, required=True, help="MinIO credentials")
        args = parser.parse_args()
    except Exception as e:
        logger.error(f"!! One of the arguments is empty! - {e}")

    # Preparing variables & creds
    try:
        creds         = json.loads(args.creds)
    except Exception as e:
        logger.error(f"!! Failed to parse JSON credentials: {e}")
        raise ValueError("!! Invalid credentials JSON format")

    # Running extractor
    try:
        extractor = ExtractData(args.file_name, creds)
        extractor.extract()
    except Exception as e:
        logger.error(f"!! Error running extractor - {e}")


if __name__ == "__main__":
    main()