####
## ETL files to merge chunked Yelp csv files to single csv
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

import os
import argparse
import pandas as pd
import json
import gc

from io import BytesIO
from pathlib import Path

from utils.etl_utils import ETLHelper
from utils.logging_utils import logger

class Mergecsv:

    def __init__(self, file_name:str, credentials: dict):
        try:
            self.helper            = ETLHelper()
            self.credentials       = credentials
            self.file_name         = file_name
            self.minio_path        = f"yelp_dataset_csv/{file_name}"
            self.bucket_staging    = credentials.get("MINIO_BUCKET_STAGING")
            self.minio_client      = self.helper.create_minio_conn(credentials)

        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise
    
    def merge(self):
        try:
            logger.info(f">> Fetching csv chunks from s3://{self.bucket_staging}/{self.minio_path}/")

            csv_files = [
                obj.object_name for obj in self.minio_client.list_objects(
                    self.bucket_staging,
                    prefix=self.minio_path,
                    recursive=True
                ) if obj.object_name.endswith(".csv") and not obj.object_name.endswith(f"{self.file_name}.csv")
            ]

            if not csv_files:
                raise ValueError(f"No csv files found to merge in {self.minio_path}")
            
            logger.info(f">> Found {len(csv_files)} csv chunks")

            df_all = []
            for file in sorted(csv_files):
                logger.info(f"--> Downloading {file}")
                response = self.minio_client.get_object(self.bucket_staging, file)
                buffer = BytesIO(response.read())
                df = pd.read_csv(buffer)
                df_all.append(df)

            merged_df = pd.concat(df_all, ignore_index=True)

            temp_output_path = f"/tmp/{self.file_name}.csv"
            merged_df.to_csv(temp_output_path, index=False)

            object_path = f"{self.minio_path}/yelp_academic_dataset_{self.file_name}.csv"
            self.helper.upload_file_to_minio(
                client=self.minio_client,
                bucket_name=self.bucket_staging,
                object_name=object_path,
                local_file_path=temp_output_path
            )

            logger.info(f">> Successfully uploaded merged csv to {object_path}")

            del merged_df
            gc.collect()
            os.remove(temp_output_path)

            # Delete all the temporary chunk files in MinIO
            for file in csv_files:
                self.minio_client.remove_object(self.bucket_staging, file)
                logger.info(f">> Deleted temporary file from MinIO: {file}")

        except Exception as e:
            logger.error(f"!! Failed to merge csv files: {e}")
            raise


def main():
    try:
        parser = argparse.ArgumentParser(description="Processing Yelp JSON to csv")
        parser.add_argument("--file_name", type=str, required=True, help="Yelp JSON file name")
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
        merger = Mergecsv(args.file_name, creds)
        merger.merge()
    except Exception as e:
        logger.error(f"!! Error running csv file processor - {e}")

if __name__ == "__main__":
    main()