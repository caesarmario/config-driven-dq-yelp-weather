####
## Script to load cleaned Parquet data into Postgres raw schema
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

import pandas as pd
import argparse
import json
import gc

from utils.etl_utils import ETLHelper
from utils.logging_utils import logger

class ParquetLoader:
    def __init__(self, folder_name: str, file_name: str, minio_creds: dict, db_creds: dict):
        try:
            self.helper            = ETLHelper()
            self.minio_creds       = minio_creds
            self.db_creds          = db_creds
            self.folder_name       = folder_name
            self.file_name         = file_name
            self.minio_path        = f"{folder_name}/{file_name}"
            self.bucket_staging    = minio_creds.get("MINIO_BUCKET_STAGING")
            self.minio_client      = self.helper.create_minio_conn(minio_creds)

        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise
    
    def loader(self):
        try:
            logger.info(f"Reading Parquet from {self.bucket_staging}, path {self.minio_path}")
            df = self.helper.read_parquet(self.bucket_staging, self.minio_creds, self.folder_name, self.file_name)
        except Exception as e:
            logger.error(f"!! Error reading Parquet from {self.bucket_staging} - {e}")

        try:
            conn = self.helper.create_postgre_conn(self.db_creds)
            with conn.cursor() as cursor:
                self.helper.check_and_create_table(conn, self.file_name, self.folder_name, df)
                self.helper.upsert_data_into_table(conn, self.file_name, self.folder_name, df)
            
            del df
            gc.collect()

        except Exception as e:
            logger.error(f"!! Error when merging data to {self.file_name} table - {e}")


def main():
    try:
        parser = argparse.ArgumentParser(description="Loading parquet file to db")
        parser.add_argument("--folder_name", type=str, required=True, help="Parquet folder path")
        parser.add_argument("--file_name", type=str, required=True, help="Parquet file name")
        parser.add_argument("--minio_creds", type=str, required=True, help="MinIO credentials")
        parser.add_argument("--db_creds", type=str, required=True, help="DB credentials")
        args = parser.parse_args()
    except Exception as e:
        logger.error(f"!! One of the arguments is empty! - {e}")

    try:
        minio_creds         = json.loads(args.minio_creds)
        db_creds            = json.loads(args.db_creds)
    except Exception as e:
        logger.error(f"!! Failed to parse JSON credentials: {e}")
        raise ValueError("!! Invalid credentials JSON format")

    try:
        merger = ParquetLoader(args.folder_name, args.file_name, minio_creds, db_creds)
        merger.loader()
    except Exception as e:
        logger.error(f"!! Error running csv file processor - {e}")

if __name__ == "__main__":
    main()