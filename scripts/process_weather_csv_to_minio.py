####
## ETL files to process weather csv files and upload to minio
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

# --- Imports libraries
import argparse
import json

from utils.logging_utils import logger
from utils.etl_utils import ETLHelper
from utils.validation_utils import ValidationHelper

class ProcessFlatFile:

    def __init__(self, file_name:str, credentials: dict):
        try:
            self.helper            = ETLHelper()
            self.validation_helper = ValidationHelper()
            self.credentials       = credentials
            self.file_name         = file_name
            self.local_csv_path      = f"./data/weather_raw/{file_name}.csv"
            self.minio_folder_path = f"weather/{file_name}"
            self.config            = self.helper.load_schema_config("weather_schema_config", file_name)
            self.bucket_staging    = credentials.get("MINIO_BUCKET_STAGING")
            self.bucket_rejected   = credentials.get("MINIO_BUCKET_REJECTED")
            self.minio_client      = self.helper.create_minio_conn(credentials)
        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise

    def process(self):
        logger.info(f">> Processing CSV - {self.local_csv_path}...")

        try:
            # Load CSV
            df = self.helper.read_csv(self.local_csv_path)

            # Perform transformation
            df = self.helper.apply_transformation(df, self.config)

            # Perform soft-validation
            df = self.helper.apply_validation(df, self.config)

            if "min" in df.columns and "max" in df.columns:
                try:
                    result = df.apply(self.validation_helper.min_less_than_max, axis=1)
                    failed = ~result
                    df.loc[failed, "is_valid"] = False
                    df.loc[failed, "error_reason"] += "min_less_than_max;"
                except Exception as e:
                    logger.error(f"!! Cross-column validation min < max failed: {e}")

            if "normal_min" in df.columns and "normal_max" in df.columns:
                try:
                    result = df.apply(self.validation_helper.normal_min_less_than_max, axis=1)
                    failed = ~result
                    df.loc[failed, "is_valid"] = False
                    df.loc[failed, "error_reason"] += "normal_min_less_than_max;"
                except Exception as e:
                    logger.error(f"!! Cross-column validation normal_min < normal_max failed: {e}")

            # Split valid and rejected
            df_valid = df[df["is_valid"] == True].drop(columns=["is_valid", "error_reason"])
            df_rejected = df[df["is_valid"] == False]

            if not df_valid.empty:
                logger.info(f">> Uploading valid rows: {len(df_valid)}")
                self.helper.upload_df_to_minio(
                    df_valid,
                    bucket=self.bucket_staging,
                    path=f"{self.minio_folder_path}/{self.file_name}_valid.parquet",
                    minio_client=self.minio_client
                )
                
            if not df_rejected.empty:
                logger.info(f">> Uploading rejected rows: {len(df_rejected)}")
                self.helper.upload_df_to_minio(
                    df_rejected,
                    bucket=self.bucket_rejected,
                    path=f"{self.minio_folder_path}/{self.file_name}_rejected.parquet",
                    minio_client=self.minio_client
                )

            logger.info(">> Finished processing weather data.")

        except Exception as e:
            logger.error(f"!! Failed to process CSV {self.local_csv_path}: {e}")
            raise

def main():
    try:
        parser = argparse.ArgumentParser(description="Processing weather CSV files")
        parser.add_argument("--file_name", type=str, required=True, help="CSV file name")
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
        extractor = ProcessFlatFile(args.file_name, creds)
        extractor.process()
    except Exception as e:
        logger.error(f"!! Error running csv file processor - {e}")


if __name__ == "__main__":
    main()