####
## ETL files to process Yelp JSON files to parquet
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

# --- Imports libraries
import argparse
import json
import pandas as pd
import os
import shutil
import gc

from pathlib import Path

from utils.etl_utils import ETLHelper
from utils.validation_utils import ValidationHelper
from utils.logging_utils import logger

class ProcessYelpJson:

    def __init__(self, file_name:str, credentials: dict):
        try:
            self.helper            = ETLHelper()
            self.validation_helper = ValidationHelper()
            self.credentials       = credentials
            self.file_name         = f"yelp_academic_dataset_{file_name}.json"
            self.minio_input_path  = f"yelp_dataset_raw/{self.file_name}"
            self.minio_output_path = f"yelp_dataset/{file_name}"
            self.config            = self.helper.load_schema_config("yelp_schema_config", file_name)
            self.bucket_staging    = credentials.get("MINIO_BUCKET_STAGING")
            self.bucket_rejected   = credentials.get("MINIO_BUCKET_REJECTED")
            self.minio_client      = self.helper.create_minio_conn(credentials)
            self.chunk_size        = 300000
        
        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise

    def process(self):
        logger.info(f">> Processing JSON {self.file_name}...")

        try:
            self.helper.delete_parquet_chunks(
                client=self.minio_client,
                bucket_name=self.bucket_staging,
                prefix=self.minio_output_path
            )

            local_json_path = self.helper.download_file_from_minio(
                client=self.minio_client,
                bucket_name=self.bucket_staging,
                object_name=self.minio_input_path
            )

            output_dir = Path("/tmp") / self.minio_output_path
            output_dir.mkdir(parents=True, exist_ok=True)

            chunk_id = 0

            with pd.read_json(local_json_path, lines=True, chunksize=self.chunk_size) as reader:
                for chunk in reader:
                    logger.info(f"-- Processing chunk {chunk_id}...")
                    try:
                        validated_df, invalid_df = self.validation_helper.validate_chunk(chunk, self.config)

                        output_file_name = f"{self.file_name.replace('.json', '')}_{chunk_id}.parquet"
                        local_output_path = output_dir / output_file_name
                        validated_df.to_parquet(local_output_path, index=False)

                        self.helper.upload_file_to_minio(
                            client=self.minio_client,
                            bucket_name=self.bucket_staging,
                            object_name=f"{self.minio_output_path}/{output_file_name}",
                            local_file_path=str(local_output_path)
                        )

                        del validated_df
                        gc.collect()

                        if not invalid_df.empty:
                            logger.info(f">> Chunk {chunk_id} has {len(invalid_df)} rejected rows")
                            
                            rejected_file_name = f"{self.file_name.replace('.json', '')}_rejected_{chunk_id}.parquet"
                            local_reject_path = output_dir / rejected_file_name
                            invalid_df.to_parquet(local_reject_path, index=False)

                            self.helper.upload_file_to_minio(
                                client=self.minio_client,
                                bucket_name=self.bucket_rejected,
                                object_name=f"{self.minio_output_path}/{rejected_file_name}",
                                local_file_path=str(local_reject_path)
                            )

                        del invalid_df
                        gc.collect()

                    except Exception as e:
                        logger.error(f"!! Failed to process df batch {chunk_id}: {e}")

                    chunk_id += 1

            logger.info(f">> Successfully processed all chunks into {output_dir}")

            # Cleanup local files
            if os.path.exists(local_json_path):
                os.remove(local_json_path)
                logger.info(f">> Removed local JSON file: {local_json_path}")

            if output_dir.exists():
                shutil.rmtree(output_dir)
                logger.info(f">> Removed local Parquet output directory: {output_dir}")

        except Exception as e:
            logger.error(f"!! Failed to process JSON {self.file_name}: {e}")
            raise


def main():
    try:
        parser = argparse.ArgumentParser(description="Processing Yelp JSON to parquet")
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
        extractor = ProcessYelpJson(args.file_name, creds)
        extractor.process()
    except Exception as e:
        logger.error(f"!! Error running csv file processor - {e}")

if __name__ == "__main__":
    main()