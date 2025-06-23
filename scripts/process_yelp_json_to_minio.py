####
## ETL files to process Yelp JSON files to parquet
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

# --- Imports libraries
import argparse
import json

from pyspark.sql import SparkSession

from utils.etl_utils import ETLHelper
from utils.validation_utils import ValidationHelper
from utils.logging_utils import logger

class ProcessYelpJsonSpark:

    def __init__(self, file_name:str, credentials: dict):
        try:
            self.helper            = ETLHelper()
            self.validation_helper = ValidationHelper()
            self.credentials       = credentials
            self.file_name         = f"yelp_academic_dataset_{file_name}.json"
            self.minio_input_path  = f"yelp_dataset_raw/{self.file_name}"
            self.minio_output_path = f"yelp_dataset/{self.file_name}"
            self.config            = self.helper.load_schema_config("yelp_schema_config", file_name)
            self.bucket_staging    = credentials.get("MINIO_BUCKET_STAGING")
            self.bucket_rejected   = credentials.get("MINIO_BUCKET_REJECTED")
            self.minio_client      = self.helper.create_minio_conn(credentials)

            self.spark = SparkSession.builder \
                .appName("YelpJsonToMinio") \
                .master("spark://spark-master:7077") \
                .getOrCreate()
            
        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise

    def process(self):
        logger.info(f">> Processing JSON {self.file_name}...")

        try:
            json_path = f"s3a://{self.bucket_staging}/{self.minio_input_path}"
            logger.info(f">> Reading JSON from {json_path}")
            df = self.spark.read.json(json_path)
            df.show(n=10)

        except Exception as e:
            logger.error(f"!! Failed to process JSON {self.file_name}: {e}")
            raise