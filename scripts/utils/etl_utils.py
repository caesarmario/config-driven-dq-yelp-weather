####
## Utilities for ETL process
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

import pandas as pd
import numpy as np
import json

from minio import Minio
from pathlib import Path
from io import BytesIO

from utils.logging_utils import logger
from utils.validation_utils import ValidationHelper

class ETLHelper:
    """
    Helper for common ETL tasks
    """
    def create_minio_conn(self, credentials):
        try:
            endpoint         = credentials["MINIO_ENDPOINT"]
            access_key       = credentials["MINIO_ACCESS_KEY"]
            secret_key       = credentials["MINIO_SECRET_KEY"]

            return Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=False
            )
        except Exception as e:
            logger.error(f"!! Failed to create MinIO connection: {e}")
    

    def upload_df_to_minio(self, df, bucket, path, minio_client):
        try:
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            minio_client.put_object(
                bucket_name=bucket,
                object_name=path,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            logger.info(f">> Uploaded to MinIO: {bucket}/{path}")
        except Exception as e:
            logger.error(f"!! Failed to upload parquet to MinIO: {e}")
            raise


    def read_csv(self, path):
        try:
            df = pd.read_csv(path)
            return df
        except Exception as e:
            logger.error(f"!! Failed to read csv: {e}")


    def load_schema_config(self, subfolder, config_name):
        try:
            with open(f'schema_config/{subfolder}/{config_name}_config.json', 'r') as file:
                return json.load(file)
        except Exception as e:
            logger.error(f"!! Failed to load schema config cols: {e}")


    def transform_to_date(self, value):
        try:
            return pd.to_datetime(str(value), format="%Y%m%d", errors="coerce")
        except Exception:
            return pd.NaT


    def to_numeric(self, value):
        try:
            return pd.to_numeric(value, errors='coerce')
        except Exception:
            return np.nan
        


    def apply_transformation(self, df, config):
        for col, meta in config.items():
            transform_func = meta.get("transformation")
            if transform_func == "transform_to_date":
                df[col] = df[meta["col_csv"]].apply(self.transform_to_date)
            elif transform_func == "to_numeric":
                df[col] = df[meta["col_csv"]].apply(self.to_numeric)
            else:
                df[col] = df[meta["col_csv"]]
        return df
    

    def apply_validation(self, df, config):
        df["is_valid"] = True
        df["error_reason"] = ""

        validation_helper = ValidationHelper()

        for col, meta in config.items():
            validations = meta.get("validation", [])
            if isinstance(validations, str):
                validations = [validations]

            for rule in validations:
                if hasattr(validation_helper, rule):
                    validation_func = getattr(validation_helper, rule)
                    try:
                        result = validation_func(df[col])
                        failed = ~result
                        df.loc[failed, "is_valid"] = False
                        df.loc[failed, "error_reason"] += f"{col}_{rule};"
                    except Exception as e:
                        logger.error(f"!! Validation rule failed: {col}.{rule} - {e}")
                else:
                    logger.warning(f">> Skipping unknown column validator: {rule}")

        return df