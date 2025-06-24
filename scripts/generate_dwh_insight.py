####
## Script to generate insight in dwh
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

import argparse
import json

from utils.etl_utils import ETLHelper
from utils.logging_utils import logger

class InsightGenerator:

    def __init__(self, sql_file_name: str, db_creds: dict):
        try:
            self.helper        = ETLHelper()
            self.db_creds      = db_creds
            self.sql_file_name = f"{sql_file_name}.sql"
            self.table_name    = sql_file_name
            self.dwh_schema    = "dwh"
            self.sql_file_path = f"scripts/sql/{self.sql_file_name}"

        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise

    def generator(self):
        try:
            logger.info(f">> Generating insights for {self.sql_file_name}...")
            conn = self.helper.create_postgre_conn(self.db_creds)
            self.helper.execute_sql_to_fact_table(conn, self.dwh_schema, self.table_name, self.sql_file_path)

            logger.info(f">> Insight generation complete for {self.dwh_schema}.{self.table_name}")

        except Exception as e:
            logger.error(f"!! Error when running the script {self.sql_file_name}: {e}")
            raise


def main():
    try:
        parser = argparse.ArgumentParser(description="Generating insight to dwh")
        parser.add_argument("--sql_file_name", type=str, required=True, help="SQL file name")
        parser.add_argument("--db_creds", type=str, required=True, help="DB credentials")
        args = parser.parse_args()
    except Exception as e:
        logger.error(f"!! One of the arguments is empty! - {e}")

    try:
        db_creds            = json.loads(args.db_creds)
    except Exception as e:
        logger.error(f"!! Failed to parse JSON credentials: {e}")
        raise ValueError("!! Invalid credentials JSON format")

    try:
        gen = InsightGenerator(args.sql_file_name, db_creds)
        gen.generator()
    except Exception as e:
        logger.error(f"!! Error running file processor - {e}")

if __name__ == "__main__":
    main()