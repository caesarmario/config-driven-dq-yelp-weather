####
## L1 data monitoring script
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

import argparse
import json

from scripts.utils.logging_utils import logger
from scripts.utils.monitor_utils import MonitorHelper

class L1Monitoring:

    def __init__(self, table_name: str, schema_name: str, db_creds: dict):
        try:
            self.helper         = MonitorHelper()
            self.db_creds       = db_creds
            self.table_name     = table_name
            self.schema_name    = schema_name
            self.target_schema  = "data_monitoring"
        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise


    def monitor(self):
        try:
            conn = self.helper.create_postgre_conn(self.db_creds)
            config = self.helper.load_config(self.schema_name, f"{self.table_name}_config")

            metrics = self.helper.extract_monitoring_metrics(
                conn=conn,
                schema=self.schema_name,
                table=self.table_name,
                config=config
            )

            if not metrics:
                logger.warning(f">> No metrics generated for {self.table_name}.")
                return

            self.helper.insert_monitoring_results(
                conn=conn,
                schema=self.target_schema,
                table=self.table_name,
                results=metrics
            )

            logger.info(f">> Successfully inserted monitoring results for {self.schema_name}.{self.table_name}")

        except Exception as e:
            logger.error(f"!! Monitoring failed for {self.table_name}: {e}")
            raise


def main():
    try:
        parser = argparse.ArgumentParser(description="Monitoring results")
        parser.add_argument("--table_name", type=str, required=True, help="Table name")
        parser.add_argument("--schema_name", type=str, required=True, help="Schema name")
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
        monitor = L1Monitoring(args.table_name, args.schema_name, db_creds)
        monitor.monitor()
    except Exception as e:
        logger.error(f"!! Error running file processor - {e}")

if __name__ == "__main__":
    main()
