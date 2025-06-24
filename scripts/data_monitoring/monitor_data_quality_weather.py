####
## Weather data monitoring script
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

import argparse
import json

from scripts.utils.logging_utils import logger
from scripts.utils.monitor_utils import MonitorHelper

class WeatherMonitoring:

    def __init__(self, table_name: str, db_creds: dict):
        try:
            self.helper         = MonitorHelper()
            self.db_creds       = db_creds
            self.table_name     = table_name
            self.weather_schema = "weather"
            self.target_schema  = "data_monitoring"
        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise


    def monitor(self):
        try:
            conn = self.helper.create_postgre_conn(self.db_creds)
            config = self.helper.load_config(self.weather_schema, f"{self.table_name}_config")

            metrics = self.helper.extract_monitoring_metrics(
                conn=conn,
                schema=self.weather_schema,
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

            logger.info(f">> Successfully inserted monitoring results for {self.weather_schema}.{self.table_name}")

        except Exception as e:
            logger.error(f"!! Monitoring failed for {self.table_name}: {e}")
            raise


def main():
    try:
        parser = argparse.ArgumentParser(description="Monitoring results")
        parser.add_argument("--table_name", type=str, required=True, help="Table name")
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
        gen = WeatherMonitoring(args.table_name, db_creds)
        gen.monitor()
    except Exception as e:
        logger.error(f"!! Error running file processor - {e}")

if __name__ == "__main__":
    main()

# ########
# def extract_monitoring_metrics(table_name: str, config: dict) -> list:
#     results = []
#     exec_dt = datetime.now()
#     for col, meta in config.items():
#         validations = meta.get("validation", [])
#         if isinstance(validations, str):
#             validations = [validations]

#         for rule in validations:
#             # Initialize default result
#             metric_result = {
#                 "table_name": table_name,
#                 "column_name": col,
#                 "validation_rule": rule,
#                 "passed_count": None,
#                 "failed_count": None,
#                 "total_records": None,
#                 "failed_percentage": None,
#                 "execution_dt": exec_dt,
#             }

#             # Sample logic for metrics (can be expanded)
#             if rule == "not_null":
#                 metric_result.update({
#                     "passed_count": f"COUNT({col})",
#                     "failed_count": f"COUNT(*) - COUNT({col})",
#                     "total_records": "COUNT(*)",
#                     "failed_percentage": f"ROUND(100.0 * (COUNT(*) - COUNT({col})) / COUNT(*), 2)"
#                 })
#             elif rule == "fahrenheit_range":
#                 metric_result.update({
#                     "passed_count": f"COUNT(*) FILTER (WHERE {col} BETWEEN -50 AND 150)",
#                     "failed_count": f"COUNT(*) FILTER (WHERE NOT ({col} BETWEEN -50 AND 150))",
#                     "total_records": "COUNT(*)",
#                     "failed_percentage": f"ROUND(100.0 * COUNT(*) FILTER (WHERE NOT ({col} BETWEEN -50 AND 150)) / COUNT(*), 2)"
#                 })
#             elif rule == "precipitation_range":
#                 metric_result.update({
#                     "passed_count": f"COUNT(*) FILTER (WHERE {col} BETWEEN 0 AND 500)",
#                     "failed_count": f"COUNT(*) FILTER (WHERE NOT ({col} BETWEEN 0 AND 500))",
#                     "total_records": "COUNT(*)",
#                     "failed_percentage": f"ROUND(100.0 * COUNT(*) FILTER (WHERE NOT ({col} BETWEEN 0 AND 500)) / COUNT(*), 2)"
#                 })

#             results.append(metric_result)
#     return results


# # Load all config files for `weather` schema
# schema_folder = Path("schema_config/weather_schema_config")
# tables = ["precipitation", "temperature"]
# all_metrics = []

# for table in tables:
#     with open(schema_folder / f"{table}_config.json") as f:
#         config = json.load(f)
#         metrics = extract_monitoring_metrics(table, config)
#         all_metrics.extend(metrics)

# # Convert into a monitoring DataFrame
# df_monitor = pd.DataFrame(all_metrics)
# import ace_tools as tools; tools.display_dataframe_to_user(name="Weather Data Quality Monitoring Metrics", dataframe=df_monitor)
