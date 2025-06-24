####
## Utilities for data monitoring process
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

import pandas as pd
import numpy as np
import psycopg2
import json

from pathlib import Path
from io import BytesIO
from datetime import datetime
from psycopg2 import OperationalError, sql
from psycopg2.extras import execute_values

from scripts.utils.logging_utils import logger

class MonitorHelper:

    # Monitoring functions
    def create_postgre_conn(self, db_creds):
        try:
            user           = db_creds["POSTGRES_USER"]
            password       = db_creds["POSTGRES_PASSWORD"]
            host           = db_creds["POSTGRES_HOST"]
            port           = db_creds["POSTGRES_PORT"]
            dbname         = db_creds["POSTGRES_DB"]

            conn = psycopg2.connect(
                database=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )

            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                logger.info(">> Successfully connected to PostgreSQL database.")
            return conn

        except Exception as e:
            logger.error(f"!! Creating postgres connection failed: {e}")
            raise


    def load_config(self, subfolder, config_name):
        with open(f'schema_config/{subfolder}_schema_config/{config_name}.json', 'r') as file:
            return json.load(file)
        
    
    def checking_date_format(self, col_name: str) -> dict:
        return {
            "passed_count": f"COUNT({col_name})",
            "failed_count": f"COUNT(*) - COUNT({col_name})",
            "total_records": "COUNT(*)",
            "failed_percentage": f"ROUND(100.0 * (COUNT(*) - COUNT({col_name})) / COUNT(*), 2)"
        }
    
    def checking_date_format_pd(self, col_name: str) -> dict:
        return self.checking_date_format(col_name)
    
    def checking_date_formats(self, col_name: str) -> dict:
        return self.checking_date_format(col_name)

    def not_null(self, col_name: str) -> dict:
        return {
            "passed_count": f"COUNT({col_name})",
            "failed_count": f"COUNT(*) - COUNT({col_name})",
            "total_records": "COUNT(*)",
            "failed_percentage": f"ROUND(100.0 * (COUNT(*) - COUNT({col_name})) / COUNT(*), 2)"
        }

    def fahrenheit_range(self, col_name: str) -> dict:
        return {
            "passed_count": f"COUNT(*) FILTER (WHERE {col_name} BETWEEN -50 AND 150)",
            "failed_count": f"COUNT(*) FILTER (WHERE NOT ({col_name} BETWEEN -50 AND 150))",
            "total_records": "COUNT(*)",
            "failed_percentage": f"ROUND(100.0 * COUNT(*) FILTER (WHERE NOT ({col_name} BETWEEN -50 AND 150)) / COUNT(*), 2)"
        }

    def precipitation_range(self, col_name: str) -> dict:
        return {
            "passed_count": f"COUNT(*) FILTER (WHERE {col_name} BETWEEN 0 AND 500)",
            "failed_count": f"COUNT(*) FILTER (WHERE NOT ({col_name} BETWEEN 0 AND 500))",
            "total_records": "COUNT(*)",
            "failed_percentage": f"ROUND(100.0 * COUNT(*) FILTER (WHERE NOT ({col_name} BETWEEN 0 AND 500)) / COUNT(*), 2)"
        }

    def negative_precipitation(self, col_name: str) -> dict:
        return {
            "passed_count": f"COUNT(*) FILTER (WHERE {col_name} >= 0)",
            "failed_count": f"COUNT(*) FILTER (WHERE {col_name} < 0)",
            "total_records": "COUNT(*)",
            "failed_percentage": f"ROUND(100.0 * COUNT(*) FILTER (WHERE {col_name} < 0) / COUNT(*), 2)"
        }

    def min_less_than_max(self, col_name: str) -> dict:
        return {
            "passed_count": "COUNT(*) FILTER (WHERE min <= max)",
            "failed_count": "COUNT(*) FILTER (WHERE min > max)",
            "total_records": "COUNT(*)",
            "failed_percentage": "ROUND(100.0 * COUNT(*) FILTER (WHERE min > max) / COUNT(*), 2)"
        }

    def normal_min_less_than_max(self, col_name: str) -> dict:
        return {
            "passed_count": "COUNT(*) FILTER (WHERE normal_min <= normal_max)",
            "failed_count": "COUNT(*) FILTER (WHERE normal_min > normal_max)",
            "total_records": "COUNT(*)",
            "failed_percentage": "ROUND(100.0 * COUNT(*) FILTER (WHERE normal_min > normal_max) / COUNT(*), 2)"
        }
    
    def validate_latitude(self, series):
        return series.apply(lambda x: -90 <= x <= 90 if pd.notnull(x) else False)
        
    def validate_longitude(self, series):
        return series.apply(lambda x: -180 <= x <= 180 if pd.notnull(x) else False)
    
    def stars_range_check(self, series):
        return series.apply(lambda x: 0 <= x <= 5 if pd.notnull(x) else False)
    
    def positive_integer(self, col_name: str) -> dict:
        return {
            "passed_count": f"COUNT(*) FILTER (WHERE {col_name} > 0)",
            "failed_count": f"COUNT(*) FILTER (WHERE {col_name} <= 0 OR {col_name} IS NULL)",
            "total_records": "COUNT(*)",
            "failed_percentage": f"""
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE {col_name} <= 0 OR {col_name} IS NULL) / COUNT(*),
                    2
                )
            """
        }
    
    def is_binary(self, col_name: str) -> dict:
        return {
            "passed_count": f"COUNT(*) FILTER (WHERE {col_name} IN (0,1))",
            "failed_count": f"COUNT(*) FILTER (WHERE {col_name} NOT IN (0,1) OR {col_name} IS NULL)",
            "total_records": "COUNT(*)",
            "failed_percentage": f"""
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE {col_name} NOT IN (0,1) OR {col_name} IS NULL) / COUNT(*),
                    2
                )
            """
        }
    
    def temperature_range(self, col_name: str) -> dict:
        return {
            "passed_count": f"COUNT(*) FILTER (WHERE {col_name} BETWEEN -100 AND 160)",
            "failed_count": f"COUNT(*) FILTER (WHERE {col_name} NOT BETWEEN -100 AND 160 OR {col_name} IS NULL)",
            "total_records": "COUNT(*)",
            "failed_percentage": f"""
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE {col_name} NOT BETWEEN -100 AND 160 OR {col_name} IS NULL) / COUNT(*),
                    2
                )
            """
        }
    
    def validate_compliment_non_negative(self, col_name: str) -> dict:
        return {
            "passed_count": f"COUNT(*) FILTER (WHERE {col_name} >= 0)",
            "failed_count": f"COUNT(*) FILTER (WHERE {col_name} < 0)",
            "total_records": "COUNT(*)",
            "failed_percentage": f"ROUND(100.0 * COUNT(*) FILTER (WHERE {col_name} < 0) / COUNT(*), 2)"
        }
    
    def text_length_nonzero(self, col_name: str) -> dict:
        return {
            "passed_count": f"COUNT(*) FILTER (WHERE LENGTH(TRIM({col_name})) > 0)",
            "failed_count": f"COUNT(*) FILTER (WHERE LENGTH(TRIM({col_name})) = 0 OR {col_name} IS NULL)",
            "total_records": "COUNT(*)",
            "failed_percentage": f"ROUND(100.0 * COUNT(*) FILTER (WHERE LENGTH(TRIM({col_name})) = 0 OR {col_name} IS NULL) / COUNT(*), 2)"
        }

    def valid_compliment_count_type(self, col_name: str) -> dict:
        return self.validate_compliment_non_negative(col_name)
    
    def non_negative(self, col_name: str) -> dict:
        return {
            "passed_count": f"COUNT(*) FILTER (WHERE {col_name} >= 0)",
            "failed_count": f"COUNT(*) FILTER (WHERE {col_name} < 0)",
            "total_records": "COUNT(*)",
            "failed_percentage": f"ROUND(100.0 * COUNT(*) FILTER (WHERE {col_name} < 0) / COUNT(*), 2)"
        }
    
    def non_negative_pd(self, col_name: str) -> dict:
        return self.non_negative(col_name)
    
    def non_negatives(self, col_name: str) -> dict:
        return self.non_negative(col_name)
    
    def extract_monitoring_metrics(self, conn, schema: str, table: str, config: dict):
        exec_dt = datetime.now()
        results = []

        with conn.cursor() as cursor:
            for col, meta in config.items():
                validations = meta.get("validation", [])

                # Normalize to list if string
                if validations is None:
                    logger.info(f">> No validation rules found for column '{col}', skipping.")
                    continue
                if isinstance(validations, str):
                    validations = [validations]

                for rule in validations:
                    if not hasattr(self, rule):
                        logger.warning(f">> Unknown rule '{rule}' for column '{col}'. Skipping.")
                        continue

                    try:
                        rule_func = getattr(self, rule)
                        expressions = rule_func(col)

                        query = f"""
                            SELECT
                                {expressions['passed_count']} AS passed,
                                {expressions['failed_count']} AS failed,
                                {expressions['total_records']} AS total
                            FROM {schema}.{table}
                        """
                        cursor.execute(query)
                        passed, failed, total = cursor.fetchone()
                        failed_pct = round(100.0 * failed / total, 2) if total else None

                        results.append({
                            "table_name": table,
                            "column_name": col,
                            "validation_rule": rule,
                            "passed_count": passed,
                            "failed_count": failed,
                            "total_records": total,
                            "failed_percentage": failed_pct,
                            "execution_dt": exec_dt,
                            "metrics": self.mapping_rule(rule),
                        })
                    except Exception as e:
                        logger.warning(f">> Failed rule '{rule}' on column '{col}': {e}")
                        continue

        return results
    

    def insert_monitoring_results(self, conn, schema: str, table: str, results: list):
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

                full_table = f"{schema}.{table}_monitoring"
                create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {full_table} (
                        table_name TEXT,
                        column_name TEXT,
                        validation_rule TEXT,
                        passed_count INT,
                        failed_count INT,
                        total_records INT,
                        failed_percentage FLOAT,
                        execution_dt TIMESTAMP,
                        metrics TEXT
                    );
                """
                cursor.execute(create_sql)

                values = [
                    (
                        row["table_name"],
                        row["column_name"],
                        row["validation_rule"],
                        row["passed_count"],
                        row["failed_count"],
                        row["total_records"],
                        row["failed_percentage"],
                        row["execution_dt"],
                        row["metrics"]
                    )
                    for row in results
                ]
                insert_sql = f"""
                    INSERT INTO {full_table} (
                        table_name, column_name, validation_rule,
                        passed_count, failed_count, total_records,
                        failed_percentage, execution_dt, metrics
                    ) VALUES %s
                """
                execute_values(cursor, insert_sql, values)
                conn.commit()
                logger.info(f">> Inserted {len(values)} monitoring records into {full_table}")

        except Exception as e:
            logger.error(f"!! Failed inserting monitoring results: {e}")
            raise

    def mapping_rule(self, rule: str) -> str:
        mapping = {
            "not_null": "Completeness",
            "checking_date_format": "Validity",
            "fahrenheit_range": "Validity",
            "temperature_range": "Validity",
            "precipitation_range": "Validity",
            "negative_precipitation": "Validity",
            "min_less_than_max": "Accuracy",
            "normal_min_less_than_max": "Accuracy",
            "validate_latitude": "Validity",
            "validate_longitude": "Validity",
            "stars_range_check": "Validity",
            "validate_compliment_non_negative": "Validity",
            "valid_compliment_count_type": "Validity",
            "text_length_nonzero": "Completeness",
            "non_negative": "Validity",
            "positive_integer": "Validity",
            "is_binary": "Validity"
        }
        return mapping.get(rule, "Unknown")
