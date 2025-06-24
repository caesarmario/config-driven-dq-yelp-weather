####
## Utils file for validation purposes
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
import pandas as pd
import numpy as np
import json

from datetime import datetime, timedelta
from utils.logging_utils import logger

class ValidationHelper:

    # Validation Function
    def checking_date_format(self, series):
        return series.notna()
        


    def not_null(self, series):
        return series.notna()
    


    def precipitation_range(self, series):
        return series.between(0, 500)
    

    def temperature_range(self, series: pd.Series):
        return series.fillna(0).between(-50, 150)


    def negative_precipitation(self, series):
        return series >= 0
    

    def fahrenheit_range(self, series):
        return series.between(-100, 150)
    

    def min_less_than_max(self, row):
        try:
            min_val = pd.to_numeric(row["min"], errors="coerce")
            max_val = pd.to_numeric(row["max"], errors="coerce")
            if pd.isnull(min_val) or pd.isnull(max_val):
                return False
            return min_val <= max_val
        except Exception as e:
            logger.error(f"!! Failed min < max validation: {e}")
            return False


    def normal_min_less_than_max(self, row):
        try:
            min_val = pd.to_numeric(row["normal_min"], errors="coerce")
            max_val = pd.to_numeric(row["normal_max"], errors="coerce")
            if pd.isnull(min_val) or pd.isnull(max_val):
                return False
            return min_val <= max_val
        except Exception as e:
            logger.error(f"!! Failed normal_min < normal_max validation: {e}")
            return False


    def non_negative(self, series):
        try:
            return series.fillna(0) >= 0
        except Exception as e:
            logger.error(f"!! Failed non_negative validation: {e}")
            return pd.Series([False] * len(series))

    def positive_integer(self, series: pd.Series):
        return pd.to_numeric(series, errors='coerce').fillna(0).astype(int) > 0
    

    def validate_latitude(self, series):
        return series.between(-90, 90)


    def validate_longitude(self, series):
        return series.between(-180, 180)


    def stars_range_check(self, series):
        return series.between(0, 5)

    def boolean_check(self, series):
        return series.isin([0, 1])


    def non_negative_pd(self, series: pd.Series) -> pd.Series:
        return series >= 0
    

    def non_negatives(self, series: pd.Series):
        return pd.to_numeric(series, errors='coerce').fillna(0) >= 0


    def is_binary(self, series: pd.Series):
        return series.isin([0, 1])


    def checking_date_format_pd(self, series: pd.Series) -> pd.Series:
        return series.apply(lambda x: isinstance(x, pd.Timestamp))
    

    def checking_date_formats(self, series: pd.Series):
        return pd.to_datetime(series, errors='coerce').notna()
    

    def json_dumps_if_dict(self, series: pd.Series) -> pd.Series:
        return series.apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)


    def split_and_normalize_datetime_list(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        try:
            # Split string by comma, strip whitespace, convert to datetime
            df[column] = df[column].apply(lambda x: [d.strip() for d in str(x).split(',')] if pd.notnull(x) else [])
            df = df.explode(column)
            df[column] = pd.to_datetime(df[column], errors='coerce')
            return df
        except Exception as e:
            logger.error(f"!! Failed to normalize datetime list in column '{column}': {e}")
            raise
        

    def validate_chunk(self, df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
        df_validated = df.copy(deep=True)
        df_invalid = pd.DataFrame()

        for col, rules in config.items():
            col_name = rules.get("col_csv")
            validation = rules.get("validation")
            transformation = rules.get("transformation")

            if col_name not in df_validated.columns:
                logger.warning(f"Column {col_name} not found in data")
                continue

            # Transformation
            if transformation == "split_and_normalize_datetime_list":
                df_validated = self.split_and_normalize_datetime_list(df_validated, col_name)
            elif transformation == "transform_to_datetime":
                df_validated[col_name] = pd.to_datetime(df_validated[col_name], errors='coerce')
            elif transformation == "to_numeric":
                df_validated[col_name] = pd.to_numeric(df_validated[col_name], errors='coerce')

            # Validation
            if validation:
                if isinstance(validation, str):
                    validation = [validation]
                for v in validation:
                    validation_func = getattr(self, v, None)
                    if callable(validation_func):
                        try:
                            mask = validation_func(df_validated[col_name])
                            df_invalid = pd.concat([df_invalid, df_validated[~mask]])
                            df_validated = df_validated[mask]
                        except Exception as e:
                            logger.error(f"!! Failed to run validation '{v}' for column '{col_name}': {e}")
                    else:
                        logger.warning(f"Validation function '{v}' not implemented")

        hashable_invalid = df_invalid.copy()
        for col in hashable_invalid.columns:
            if hashable_invalid[col].apply(lambda x: isinstance(x, (dict, list))).astype(bool).any():
                hashable_invalid.drop(columns=[col], inplace=True)

        return df_validated, hashable_invalid.drop_duplicates(ignore_index=True).reset_index(drop=True)