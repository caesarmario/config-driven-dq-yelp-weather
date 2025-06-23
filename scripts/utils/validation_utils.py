####
## Utils file for validation purposes
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
import pandas as pd
import numpy as np

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
    
    def negative_precipitation(self, series):
        return series >= 0
    
    def fahrenheit_range(self, series):
        return series.between(-100, 150)
    
    def min_less_than_max(self, row):
        try:
            return row["min"] <= row["max"]
        except Exception as e:
            logger.error(f"!! Failed min < max validation: {e}")
            return False

    def normal_min_less_than_max(self, row):
        try:
            return row["normal_min"] <= row["normal_max"]
        except Exception as e:
            logger.error(f"!! Failed normal_min < normal_max validation: {e}")
            return False

    def non_negative(self, series):
        try:
            return series.fillna(0) >= 0
        except Exception as e:
            logger.error(f"!! Failed non_negative validation: {e}")
            return pd.Series([False] * len(series))