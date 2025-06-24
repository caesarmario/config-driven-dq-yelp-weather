
# 🧱 Yelp Weather Insight Data Pipeline

> End-to-end ETL pipeline using **Apache Airflow**, **MinIO**, and **PostgreSQL** to process Yelp + Weather data for insight generation. Designed to be modular, schema-aware, and auto-validated with data quality monitoring.

---

## 📌 Features

- ⬇ Extract Yelp TAR datasets to MinIO
- 🔄 Transform Yelp JSON & Weather CSV into CSV/Parquet with validation
- 🧺 Merge chunked files into single CSVs
- 🛢 Load into PostgreSQL with schema creation & upsert support
- 📊 Generate DWH insights (fact tables)
- ✅ Data Quality checks on L1 & DWH layer

---

## 🔧 Main Modules

### `etl_utils.py`
Provides helper methods for:
- Connecting to MinIO / PostgreSQL
- Dynamic schema creation & alteration
- Upload/download files from MinIO
- Auto-upsert into PostgreSQL
- Execute SQL insights with temp table approach

### `validation_utils.py`
Supports:
- Field-level validations (e.g. `not_null`, `stars_range_check`)
- Cross-column checks (`min_less_than_max`)
- Auto chunk validation and rejection output

### `monitor_utils.py`
- Runs SQL-based data quality checks
- Supports over 15+ validation rule types
- Pushes metrics to monitoring tables

---

## 📁 Sample Config (Schema-Aware)

Example from `business_config.json`:

```json
{
  "latitude": {
    "col_csv": "latitude",
    "dtype": "FLOAT",
    "transformation": "to_numeric",
    "validation": ["not_null", "validate_latitude"]
  }
}
```

This enables:
- ✅ Auto-transform to numeric
- ✅ Rule: must not be null
- ✅ Rule: must be between -90 and 90

---