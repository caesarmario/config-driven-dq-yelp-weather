####
## Airflow DAG: To upload csv data to MinIO
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

# -- Imports
from airflow import DAG
from airflow.sdk import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import subprocess
from datetime import datetime, timedelta

# from utils.email_utils import send_email_alert

# -- DAG-level settings
job_name        = "process_weather_csv_to_minio"

default_args = {
    "owner"             : "caesarmario87@gmail.com",
    "depends_on_past"   : False,
    "start_date"        : datetime(2025, 5, 1),
    "retries"           : 1,
    "max_active_runs"   : 1,
    "retry_delay"       : timedelta(minutes=2),
}

dag = DAG(
    dag_id            = f"02_dag_{job_name}",
    default_args      = default_args,
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["test_case", "tech_implementation"]
)

# -- Function: run data processor script
def run_processor(file_name: str, **kwargs):
    """
    Execute processing weather script data to MinIO.
    """
    # MinIO Credentials
    minio_creds     = Variable.get("minio_creds")

    # Build command
    cmd = [
        "python", "scripts/process_weather_csv_to_minio.py",
        "--file_name", file_name,
        "--creds", minio_creds
    ]

    # Execute script
    subprocess.run(cmd, check=True)


# -- Tasks
# Dummy Start
task_start = EmptyOperator(
    task_id="task_start",
    dag=dag
)

# Extract & transform csv to Parquet files
with TaskGroup("process_weather", dag=dag) as process_group:
    process_precipitation = PythonOperator(
        task_id="process_precipitation",
        python_callable=run_processor,
        op_kwargs={
            "file_name": "precipitation",
        },
        dag=dag,
    )
    
    process_temperature = PythonOperator(
        task_id="process_temperature",
        python_callable=run_processor,
        op_kwargs={
            "file_name": "temperature",
        },
        dag=dag,
    )
    

# Dummy End
task_end = EmptyOperator(
    task_id="task_end",
    dag=dag
)

# -- Define execution order
task_start >> process_group >> task_end