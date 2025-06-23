####
## Airflow DAG: To load cleaned parquet data to postgre
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

# -- Imports
from airflow import DAG
from airflow.sdk import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import subprocess
from datetime import datetime, timedelta

from utils.email_utils import send_email_alert

# -- DAG-level settings
job_name        = "load_parquet_to_db"

default_args = {
    "owner"             : "caesarmario87@gmail.com",
    "depends_on_past"   : False,
    "start_date"        : datetime(2025, 5, 1),
    "retries"           : 1,
    "max_active_runs"   : 1,
    "retry_delay"       : timedelta(minutes=2),
}

dag = DAG(
    dag_id            = f"03_dag_{job_name}",
    default_args      = default_args,
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["test_case", "tech_implementation"]
)


# -- Function: run data loader script
def run_loader(file_name: str, folder_name:str, **kwargs):
    """
    Run script to load weather parquet into db.
    """
    minio_creds = Variable.get("minio_creds")
    db_creds    = Variable.get("db_creds")

    # Build command
    cmd = [
        "python", "scripts/load_parquet_to_db.py",
        "--folder_name", folder_name,
        "--file_name", file_name,
        "--minio_creds", minio_creds,
        "--db_creds", db_creds
    ]

    # Execute script
    subprocess.run(cmd, check=True)


# -- Tasks
# Dummy Start
task_start = EmptyOperator(
    task_id="task_start",
    dag=dag
)

# Load Parquet files to db
with TaskGroup("loader_group", dag=dag) as loader_group:
    with TaskGroup("load_weather", dag=dag) as weather_group:
        load_precipitation = PythonOperator(
            task_id="load_precipitation",
            python_callable=run_loader,
            op_kwargs={
                "folder_name": "weather",
                "file_name": "precipitation"
            },
            dag=dag
        )
        
        load_temperature = PythonOperator(
            task_id="load_temperature",
            python_callable=run_loader,
            op_kwargs={
                "folder_name": "weather",
                "file_name": "temperature"
            },
            dag=dag
        )


    with TaskGroup("load_yelp", dag=dag) as yelp_group:
        load_tip = PythonOperator(
            task_id="load_tip",
            python_callable=run_loader,
            op_kwargs={
                "folder_name": "yelp",
                "file_name": "tip"
            },
            dag=dag,
        )
        
        load_checkin = PythonOperator(
            task_id="load_checkin",
            python_callable=run_loader,
            op_kwargs={
                "folder_name": "yelp",
                "file_name": "checkin"
            },
            dag=dag,
        )

        load_business = PythonOperator(
            task_id="load_business",
            python_callable=run_loader,
            op_kwargs={
                "folder_name": "yelp",
                "file_name": "business"
            },
            dag=dag,
        )

        load_review = PythonOperator(
            task_id="load_review",
            python_callable=run_loader,
            op_kwargs={
                "folder_name": "yelp",
                "file_name": "review"
            },
            dag=dag,
        )

        load_user = PythonOperator(
            task_id="load_user",
            python_callable=run_loader,
            op_kwargs={
                "folder_name": "yelp",
                "file_name": "user"
            },
            dag=dag,
        )

        load_tip >> load_checkin >> load_business >> load_review >> load_user
    
    weather_group >> yelp_group

# Dummy End
task_end = EmptyOperator(
    task_id="task_end",
    dag=dag
)

# -- Define execution order
task_start >> loader_group >> task_end