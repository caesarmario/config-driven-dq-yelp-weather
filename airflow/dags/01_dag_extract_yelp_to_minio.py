####
## Airflow DAG: Extract yelp data to MinIO
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
job_name        = "extract_yelp_to_minio"

default_args = {
    "owner"             : "caesarmario87@gmail.com",
    "depends_on_past"   : False,
    "start_date"        : datetime(2025, 5, 1),
    "retries"           : 1,
    "max_active_runs"   : 1,
    "retry_delay"       : timedelta(minutes=2),
}

dag = DAG(
    dag_id            = f"01_dag_{job_name}",
    default_args      = default_args,
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["test_case", "tech_implementation"]
)

# -- Function: run data extractor script
def run_extractor(file_name: str, **kwargs):
    """
    Execute extract yelp script by pass file name and MinIO creds.
    """
    # MinIO Credentials
    minio_creds     = Variable.get("minio_creds")

    # Build command
    cmd = [
        "python", "scripts/extract_yelp_to_minio.py",
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

# Extract & transform JSON to Parquet files
with TaskGroup("extract_yelp", dag=dag) as extract_group:
    extract_yelp_dataset = PythonOperator(
        task_id="yelp_dataset",
        python_callable=run_extractor,
        op_kwargs={
            "file_name": "yelp_dataset",
        },
        dag=dag,
    )
    
    # extract_yelp_photos = PythonOperator(
    #     task_id="yelp_photos",
    #     python_callable=run_extractor,
    #     op_kwargs={
    #         "file_name": "yelp_photos",
    #     },
    #     dag=dag,
    # )
    

# Trigger downstream DAGs
trigger_weather_csv = TriggerDagRunOperator(
    task_id="trigger_weather_csv_dag",
    trigger_dag_id="02_dag_process_weather_csv_to_minio",
    dag=dag,
)

# trigger_yelp_json = TriggerDagRunOperator(
#     task_id="trigger_yelp_json_dag",
#     trigger_dag_id="02_dag_process_yelp_json_to_minio",
#     dag=dag,
# )

# Dummy End
task_end = EmptyOperator(
    task_id="task_end",
    dag=dag
)

# -- Define execution order
task_start >> extract_group
extract_group >> [trigger_weather_csv] >> task_end