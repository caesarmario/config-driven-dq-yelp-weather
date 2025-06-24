####
## Airflow DAG: To process yelp json data
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

# from utils.email_utils import send_email_alert

# -- DAG-level settings
job_name        = "process_yelp_json_to_minio"

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

# -- Function: run data processor & merger script
def run_processor(file_name: str, **kwargs):
    """
    Run script to process Yelp JSON into chunked Parquet via MinIO.
    """
    minio_creds = Variable.get("minio_creds")

    # Build command
    cmd = [
        "python", "scripts/process_yelp_json_to_minio.py",
        "--file_name", file_name,
        "--creds", minio_creds
    ]

    # Execute script
    subprocess.run(cmd, check=True)


def run_merger(file_name: str, **kwargs):
    """
    Run script to merge chunk parquet into MinIO
    """
    minio_creds = Variable.get("minio_creds")

    # Build command
    cmd = [
        "python", "scripts/merge_yelp_paruqet_to_minio.py",
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
with TaskGroup("process_yelp", dag=dag) as process_group:
    with TaskGroup("tip_group", tooltip="Process and merge tip data") as tip_group:
        process_tip = PythonOperator(
            task_id="process_tip",
            python_callable=run_processor,
            op_kwargs={
                "file_name": "tip",
            },
            dag=dag,
        )

        # merge_tip = PythonOperator(
        #     task_id="merge_tip",
        #     python_callable=run_merger,
        #     op_kwargs={
        #         "file_name": "tip",
        #     },
        #     dag=dag,
        # )

        # process_tip >> merge_tip

    
    with TaskGroup("checkin_group", tooltip="Process and merge checkin data") as checkin_group:
        process_checkin = PythonOperator(
            task_id="process_checkin",
            python_callable=run_processor,
            op_kwargs={
                "file_name": "checkin",
            },
            dag=dag,
        )

        # merge_checkin = PythonOperator(
        #     task_id="merge_checkin",
        #     python_callable=run_merger,
        #     op_kwargs={
        #         "file_name": "checkin",
        #     },
        #     dag=dag,
        # )

        # process_checkin >> merge_checkin


    with TaskGroup("business_group", tooltip="Process and merge business data") as business_group:
        process_business = PythonOperator(
            task_id="process_business",
            python_callable=run_processor,
            op_kwargs={
                "file_name": "business",
            },
            dag=dag,
        )

        # merge_business = PythonOperator(
        #     task_id="merge_business",
        #     python_callable=run_merger,
        #     op_kwargs={
        #         "file_name": "business",
        #     },
        #     dag=dag,
        # )

        # process_business >> merge_business


    with TaskGroup("review_group", tooltip="Process and merge review data") as review_group:
        process_review = PythonOperator(
            task_id="process_review",
            python_callable=run_processor,
            op_kwargs={
                "file_name": "review",
            },
            dag=dag,
        )

        # merge_review = PythonOperator(
        #     task_id="merge_review",
        #     python_callable=run_merger,
        #     op_kwargs={
        #         "file_name": "review",
        #     },
        #     dag=dag,
        # )

        # process_review >> merge_review


    with TaskGroup("user_group", tooltip="Process and merge user data") as user_group:
        process_user = PythonOperator(
            task_id="process_user",
            python_callable=run_processor,
            op_kwargs={
                "file_name": "user",
            },
            dag=dag,
        )

        # merge_user = PythonOperator(
        #     task_id="merge_user",
        #     python_callable=run_merger,
        #     op_kwargs={
        #         "file_name": "user",
        #     },
        #     dag=dag,
        # )

        # process_user >> merge_user

# Trigger DAG
trigger_loader = TriggerDagRunOperator(
    task_id="trigger_load_parquet",
    trigger_dag_id="03_dag_load_parquet_to_db",
    dag=dag
)

# Dummy End
task_end = EmptyOperator(
    task_id="task_end",
    dag=dag
)

# -- Define execution order
task_start >> process_group >> trigger_loader >> task_end