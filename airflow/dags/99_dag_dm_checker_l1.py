####
## Airflow DAG: To perform data monitoring for l1 layer
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

# -- DAG-level settings
job_name        = "dm_checker_l1"

default_args = {
    "owner"             : "caesarmario87@gmail.com",
    "depends_on_past"   : False,
    "start_date"        : datetime(2025, 5, 1),
    "retries"           : 1,
    "max_active_runs"   : 1,
    "retry_delay"       : timedelta(minutes=2),
}

dag = DAG(
    dag_id            = f"99_dag_{job_name}",
    default_args      = default_args,
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["test_case", "tech_implementation"]
)


# -- Function: run data monitoring script
def run_dm(table_name: str, schema_name: str, **kwargs):
    db_creds    = Variable.get("db_creds")

    # Build command
    cmd = [
        "python", "scripts/data_monitoring/monitor_data_quality_l1.py",
        "--table_name", table_name,
        "--schema_name", schema_name,
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

# Monitoring
with TaskGroup("monitoring_group", dag=dag) as monitor_group:
    with TaskGroup("monitor_weather", dag=dag) as weather_group:

        weather_precipitation = PythonOperator(
            task_id="weather_precipitation",
            python_callable=run_dm,
            op_kwargs={
                "table_name": "precipitation",
                "schema_name": "weather"
            },
            dag=dag,
        )

        weather_temperature = PythonOperator(
            task_id="weather_temperature",
            python_callable=run_dm,
            op_kwargs={
                "table_name": "temperature",
                "schema_name": "weather"
            },
            dag=dag,
        )


    with TaskGroup("monitor_yelp", dag=dag) as yelp_group:

        yelp_business = PythonOperator(
            task_id="yelp_business",
            python_callable=run_dm,
            op_kwargs={
                "table_name": "business",
                "schema_name": "yelp"
            },
            dag=dag,
        )

        yelp_tip = PythonOperator(
            task_id="yelp_tip",
            python_callable=run_dm,
            op_kwargs={
                "table_name": "tip",
                "schema_name": "yelp"
            },
            dag=dag,
        )

        yelp_checkin = PythonOperator(
            task_id="yelp_checkin",
            python_callable=run_dm,
            op_kwargs={
                "table_name": "checkin",
                "schema_name": "yelp"
            },
            dag=dag,
        )

        yelp_user = PythonOperator(
            task_id="yelp_user",
            python_callable=run_dm,
            op_kwargs={
                "table_name": "user",
                "schema_name": "yelp"
            },
            dag=dag,
        )

        yelp_review = PythonOperator(
            task_id="yelp_review",
            python_callable=run_dm,
            op_kwargs={
                "table_name": "review",
                "schema_name": "yelp"
            },
            dag=dag,
        )

    weather_group >> yelp_group

# Dummy End
task_end = EmptyOperator(
    task_id="task_end",
    dag=dag
)

# -- Define execution order
task_start >> monitor_group >> task_end