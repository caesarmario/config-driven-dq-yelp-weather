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
def run_dm_weather(table_name: str, **kwargs):
    """
    Run script to load weather parquet into db.
    """
    db_creds    = Variable.get("db_creds")

    # Build command
    cmd = [
        "python", "scripts/data_monitoring/monitor_data_quality_weather.py",
        "--table_name", table_name,
        "--db_creds", db_creds
    ]

    # Execute script
    subprocess.run(cmd, check=True)


def run_dm_yelp(table_name: str, **kwargs):
    """
    Run script to load weather parquet into db.
    """
    db_creds    = Variable.get("db_creds")

    # Build command
    cmd = [
        "python", "scripts/monitor_data_quality_yelp.py",
        "--table_name", table_name,
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
with TaskGroup("monitor_weather", dag=dag) as weather_group:

    weather_precipitation = PythonOperator(
        task_id="weather_precipitation",
        python_callable=run_dm_weather,
        op_kwargs={
            "table_name": "precipitation",
        },
        dag=dag,
    )

    weather_temperature = PythonOperator(
        task_id="weather_temperature",
        python_callable=run_dm_weather,
        op_kwargs={
            "table_name": "temperature",
        },
        dag=dag,
    )

# Dummy End
task_end = EmptyOperator(
    task_id="task_end",
    dag=dag
)

# -- Define execution order
task_start >> weather_group >> task_end