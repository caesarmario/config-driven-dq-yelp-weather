from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='dbt_test_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['dbt', 'quality'],
) as dag:

    dbt_test = BashOperator(
        task_id='run_dbt_test',
        bash_command='cd /usr/app && dbt test',
    )

    dbt_test
