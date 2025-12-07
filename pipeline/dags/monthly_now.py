from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="monthly_now",
    start_date=datetime(2025, 12, 1),
    schedule="40 23 L * *",
    catchup=False,
):

    dump_monthly = BashOperator(
        task_id="dump_monthly_now",
        bash_command="python /opt/pipelines/now/db_to_gcs_now_monthly.py"
    )