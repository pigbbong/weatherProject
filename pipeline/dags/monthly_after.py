from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="monthly_after",
    start_date=datetime(2025, 12, 1, tzinfo=KST),
    schedule="40 23 L * *",  # KST 기준 마지막 날 23:40
    catchup=False,
    timezone=KST,
):

    dump_after = BashOperator(
        task_id="dump_after_monthly",
        bash_command="python /opt/pipelines/after/db_to_gcs_after.py"
    )
