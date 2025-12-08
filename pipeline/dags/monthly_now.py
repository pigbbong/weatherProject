from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="monthly_now",
    start_date=datetime(2025, 12, 1, tzinfo=KST),
    schedule="40 23 L * *",  # KST 기준 마지막 날 23:40
    catchup=False,
    timezone=KST,
):

    dump_monthly = BashOperator(
        task_id="dump_monthly_now",
        bash_command="python /opt/pipelines/now/db_to_gcs_now_monthly.py"
    )
