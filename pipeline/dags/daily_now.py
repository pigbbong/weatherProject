from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="daily_now",
    start_date=datetime(2025, 12, 1, tzinfo=KST),
    schedule="40 23 * * *",  # KST 기준 매일 23:40
    catchup=False,
    timezone=KST,
):

    dump_daily = BashOperator(
        task_id="dump_daily_now",
        bash_command="python /opt/pipelines/now/db_to_gcs_now_daily.py"
    )
