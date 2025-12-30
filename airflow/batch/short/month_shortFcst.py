from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "weather",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="batch_shortFcst_month",
    description="단기예보 월 단위 배치",
    default_args=default_args,
    start_date=datetime(2025, 12, 1, tzinfo=KST),
    schedule_interval="0 1 1 * *",  # 매달 1일 01:00
    catchup=False,
    max_active_runs=1,
    tags=["batch"],
) as dag:

    shortFcst_month_batch = BashOperator(
        task_id="run_shortFcst_month_batch",
        bash_command="""
        /opt/spark/bin/spark-submit \
        /app/batch/short/batch_month_shortFcst.py
        """,
    )

    shortFcst_month_batch
