from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "weather",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="batch_ultraShortFcst_year",
    description="초단기예보 연 단위 배치",
    default_args=default_args,
    start_date=datetime(2025, 12, 1, tzinfo=KST),
    schedule_interval="0 2 1 1 *",  # 매년 1월 1일 02:00
    catchup=False,
    max_active_runs=1,
    tags=["batch"],
) as dag:

    ultraShortFcst_year_batch = BashOperator(
        task_id="run_ultraShortFcst_year_batch",
        bash_command="""
        /opt/spark/bin/spark-submit \
        /app/batch/ultraShort/batch_year_ultraShortFcst.py
        """,
    )

    ultraShortFcst_year_batch
