from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "weather",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weekly_city_temp",
    description="도시별 1주일치 기온 변화",
    default_args=default_args,
    start_date=datetime(2025, 12, 1, tzinfo=KST),
    schedule_interval="0 1 * * *",  # 매일 01:00
    catchup=False,
    max_active_runs=1,
    tags=["analysis_temp"],
) as dag:

    weekly_city_temp = BashOperator(
        task_id="run_weekly_city_temp",
        bash_command="""
        /opt/spark/bin/spark-submit \
        /app/analysis/weekly_city_temp.py
        """,
    )

    weekly_city_temp

