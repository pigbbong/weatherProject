from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

# KST 타임존
KST = pendulum.timezone("Asia/Seoul")

# 기본 DAG 설정
default_args = {
    "owner": "weather",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="crawl_weather_now",
    description="초단기 실황 크롤링 DAG",
    default_args=default_args,
    start_date=datetime(2025, 12, 1, tzinfo=KST),
    schedule="12 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["crawl"],
) as dag:

    # 초단기실황 크롤링
    crawl_now = BashOperator(
        task_id="crawl_now",
        bash_command="""
        sleep $((RANDOM % 480));
        python /app/crawling/crawl_now.py
        """
    )

    crawl_now
