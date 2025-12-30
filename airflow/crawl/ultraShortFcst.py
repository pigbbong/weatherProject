from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
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
    dag_id="crawl_ultrashort_fcst",
    description="초단기예보 크롤링",
    default_args=default_args,
    start_date=datetime(2025, 12, 1, tzinfo=KST),
    schedule_interval="47 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["crawl"],
) as dag:

    # 초단기예보 크롤링
    crawl_ultrashort = BashOperator(
        task_id="crawl_ultrashort_fcst",
        bash_command="""python /app/crawling/crawl_ultraShort.py""",
    )

    # Redis 캐시 갱신
    cache_ultrashort = BashOperator(
        task_id="cache_ultrashort_to_redis",
        bash_command="""python /app/web/cache/cache_ultrashort.py""",
    )

    crawl_ultrashort >> cache_ultrashort
