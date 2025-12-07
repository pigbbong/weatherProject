from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# 실시간(now) 파이프라인 (매 1시간 실행)
with DAG(
    dag_id="current_now",
    start_date=datetime(2025, 12, 1),
    schedule="0,30 * * * *",  # 30분마다 실행
    catchup=False,
):

    crawl = BashOperator(
        task_id="crawl_now",
        bash_command="python /opt/pipelines/now/crawl_now.py"
    )

    load_db = BashOperator(
        task_id="gcs_to_db_now",
        bash_command="python /opt/pipelines/now/gcs_to_db_now.py"
    )

    refresh_view = BashOperator(
        task_id="refresh_view_now",
        bash_command="psql postgresql://airflow:airflow@postgres/weather -f /opt/pipelines/now/view_now.sql"
    )

    crawl >> load_db >> refresh_view
