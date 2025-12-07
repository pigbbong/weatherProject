from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="current_after",
    start_date=datetime(2025, 12, 1),
    schedule="10 3,9,15,21 * * *",  # 매일 3,9,15,21시 10분에 실행
    catchup=False,
):

    crawl = BashOperator(
        task_id="crawl_after",
        bash_command="python /opt/pipelines/after/crawl_after.py"
    )

    load_db = BashOperator(
        task_id="gcs_to_db_after",
        bash_command="python /opt/pipelines/after/gcs_to_db_after.py"
    )

    refresh_view = BashOperator(
        task_id="refresh_view_after",
        bash_command="psql postgresql://airflow:airflow@postgres/weather -f /opt/pipelines/after/view_after.sql"
    )

    crawl >> load_db >> refresh_view
