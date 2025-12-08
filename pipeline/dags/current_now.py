from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="current_now",
    start_date=datetime(2025, 12, 1, tzinfo=KST),
    schedule="0,30 * * * *",  # KST 기준 매시 0분, 30분
    catchup=False,
    timezone=KST,
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
