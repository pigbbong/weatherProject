from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="current_after",
    start_date=datetime(2025, 12, 1, tzinfo=KST),
    schedule="10 3,9,15,21 * * *",  # KST 기준 03:10, 09:10, 15:10, 21:10
    catchup=False,
    timezone=KST,
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
