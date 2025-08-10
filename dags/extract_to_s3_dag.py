from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
import os

from include.extract_to_s3 import fetch_api, to_parquet, upload_file
from include.dq import validate_parquet_file   # ← add this

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="extract_to_s3_dag",
    description="Extract JSON → Parquet → S3 (Snowpipe auto-loads)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "s3", "dq"],
) as dag:

    def _extract(ti):
        df = fetch_api()
        fname = to_parquet(df)
        ti.xcom_push(key="parquet_file", value=fname)

    def _dq(ti):
        fname = ti.xcom_pull(key="parquet_file", task_ids="extract_to_parquet")
        validate_parquet_file(fname)

    def _upload(ti):
        fname = ti.xcom_pull(key="parquet_file", task_ids="extract_to_parquet")
        bucket = os.getenv("S3_BUCKET")
        upload_file(fname, bucket=bucket, key=f"raw/{fname}")

    extract = PythonOperator(task_id="extract_to_parquet", python_callable=_extract)
    dq      = PythonOperator(task_id="data_quality_check", python_callable=_dq)
    upload  = PythonOperator(task_id="upload_to_s3",      python_callable=_upload)

    extract >> dq >> upload
