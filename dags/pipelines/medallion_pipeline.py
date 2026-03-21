from airflow.decorators import dag
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from utils.load_raw_data import extract_and_upload_to_minio

@dag(
    dag_id="medallion_v2_spark_submit",
    start_date=datetime(2026, 3, 18),
    schedule="@daily",
    catchup=False,
    tags=["medallion", "spark_submit"]
)
def medallion_pipeline():

    # upload raw file(independet Task)
    ingest_raw = PythonOperator(
        task_id="ingest_raw_to_minio",
        python_callable=extract_and_upload_to_minio,
        trigger_rule="none_failed_min_one_success" # execute anyway
    )

    # Spark pipeline(Bind with Task Group)
    with TaskGroup("spark_processing_jobs") as spark_jobs:

        bronze = SparkSubmitOperator(
            task_id="bronze_create_table",
            application="/opt/airflow/dags/spark_jobs/bronze/bronze_create_table.py",
            conn_id="bronze",
            executor_memory="2g",
            driver_memory="1g",
            verbose=True,
            trigger_rule="all_done" # whatever prior task's state, just execute
        )

        silver = SparkSubmitOperator(
            task_id="silver_pipeline",
            application="/opt/airflow/dags/spark_jobs/silver/silver_pipeline.py",
            conn_id="silver",
            executor_memory="2g",
            driver_memory="1g",
            jars="/opt/airflow/jars/*",
            verbose=True
        )

        gold = SparkSubmitOperator(
            task_id="gold_stats",
            application="/opt/airflow/dags/spark_jobs/gold/gold_stats.py",
            conn_id="gold",
            executor_memory="2g",
            driver_memory="1g",
            jars="/opt/airflow/jars/*",
            verbose=True
        )

        bronze >> silver >> gold
    ingest_raw >> spark_jobs

dag = medallion_pipeline()