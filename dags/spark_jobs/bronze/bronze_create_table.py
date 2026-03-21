import sys
import os
from datetime import datetime

# Airflow / SparkSubmitOperator 환경에서 utils import 가능하게
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))  # /opt/airflow/dags 기준으로 utils 폴더 접근

from utils.spark_runner import SparkJobRunner


files_to_convert = [
    "olist_customers_dataset",
    "olist_geolocation_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "product_category_name_translation"
]


def create_table(spark, date_str=None):
    """
    Bronze CSV → Iceberg bronze table
    spark : SparkSession
    date_str : str, 변환할 날짜, 기본: 오늘
    """
    if date_str is None:
        date_str = datetime.today().strftime("%Y-%m-%d")

    import logging
    logger = logging.getLogger("bronze-create")

    for file_name in files_to_convert:
        csv_path = f"s3a://bronze/{file_name}/{date_str}/{file_name}.csv"

        try:
            df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)

            # Iceberg bronze 테이블 생성 (overwrite)
            df.writeTo(f"my_catalog.bronze.{file_name}").using("iceberg").createOrReplace()

            logger.info(f"[SUCCESS] {file_name} → Iceberg bronze 저장 완료")

        except Exception as e:
            logger.error(f"[ERROR] {file_name} → {e}", exc_info=True)


if __name__ == "__main__":

    runner = SparkJobRunner("bronze-create")

    runner.run(create_table)